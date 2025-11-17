import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import folium
from streamlit_folium import st_folium
from folium.plugins import MiniMap
import requests
import numpy as np
import yaml

# -------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------
st.set_page_config(layout="wide")
st.title("ROSA - Retail Opportunity Site Analysis")

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
SHEET_NAME = "ROSA Database"
WORKSHEET_NAME = "Sheet1"
CREDS_FILE = "credentials.json"
PROPERTY_ID_COL = 12   # Column L
NOTES_COL = 3          # Column C
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# -------------------------------------------------
# Load secrets – Streamlit Cloud + local (no local file fallback on Cloud)
# -------------------------------------------------

import json

GEOCODIO_KEY = st.secrets["GEOCODIO_KEY"]
creds_dict = json.loads(st.secrets["GSPREAD_JSON"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)

# -------------------------------------------------
# GOOGLE SHEETS CONNECTION
# -------------------------------------------------
@st.cache_resource(show_spinner=False)
def get_sheet():
    try:
        client = gspread.authorize(creds)
        spreadsheet = client.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        return worksheet
    except Exception as e:
        st.error(f"Could not connect to Google Sheet: {e}")
        return None

sheet = get_sheet()
if not sheet:
    st.stop()

# -------------------------------------------------
# GEOCODING FUNCTION
# -------------------------------------------------
@st.cache_data(show_spinner=False)
def geocode_address(address, pid):
    if pd.isna(address) or str(address).strip() == "":
        return None, None
    try:
        cell = sheet.find(str(pid), in_column=PROPERTY_ID_COL)
        if cell:
            row = sheet.row_values(cell.row)
            headers = sheet.row_values(1)
            lat_idx = headers.index("Latitude") + 1 if "Latitude" in headers else None
            lon_idx = headers.index("Longitude") + 1 if "Longitude" in headers else None
            if lat_idx and lon_idx and row[lat_idx-1] and row[lon_idx-1]:
                return float(row[lat_idx-1]), float(row[lon_idx-1])
    except:
        pass

    try:
        url = "https://api.geocod.io/v1.7/geocode"
        params = {"q": address, "api_key": GEOCODIO_KEY}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get("results"):
                loc = data["results"][0]["location"]
                if cell:
                    headers = sheet.row_values(1)
                    lat_idx = headers.index("Latitude") + 1 if "Latitude" in headers else None
                    lon_idx = headers.index("Longitude") + 1 if "Longitude" in headers else None
                    if lat_idx and lon_idx:
                        sheet.update_cell(cell.row, lat_idx, loc["lat"])
                        sheet.update_cell(cell.row, lon_idx, loc["lng"])
                return loc["lat"], loc["lng"]
    except:
        pass
    return None, None

# -------------------------------------------------
# SAVE NOTES FUNCTION
# -------------------------------------------------
def save_notes_to_sheet(pid, new_notes):
    cell = sheet.find(str(pid), in_column=PROPERTY_ID_COL)
    if cell:
        sheet.update_cell(cell.row, NOTES_COL, str(new_notes))
        return True
    return False

# -------------------------------------------------
# LOAD DATA
# -------------------------------------------------
@st.cache_data(show_spinner="Loading data from Google Sheets...")
def load_data():
    records = sheet.get_all_records()
    df = pd.DataFrame(records)
    required = ["Property_ID","Address","Notes","Contact Name","Number","Status","Market","Latitude","Longitude"]
    for col in required:
        if col not in df.columns:
            df[col] = ""
    df["Status_Display"] = df["Status"].replace({"":"Blank","Y":"Yes","N":"No","P":"Pending"})
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    return df

df_raw = load_data()

# -------------------------------------------------
# FILTERS
# -------------------------------------------------
col1, col2 = st.columns(2)
with col1:
    markets = ["All"] + sorted(df_raw["Market"].dropna().unique().tolist())
    selected_market = st.selectbox("Market", markets, index=0)
with col2:
    status_options = ["Yes", "No", "Pending", "Blank"]
    selected_status = st.multiselect("Status", status_options, default=status_options)

df = df_raw.copy()
if selected_market != "All":
    df = df[df["Market"] == selected_market]
status_map = {"Yes":"Y","No":"N","Pending":"P","Blank":""}
selected_codes = [status_map[s] for s in selected_status]
df = df[df["Status"].isin(selected_codes)]

# -------------------------------------------------
# GEOCODE FILTERED DATA
# -------------------------------------------------
with st.spinner("Processing addresses..."):
    to_geocode = df[(df["Latitude"].isna()) | (df["Longitude"].isna())]
    if not to_geocode.empty:
        lat_lon = to_geocode.apply(lambda row: geocode_address(row["Address"], row["Property_ID"]), axis=1)
        df.loc[to_geocode.index, ["Latitude","Longitude"]] = pd.DataFrame(lat_lon.tolist(), index=to_geocode.index)
    df_geo = df.dropna(subset=["Latitude","Longitude"]).copy()
    st.write(f"**{len(df_geo)} properties mapped**")
    if len(df) - len(df_geo):
        st.warning(f"{len(df)-len(df_geo)} failed to geocode")

# -------------------------------------------------
# LAYOUT
# -------------------------------------------------
map_col, notes_col = st.columns([2, 1])   # ← this line was missing before

# -------------------------------------------------
# MAP (left 2/3)
# -------------------------------------------------
with map_col:
    if not df_geo.empty:
        center_lat = df_geo["Latitude"].mean()
        center_lon = df_geo["Longitude"].mean()
        m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles="CartoDB positron")

        status_color = {"Y": "green", "N": "red", "P": "orange", "": "gray"}
        marker_dict = {}

        for _, row in df_geo.iterrows():
            color = status_color.get(row["Status"], "blue")
            sv_url = f"https://www.google.com/maps?q=&layer=c&cbll={row['Latitude']},{row['Longitude']}"
            popup_html = f"""
            <b>{row['Address']}</b><br>
            <b>Notes:</b> {row['Notes']}<br>
            <b>Contact:</b> {row['Contact Name'] or 'N/A'} ({row['Number'] or 'N/A'})<br>
            <b>Status:</b> {row['Status_Display']}<br>
            <b>Market:</b> {row['Market']}<br>
            <a href="{sv_url}" target="_blank">Street View</a>
            """
            marker = folium.Marker(
                location=[row["Latitude"], row["Longitude"]],
                popup=folium.Popup(popup_html, max_width=350),
                tooltip=row["Address"],
                icon=folium.Icon(color=color, icon="circle", prefix="fa")
            )
            marker.add_to(m)
            marker_dict[(round(row["Latitude"], 6), round(row["Longitude"], 6))] = row["Property_ID"]

            if "editing_pid" in st.session_state and row["Property_ID"] == st.session_state.editing_pid:
                folium.CircleMarker(
                    location=[row["Latitude"], row["Longitude"]],
                    radius=15, color="red", fill=True, fill_color="red", fill_opacity=0.7
                ).add_to(m)

        m.fit_bounds(m.get_bounds())
        MiniMap().add_to(m)
        map_data = st_folium(m, width=800, height=600, key="map")

        if map_data and map_data.get("last_object_clicked"):
            click_lat = map_data["last_object_clicked"]["lat"]
            click_lng = map_data["last_object_clicked"]["lng"]
            key = (round(click_lat, 6), round(click_lng, 6))
            clicked_pid = marker_dict.get(key)
            if clicked_pid:
                row = df_geo[df_geo["Property_ID"] == clicked_pid].iloc[0]
                st.session_state.editing_pid = clicked_pid
                st.session_state.editing_notes = row["Notes"]
                st.session_state.editing_address = row["Address"]
                st.session_state.editing_lat = row["Latitude"]
                st.session_state.editing_lng = row["Longitude"]
                st.rerun()

# -------------------------------------------------
# NOTES EDITOR (right 1/3)
# -------------------------------------------------
with notes_col:
    if "editing_pid" in st.session_state:
        pid = st.session_state.editing_pid
        current_notes = st.session_state.editing_notes
        address = st.session_state.editing_address
        lat = st.session_state.editing_lat
        lng = st.session_state.editing_lng
        sv_url = f"https://www.google.com/maps?q=&layer=c&cbll={lat},{lng}"

        st.subheader(f"Edit: {address}")
        st.markdown(f"[**Street View**]({sv_url})", unsafe_allow_html=True)
        new_notes = st.text_area("Notes", value=current_notes, height=250, key=f"notes_{pid}")

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Save", key=f"save_{pid}"):
                if save_notes_to_sheet(pid, new_notes):
                    st.success("Saved to Sheet!")
                    df_raw.loc[df_raw["Property_ID"] == pid, "Notes"] = new_notes
                    st.rerun()
                else:
                    st.error("Failed to save")
        with c2:
            if st.button("Cancel", key=f"cancel_{pid}"):
                for k in list(st.session_state.keys()):
                    if k.startswith("editing_"):
                        del st.session_state[k]
                st.rerun()
    else:
        st.info("Click a row or pin to edit notes")

# -------------------------------------------------
# TABLE (below map – optional, you can keep or delete)
# -------------------------------------------------
st.subheader("Data Table")
edited_df = st.data_editor(
    df[["Property_ID", "Address", "Notes", "Contact Name", "Number", "Status_Display", "Market"]],
    key="table"
)

if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()