import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIG ---
CREDS_FILE = "../credentials.json"
SHEET_NAME = "ROSA Database"
WORKSHEET_NAME = "Sheet1"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# --- CONNECT ---
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)

# --- UI ---
st.title("ROSA Save Test")
records = sheet.get_all_records()
df = st.dataframe(records, use_container_width=True)

pid = st.text_input("Enter Property_ID (from column L)")
new_notes = st.text_area("New Notes")

if st.button("SAVE TO COLUMN C"):
    if not pid or not new_notes:
        st.error("Fill both fields")
    else:
        cell = sheet.find(pid, in_column=12)  # Column L
        if cell:
            row_idx = cell.row
            st.write(f"Found in row **{row_idx}** → updating column C")
            sheet.update_cell(row_idx, 3, new_notes)  # Column C
            st.success(f"Saved! Check **ROSA Database → Row {row_idx}, Column C**")
        else:
            st.error("Property_ID not found in column L")