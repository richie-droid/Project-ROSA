import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import time
import logging

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
SHEET_NAME = "ROSA Database"
WORKSHEET_NAME = "Sheet1"
CREDS_FILE = "credentials.json"
GEOCODIO_KEY = "001c70ca6864c9094010d9cc4da16ccc9dc00a0"
PROPERTY_ID_COL = 12  # Column L (1-based index)
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("geocode_rosa.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# GOOGLE SHEETS CONNECTION
# -------------------------------------------------
def get_sheet():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        # Initialize Latitude/Longitude columns
        headers = worksheet.row_values(1)
        if "Latitude" not in headers:
            worksheet.update_cell(1, len(headers) + 1, "Latitude")
            logger.info("Added Latitude column")
        if "Longitude" not in headers:
            worksheet.update_cell(1, len(headers) + 2, "Longitude")
            logger.info("Added Longitude column")
        return worksheet
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Spreadsheet '{SHEET_NAME}' not found. Check title & sharing.")
        raise
    except Exception as e:
        logger.error(f"Connection error: {e}")
        raise

# -------------------------------------------------
# GEOCODING FUNCTION
# -------------------------------------------------
def geocode_address(address, pid, worksheet):
    if pd.isna(address) or str(address).strip() == "":
        logger.warning(f"Invalid address for Property_ID {pid}: {address}")
        return None, None
    try:
        url = "https://api.geocod.io/v1.7/geocode"
        params = {"q": address, "api_key": GEOCODIO_KEY}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get("results"):
                loc = data["results"][0]["location"]
                logger.info(f"Geocoded {address} (Property_ID: {pid}) -> ({loc['lat']}, {loc['lng']})")
                return loc["lat"], loc["lng"]
            else:
                logger.warning(f"No geocoding results for {address} (Property_ID: {pid})")
                return None, None
        elif response.status_code == 429:
            logger.warning("Rate limit hit. Pausing...")
            time.sleep(60)  # Wait 1 minute for rate limit reset
            return geocode_address(address, pid, worksheet)  # Retry
        else:
            logger.error(f"Geocod.io error for {address} (Property_ID: {pid}): {response.status_code}")
            return None, None
    except Exception as e:
        logger.error(f"Geocode error for {address} (Property_ID: {pid}): {e}")
        return None, None

# -------------------------------------------------
# MAIN BATCH GEOCODING
# -------------------------------------------------
def batch_geocode():
    logger.info("Starting batch geocoding...")
    worksheet = get_sheet()
    
    # Load data
    records = worksheet.get_all_records()
    df = pd.DataFrame(records)
    required = ["Property_ID", "Address", "Latitude", "Longitude"]
    for col in required:
        if col not in df.columns:
            df[col] = ""
    
    # Convert Latitude/Longitude to numeric, handle invalid
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    
    # Identify rows needing geocoding
    to_geocode = df[(df["Latitude"].isna()) | (df["Longitude"].isna()) | 
                    (df["Address"].str.strip() != "") & (df["Property_ID"] != "")]
    if to_geocode.empty:
        logger.info("No addresses need geocoding.")
        return
    
    logger.info(f"Found {len(to_geocode)} addresses to geocode.")
    
    # Geocode and update Sheet
    headers = worksheet.row_values(1)
    lat_idx = headers.index("Latitude") + 1 if "Latitude" in headers else None
    lon_idx = headers.index("Longitude") + 1 if "Longitude" in headers else None
    geocoded_count = 0
    failed_count = 0
    
    for idx, row in to_geocode.iterrows():
        pid = row["Property_ID"]
        address = row["Address"]
        cell = worksheet.find(pid, in_column=PROPERTY_ID_COL)
        if not cell:
            logger.warning(f"Property_ID {pid} not found in Sheet.")
            failed_count += 1
            continue
        
        lat, lon = geocode_address(address, pid, worksheet)
        if lat is not None and lon is not None:
            try:
                worksheet.update_cell(cell.row, lat_idx, lat)
                worksheet.update_cell(cell.row, lon_idx, lon)
                geocoded_count += 1
            except Exception as e:
                logger.error(f"Failed to update Sheet for {pid}: {e}")
                failed_count += 1
        else:
            failed_count += 1
        # Avoid rate limits (Geocod.io free tier: ~1 req/sec)
        time.sleep(1)
    
    logger.info(f"Batch geocoding complete: {geocoded_count} successful, {failed_count} failed.")
    if failed_count > 0:
        logger.info("Check geocode_rosa.log for details on failed addresses.")

# -------------------------------------------------
# RUN
# -------------------------------------------------
if __name__ == "__main__":
    try:
        batch_geocode()
    except Exception as e:
        logger.error(f"Script failed: {e}")