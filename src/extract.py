import requests
import json
import logging
import time
from datetime import datetime
from src.config import FRED_API_KEY, BLS_API_KEY, DATA_RAW_DIR

# --- HELPER: IDEMPOTENCY & STORAGE ---

def get_storage_path(source, identifier):
    """
    Standardizes file naming: Source_ID_YYYY_MM_DD.json
    This allows the script to pick up revisions once per day instead of once per month. 
    """
    datestamp = datetime.now().strftime("%Y_%m_%d")
    filename = f"{source}_{identifier}_{datestamp}.json"
    return DATA_RAW_DIR / filename

def is_already_extracted(source, identifier):
    """
    Checks if the file exists. 
    Returns: (bool, filepath, data_or_none)
    """
    filepath = get_storage_path(source, identifier)
    if filepath.exists():
        logging.info(f"⏩ Idempotency: {source} {identifier} already exists. Skipping.")
        with open(filepath, 'r') as f:
            try:
                return True, filepath, json.load(f)
            except json.JSONDecodeError:
                return False, filepath, None
    return False, filepath, None

# --- HELPER: ROBUSTNESS ---

def fetch_with_retry(func):
    """Decorator to handle transient network errors or rate limits."""
    def wrapper(*args, **kwargs):
        for attempt in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.warning(f"⚠️ Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt) # Exponential backoff
                else:
                    raise e
    return wrapper

# --- CORE EXTRACTION FUNCTIONS ---

@fetch_with_retry
def fetch_fred_data(series_id):
    """Fetches a single series from FRED API."""
    # UPDATED: Added 'saved_data' to match the 3 values returned by the helper
    exists, filepath, saved_data = is_already_extracted("FRED", series_id)
    if exists:
        return

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    with open(filepath, 'w') as f:
        json.dump(response.json(), f)
        
    logging.info(f"✅ Extracted FRED: {series_id}")

@fetch_with_retry
def fetch_bls_data(series_dict, start_year, end_year):
    # The helper now handles the 'Already Exists' logic and returns the data
    exists, filepath, saved_data = is_already_extracted("BLS", "batch_pull")
    
    if exists and saved_data:
        # Granular logging using the data returned by the helper
        series_results = saved_data.get('Results', {}).get('series', [])
        for s in series_results:
            logging.info(f"⏩ Verified in Local Cache: BLS {s['seriesID']}")
        return

    # If it doesn't exist, proceed with the API call
    series_ids = list(series_dict.values())
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    headers = {'Content-type': 'application/json'}
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY
    }
    
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    data = response.json()

    if data.get("status") != "REQUEST_SUCCEEDED":
        logging.error(f"❌ BLS API Error: {data.get('message')}")
        return
    
    with open(filepath, 'w') as f:
        json.dump(data, f)
        
    for s in data.get('Results', {}).get('series', []):
        logging.info(f"✅ Extracted BLS: {s['seriesID']}")