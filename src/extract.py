import requests
import json
import logging
import time
import hashlib
from datetime import datetime
from pathlib import Path
from src.config import (
    FRED_API_KEY,
    BLS_API_KEY,
    DATA_RAW_DIR,
    DATA_METADATA_DIR
)

# ==========================================================
# Utility Functions
# ==========================================================

def compute_hash(data: dict) -> str:
    """Create SHA256 hash of JSON data for revision detection."""
    encoded = json.dumps(data, sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def load_metadata(source: str, identifier: str) -> dict:
    """Load metadata file if exists."""
    metadata_path = DATA_METADATA_DIR / f"{source}_{identifier}_metadata.json"
    if metadata_path.exists():
        with open(metadata_path, "r") as f:
            return json.load(f)
    return {}


def save_metadata(source: str, identifier: str, metadata: dict):
    """Persist metadata to disk."""
    metadata_path = DATA_METADATA_DIR / f"{source}_{identifier}_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)


def get_storage_path(source, identifier):
    """Daily snapshot naming convention."""
    datestamp = datetime.now().strftime("%Y_%m_%d")
    filename = f"{source}_{identifier}_{datestamp}.json"
    return DATA_RAW_DIR / filename


def fetch_with_retry(func):
    """Retry decorator with exponential backoff."""
    def wrapper(*args, **kwargs):
        for attempt in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.warning(f"⚠️ Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    raise e
    return wrapper


# ==========================================================
# FRED Extraction (Incremental + Revision Aware)
# ==========================================================

@fetch_with_retry
def fetch_fred_data(series_id):

    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY not set.")

    metadata = load_metadata("FRED", series_id)
    last_observation_date = metadata.get("last_observation_date")

    url = "https://api.stlouisfed.org/fred/series/observations"

    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }

    if last_observation_date:
        params["observation_start"] = last_observation_date

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()

    if "observations" not in data:
        raise ValueError(f"Malformed FRED response for {series_id}")

    new_hash = compute_hash(data.get("observations", []))
    old_hash = metadata.get("last_hash")

    if old_hash == new_hash:
        logging.debug(f"No changes detected for FRED {series_id}")
        return

    filepath = get_storage_path("FRED", series_id)
    with open(filepath, "w") as f:
        json.dump(data, f)

    # Update metadata
    observations = data.get("observations", [])
    if observations:
        latest_date = observations[-1]["date"]
    else:
        latest_date = last_observation_date

    save_metadata("FRED", series_id, {
        "last_observation_date": latest_date,
        "last_hash": new_hash,
        "last_updated": datetime.now().isoformat()
    })

    logging.info(f"✅ Extracted / Updated FRED: {series_id}")


# ==========================================================
# BLS Extraction (Revision Aware Batch)
# ==========================================================

@fetch_with_retry
def fetch_bls_data(series_dict, start_year, end_year):

    if not BLS_API_KEY:
        raise ValueError("BLS_API_KEY not set.")

    identifier = "batch_pull"
    metadata = load_metadata("BLS", identifier)

    series_ids = list(series_dict.values())

    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    headers = {'Content-type': 'application/json'}
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY
    }

    response = requests.post(url, json=payload, headers=headers, timeout=15)
    response.raise_for_status()

    data = response.json()

    if data.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API Error: {data.get('message')}")

    new_hash = compute_hash(data)
    old_hash = metadata.get("last_hash")

    if old_hash == new_hash:
        logging.info("⏩ No changes detected for BLS batch pull")
        return

    filepath = get_storage_path("BLS", identifier)
    with open(filepath, "w") as f:
        json.dump(data, f)

    save_metadata("BLS", identifier, {
        "last_hash": new_hash,
        "last_updated": datetime.now().isoformat()
    })

    logging.info("✅ Extracted / Updated BLS Batch")