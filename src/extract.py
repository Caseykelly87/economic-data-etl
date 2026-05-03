import functools
import json
import logging
import hashlib
import re
import csv as _csv
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

import requests

from src.config import (
    FRED_API_KEY,
    BLS_API_KEY,
    ERS_SUMMARY_URL,
    DATA_RAW_DIR,
    DATA_METADATA_DIR,
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
    """Retry decorator with exponential backoff for transient network errors."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for attempt in range(3):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                logging.warning(
                    f"⚠️ Attempt {attempt+1} failed: {e}",
                    extra={
                        "source": "http_client",
                        "attempt": attempt + 1,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    raise
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
        logging.info(
            f"⏩ No changes detected for FRED {series_id}, skipping write",
            extra={"source": "fred", "series_id": series_id, "status": "skipped"},
        )
        return data

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

    logging.info(
        f"✅ Extracted / Updated FRED: {series_id}",
        extra={"source": "fred", "series_id": series_id, "status": "updated"},
    )
    return data


# ==========================================================
# BLS Extraction (Revision Aware Batch)
# ==========================================================

@fetch_with_retry
def fetch_bls_data(series_dict, start_year, end_year):
    """Batch-fetch all BLS series and persist a single raw JSON snapshot."""    

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
        logging.info(
            "⏩ No changes detected for BLS batch pull",
            extra={"source": "bls", "status": "skipped"},
        )
        return data

    filepath = get_storage_path("BLS", identifier)
    with open(filepath, "w") as f:
        json.dump(data, f)

    save_metadata("BLS", identifier, {
        "last_hash": new_hash,
        "last_updated": datetime.now().isoformat()
    })

    logging.info(
        "✅ Extracted / Updated BLS Batch",
        extra={"source": "bls", "status": "updated"},
    )
    return data


# ==========================================================
# ERS URL Discovery
# ==========================================================

_ERS_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_ERS_FALLBACK_URL = (
    "https://www.ers.usda.gov/media/6460/"
    "changes-in-consumer-price-indexes-2023-through-2026.csv"
)


def get_dynamic_ers_url() -> str:
    """
    Scrape the ERS Food Price Outlook page to find the current CSV link.
    ERS rotates media IDs on every monthly update.
    Returns the fallback URL if discovery fails.
    """
    try:
        response = requests.get(ERS_SUMMARY_URL, headers=_ERS_BROWSER_HEADERS, timeout=10)
        if response.status_code == 200:
                        match = re.search(
                r'href="([^"]*(?:consumer.price.index|CPIforecast|cpi_forecast|changes-in-consumer)[^"]*\.csv[^"]*)"',
                response.text,
                re.IGNORECASE,
            )
        if match:
                raw = match.group(1)
                url = raw if raw.startswith("http") else "https://www.ers.usda.gov" + raw
                logging.info(
                    f"ERS URL discovered: {url}",
                    extra={"source": "ers", "discovered_url": url, "status": "discovered"},
                )
                return url
    except Exception as e:
        logging.warning(
            f"ERS URL discovery failed: {e}",
            extra={
                "source": "ers",
                "status": "discovery_failed",
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )

    logging.warning(
        f"Using ERS fallback URL — update _ERS_FALLBACK_URL if this 404s: {_ERS_FALLBACK_URL}",
        extra={"source": "ers", "fallback_url": _ERS_FALLBACK_URL, "status": "using_fallback"},
    )
    return _ERS_FALLBACK_URL

# ==========================================================
# USDA ERS Extraction (Revision Aware)
# ==========================================================

@fetch_with_retry
def fetch_ers_price_outlook():
    """Download USDA ERS CPI Forecasts CSV and persist as a raw JSON snapshot."""

    identifier = "cpi_forecasts"
    metadata = load_metadata("ERS", identifier)

    csv_url = get_dynamic_ers_url()
    
    headers = _ERS_BROWSER_HEADERS
    response = requests.get(csv_url, headers=headers, timeout=15)
    if response.status_code == 404:
        raise ValueError(
            f"ERS CSV URL returned 404. Update _ERS_FALLBACK_URL in extract.py.\n"
            f"Get the current URL from: https://www.ers.usda.gov/data-products/food-price-outlook/"
        )
    response.raise_for_status()


    reader = _csv.DictReader(StringIO(response.text))
    rows = [dict(row) for row in reader]
    data = {"rows": rows}

    new_hash = compute_hash(data)
    old_hash = metadata.get("last_hash")

    if old_hash == new_hash:
        logging.info(
            "⏩ No changes detected for ERS CPI Forecasts, skipping write",
            extra={"source": "ers", "dataset": "cpi_forecasts", "status": "skipped"},
        )
        return data

    filepath = get_storage_path("ERS", identifier)
    with open(filepath, "w") as f:
        json.dump(data, f)

    save_metadata("ERS", identifier, {
        "last_hash": new_hash,
        "last_updated": datetime.now().isoformat(),
    })

    logging.info(
        "✅ Extracted / Updated ERS CPI Forecasts",
        extra={"source": "ers", "dataset": "cpi_forecasts", "status": "updated"},
    )
    return data
