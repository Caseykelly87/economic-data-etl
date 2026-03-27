"""
One-time download: USDA Food Environment Atlas
Filters for St. Louis County (FIPS 29189) and St. Louis City (FIPS 29510).

Outputs (both committed to the repo):
    data/stl_food_environment.json       — filtered ~50-row data slice
    data/stl_food_environment_meta.json  — provenance: URL, version, date, row count

Run once, or whenever ERS publishes an Atlas update:
    python scripts/download_food_atlas.py

Re-run instructions are embedded in the meta file.
Do NOT call this from the main ETL pipeline.

Atlas data page:
    https://www.ers.usda.gov/data-products/food-environment-atlas/data-access-and-documentation-downloads/
"""
import json
import re
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration — update _FALLBACK_ATLAS_URL when ERS rotates the file
# ---------------------------------------------------------------------------

_DATA_PAGE_URL = (
    "https://www.ers.usda.gov/data-products/food-environment-atlas/"
    "data-access-and-documentation-downloads/"
)

# Right-click the XLSX button on _DATA_PAGE_URL → Copy link address → paste here
_FALLBACK_ATLAS_URL = "https://www.ers.usda.gov/media/5569/food-environment-atlas-data-download.xlsx?v=61287"

STL_FIPS = {"29189", "29510"}  # St. Louis County and St. Louis City

_OUTPUT_DIR  = Path(__file__).resolve().parent.parent / "data"
_OUTPUT_DATA = _OUTPUT_DIR / "stl_food_environment.json"
_OUTPUT_META = _OUTPUT_DIR / "stl_food_environment_meta.json"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


# ---------------------------------------------------------------------------
# URL Discovery
# ---------------------------------------------------------------------------

def get_atlas_url() -> str:
    """
    Attempt to discover the current XLSX URL by scraping the ERS data page.
    Falls back to _FALLBACK_ATLAS_URL if the page is inaccessible or the
    pattern doesn't match (ERS frequently blocks automated requests).
    """
    try:
        resp = requests.get(_DATA_PAGE_URL, headers=_HEADERS, timeout=10)
        if resp.status_code == 200:
            match = re.search(
                r'href="([^"]*(?:FoodEnvironmentAtlas|DataDownload|food.environment)[^"]*\.xlsx[^"]*)"',
                resp.text,
                re.IGNORECASE,
            )
            if match:
                raw = match.group(1)
                url = raw if raw.startswith("http") else "https://www.ers.usda.gov" + raw
                print(f"Atlas URL discovered via page scrape: {url}")
                return url
        print(f"Discovery page returned HTTP {resp.status_code} — using fallback.")
    except Exception as e:
        print(f"Discovery failed ({e}) — using fallback.")

    if _FALLBACK_ATLAS_URL == "PASTE_ATLAS_XLSX_URL_HERE":
        print(
            "\nERROR: No Atlas URL discovered and _FALLBACK_ATLAS_URL is not set.\n"
            "\nTo fix:\n"
            f"  1. Open in your browser: {_DATA_PAGE_URL}\n"
            "  2. Right-click the XLSX download button → Copy link address\n"
            "  3. Paste it into _FALLBACK_ATLAS_URL in scripts/download_food_atlas.py\n"
        )
        sys.exit(1)

    print(f"Using fallback URL: {_FALLBACK_ATLAS_URL}")
    return _FALLBACK_ATLAS_URL


# ---------------------------------------------------------------------------
# Download and Filter
# ---------------------------------------------------------------------------

def download_and_filter(url: str):
    print("Downloading Food Environment Atlas (8MB+)...")
    resp = requests.get(url, headers=_HEADERS, timeout=120)
    resp.raise_for_status()

    xl = pd.ExcelFile(BytesIO(resp.content))
    # We will join all these sheets into one St. Louis record
    data_sheets = ['STORES', 'RESTAURANTS', 'ACCESS', 'ASSISTANCE', 'INSECURITY', 'TAXES', 'LOCAL', 'HEALTH', 'SOCIOECONOMIC']
    
    # Initialize a master dataframe with FIPS as index
    combined_stl = pd.DataFrame(index=list(STL_FIPS))
    combined_stl.index.name = 'FIPS'

    for sheet in data_sheets:
        if sheet not in xl.sheet_names:
            continue
            
        # Robust Header Detection: Find the row containing 'FIPS'
        temp_df = xl.parse(sheet, nrows=10, header=None)
        header_row = None
        fips_col_idx = None
        
        for idx, row in temp_df.iterrows():
            row_values = [str(v).lower() for v in row]
            if any("fips" in v for v in row_values):
                header_row = idx
                fips_col_idx = row_values.index(next(v for v in row_values if "fips" in v))
                break
        
        if header_row is None:
            continue

        # Load the sheet with the correct header
        df = xl.parse(sheet, skiprows=header_row)
        fips_col = df.columns[fips_col_idx]
        
        # Robust FIPS normalization: handles '29189.0' or '29189'
        df[fips_col] = pd.to_numeric(df[fips_col], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(5)
        
        # Filter for St. Louis and drop duplicate geo columns (State/County names)
        stl_rows = df[df[fips_col].isin(STL_FIPS)].copy()
        stl_rows = stl_rows.set_index(fips_col)
        
        # Drop common columns that exist in every sheet to avoid suffixes
        cols_to_drop = [c for c in ['State', 'County', 'STATE', 'COUNTY'] if c in stl_rows.columns]
        stl_rows = stl_rows.drop(columns=cols_to_drop)
        
        combined_stl = combined_stl.join(stl_rows, how='left')
        print(f"  Joined {len(stl_rows)} STL rows from sheet '{sheet}'")

    if combined_stl.empty:
        print("ERROR: No St. Louis data found across sheets.")
        sys.exit(1)
        
    sheets_joined = ", ".join(data_sheets)
    return combined_stl.reset_index(), sheets_joined


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    url = get_atlas_url()
    df, sheet = download_and_filter(url)

    # Data file
    records = df.where(pd.notna(df), None).to_dict("records")
    with open(_OUTPUT_DATA, "w") as f:
        json.dump(records, f, indent=2)

    # Metadata file — provenance record committed alongside the data
    meta = {
        "atlas_source_url": url,
        "atlas_sheet": sheet,
        "downloaded": datetime.now().strftime("%Y-%m-%d"),
        "fips_codes": sorted(STL_FIPS),
        "fips_labels": {"29189": "St. Louis County, MO", "29510": "St. Louis City, MO"},
        "row_count": len(records),
        "data_page": _DATA_PAGE_URL,
        "note": (
            "Re-run scripts/download_food_atlas.py when ERS publishes an Atlas update. "
            "Check data_page for release announcements. Commit both output files after re-running."
        ),
    }
    with open(_OUTPUT_META, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\n✅  {len(records)} rows  →  {_OUTPUT_DATA}")
    print(f"✅  Metadata        →  {_OUTPUT_META}")
    print("\nCommit both files to the repo.")
    print(f"Re-run only when the Atlas is updated (see 'data_page' in the meta file).")


if __name__ == "__main__":
    main()
