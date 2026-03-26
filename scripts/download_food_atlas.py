"""
One-time download: USDA Food Environment Atlas
Filters for St. Louis County (FIPS 29189) and St. Louis City (FIPS 29510).
Saves a local JSON slice to data/stl_food_environment.json.

Run once:
    python scripts/download_food_atlas.py

The output file is committed to the repo and read by the pipeline directly.
Do NOT call this from the main ETL — the full atlas is 15MB+.
"""
import json
import sys
from pathlib import Path

import pandas as pd
import requests

ATLAS_URL = (
    "https://www.ers.usda.gov/webdocs/DataFiles/80591/FoodEnvironmentAtlas.xls"
)
STL_FIPS = {"29189", "29510"}  # St. Louis County and St. Louis City
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "stl_food_environment.json"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ETL-Bot/1.0"}


def download_atlas() -> pd.DataFrame:
    print(f"Downloading Food Environment Atlas from:\n  {ATLAS_URL}")
    response = requests.get(ATLAS_URL, headers=HEADERS, timeout=120)
    response.raise_for_status()

    # The atlas is a multi-sheet Excel file; 'Supplemental Data - County' holds FIPS codes
    xl = pd.ExcelFile(pd.io.common.BytesIO(response.content))
    print(f"Sheets available: {xl.sheet_names}")

    # Try common sheet names; adjust if USDA renames them
    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        fips_col = next((c for c in df.columns if "fips" in c.lower()), None)
        if fips_col:
            df[fips_col] = df[fips_col].astype(str).str.zfill(5)
            stl = df[df[fips_col].isin(STL_FIPS)]
            if not stl.empty:
                print(f"Found {len(stl)} rows in sheet '{sheet}'")
                return stl

    print("ERROR: Could not find FIPS column in any sheet.", file=sys.stderr)
    sys.exit(1)


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = download_atlas()
    records = df.where(pd.notna(df), None).to_dict("records")
    with open(OUTPUT_PATH, "w") as f:
        json.dump(records, f, indent=2)
    print(f"\n✅ Saved {len(records)} rows to {OUTPUT_PATH}")
    print("Commit this file to the repo — do not re-run unless the Atlas is updated.")


if __name__ == "__main__":
    main()
