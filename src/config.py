import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Project Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = BASE_DIR / "data" / "raw"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"

DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# API Keys
FRED_API_KEY = os.getenv("FRED_API_KEY")
BLS_API_KEY = os.getenv("BLS_API_KEY")

# --- Refined Data Selection (Dictionary Format) ---
# Format: "Human_Readable_Name": "Technical_Series_ID"

FRED_SERIES = {
    # Primary Consumption & Sentiment
    "PCE_NOMINAL": "PCEC",        # Personal Consumption Expenditures
    "PCE_REAL": "PCECC96",        # Real PCE (Inflation Adjusted)
    "RETAIL_SALES": "RSXFS",      # Retail Sales (Excl. Food)
    "SENTIMENT": "UMCSENT",       # Consumer Sentiment
    
    # Macro Drivers
    "CPI_ALL": "CPIAUCSL",        # Consumer Price Index [cite: 1]
    "GDP_REAL": "GDPC1",          # Real GDP
    "UNRATE": "UNRATE",           # Unemployment Rate
    "SAVINGS_RATE": "PSAVERT",    # Personal Saving Rate
    "MONEY_COST": "FEDFUNDS",     # Fed Funds Rate
}

BLS_SERIES = {
    # Prices (The "Cost" of Goods/Services)
    "CPI_URBAN": "CUUR0000SA0",   # Headline CPI
    "CPI_CORE": "CUUR0000SA0L1E", # Core CPI (Ex-Food/Energy)
    "GAS_PRICE": "APU000074714",  # Avg Price: Gasoline
    
    # Labor (The "Income" for Spending)
    "AVG_WAGES": "CES0500000003", # Avg Hourly Earnings
    "WAGE_INDEX": "CIU2020000000000I" # Employment Cost Index
}

# --- Metadata Storage ---
DATA_METADATA_DIR = BASE_DIR / "data" / "metadata"
DATA_METADATA_DIR.mkdir(parents=True, exist_ok=True)