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

# Database
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///" + (BASE_DIR / "data" / "economic_data.db").as_posix(),
)

# API Keys
FRED_API_KEY = os.getenv("FRED_API_KEY")
BLS_API_KEY = os.getenv("BLS_API_KEY")
ERS_SUMMARY_URL = "https://www.ers.usda.gov/data-products/food-price-outlook/summary-findings/"


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

    # Grocery / Retail (Missouri)
    "GROCERY_SALES_MO": "MSRSMO445",  # MO Food & Beverage Stores YoY % change (NAICS 445)
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


# --- ERS Food Price Outlook Configuration ---
# Mapping from ERS CSV category strings to internal series IDs (used by transform)
ERS_CATEGORY_MAP = {
    "All food": "ERS_ALL_FOOD",
    "Food at home": "ERS_FOOD_HOME",
    "Food away from home": "ERS_FOOD_AWAY",
    "Cereals and bakery products": "ERS_CEREALS",
    "Meats, poultry, and fish": "ERS_MEATS",
    "Dairy products": "ERS_DAIRY",
    "Fruits and vegetables": "ERS_FRUITS_VEG",
    "Nonalcoholic beverages and beverage materials": "ERS_BEVERAGES",
}

# ERS dimension entries (name -> series_id)
ERS_SERIES = {sid: sid for sid in ERS_CATEGORY_MAP.values()}