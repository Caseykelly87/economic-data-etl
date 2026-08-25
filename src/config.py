import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Project Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = BASE_DIR / "data" / "raw"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"

# Database
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///" + (BASE_DIR / "data" / "economic_data.db").as_posix(),
)

# API Keys
FRED_API_KEY = os.getenv("FRED_API_KEY")
BLS_API_KEY = os.getenv("BLS_API_KEY")
ERS_SUMMARY_URL = "https://www.ers.usda.gov/data-products/food-price-outlook/summary-findings/"

# Macro extraction windows
BLS_START_YEAR: int = 2021  # Earliest year fetched from BLS
ERS_START_YEAR: int = 2024  # Earliest year fetched from ERS food price outlook


# --- Refined Data Selection (Dictionary Format) ---
# Format: "Human_Readable_Name": "Technical_Series_ID"

FRED_SERIES = {
    # Primary Consumption & Sentiment
    "PCE_NOMINAL": "PCEC",        # Personal Consumption Expenditures
    "PCE_REAL": "PCECC96",        # Real PCE (Inflation Adjusted)
    "RETAIL_SALES": "RSXFS",      # Retail Sales (Excl. Food)
    "SENTIMENT": "UMCSENT",       # Consumer Sentiment

    # Macro Drivers
    "CPI_ALL": "CPIAUCSL",        # Consumer Price Index
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
    "WAGE_INDEX": "CIU2020000000000I", # Employment Cost Index

    # Food-category CPI indexes (monthly, U.S. city average, NSA). These
    # carry the ERS_ prefix because the sim engine's realism layer queries
    # raw.fact_economic_observations by these exact series_names — the
    # names predate the switch to BLS as the source and are kept stable
    # across the platform. The layer's multiplier math needs monthly index
    # levels; the annual ERS forecast series below (ERS_FORECAST_*) are the
    # wrong granularity for it and use distinct names so the two never mix.
    "ERS_ALL_FOOD":   "CUUR0000SAF1",    # Food
    "ERS_FOOD_HOME":  "CUUR0000SAF11",   # Food at home
    "ERS_FOOD_AWAY":  "CUUR0000SEFV",    # Food away from home
    "ERS_CEREALS":    "CUUR0000SAF111",  # Cereals and bakery products
    "ERS_MEATS":      "CUUR0000SAF112",  # Meats, poultry, fish, and eggs
    "ERS_DAIRY":      "CUUR0000SEFJ",    # Dairy and related products
    "ERS_FRUITS_VEG": "CUUR0000SAF113",  # Fruits and vegetables
    "ERS_BEVERAGES":  "CUUR0000SAF114",  # Nonalcoholic beverages
}

# --- Analytical mart domain routing ---
# Maps a domain mart to the technical series_ids it projects from staging.
# Keyed by series_id (the value stored in raw.fact_economic_observations.
# series_id), not the human-readable config key. The split mirrors the
# consumer contract the API serves: /metrics/inflation reads CPI and gas
# prices, /metrics/unemployment reads the unemployment rate and wage series,
# /metrics/gdp reads real GDP.
#
# A series_id absent from every domain here is not dropped: it is still
# carried into mart_economic_summary, which holds the latest observation for
# every ingested series. The three domain marts are deliberate subsets of the
# warehouse the API exposes as named metric routes; the summary is the
# no-loss rollup across all series (consumption, sentiment, savings, the fed
# funds rate, the Missouri grocery-sales series, and the ERS food-price
# forecasts reach the serving layer through the summary, which is the only
# mart the API populates from every series).
MART_DOMAINS: dict[str, list[str]] = {
    "inflation": [
        "CPIAUCSL",        # CPI, All Urban Consumers (FRED)
        "CUUR0000SA0",     # Headline CPI (BLS)
        "CUUR0000SA0L1E",  # Core CPI, ex food/energy (BLS)
        "APU000074714",    # Avg price: gasoline (BLS)
    ],
    "labor_market": [
        "UNRATE",              # Unemployment rate (FRED)
        "CES0500000003",       # Avg hourly earnings (BLS)
        "CIU2020000000000I",   # Employment Cost Index (BLS)
    ],
    "gdp": [
        "GDPC1",  # Real GDP (FRED)
    ],
}


# --- Metadata Storage ---
DATA_METADATA_DIR = BASE_DIR / "data" / "metadata"


# --- ERS Food Price Outlook Configuration ---
# Mapping from ERS CSV category strings to internal series IDs (used by
# transform). These are annual year-over-year forecast percentages, not
# index levels — the FORECAST_ segment keeps them apart from the monthly
# ERS_* CPI series in BLS_SERIES above, which the sim engine's realism
# layer consumes. Before the split, the forecasts sat under the plain
# ERS_* names and the realism layer's DB mode read annual percentages
# as if they were monthly index levels.
ERS_CATEGORY_MAP = {
    "All food": "ERS_FORECAST_ALL_FOOD",
    "Food at home": "ERS_FORECAST_FOOD_HOME",
    "Food away from home": "ERS_FORECAST_FOOD_AWAY",
    "Cereals and bakery products": "ERS_FORECAST_CEREALS",
    "Meats, poultry, and fish": "ERS_FORECAST_MEATS",
    "Dairy products": "ERS_FORECAST_DAIRY",
    "Fruits and vegetables": "ERS_FORECAST_FRUITS_VEG",
    "Nonalcoholic beverages and beverage materials": "ERS_FORECAST_BEVERAGES",
}

# ERS dimension entries (name -> series_id)
ERS_SERIES = {sid: sid for sid in ERS_CATEGORY_MAP.values()}


def bootstrap_paths() -> None:
    """Create the data directories the pipeline writes to.

    Called explicitly by the pipeline entry point. Importing this
    module no longer creates directories — that simplifies test setup
    and avoids side-effecting any consumer that imports FRED_SERIES,
    BLS_SERIES, or DATABASE_URL but doesn't run the pipeline.
    """
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    DATA_METADATA_DIR.mkdir(parents=True, exist_ok=True)
