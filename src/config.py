import os
from dotenv import load_dotenv

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
BASE_DATA_DIR = "data/raw"
# Add your Series IDs here (e.g., UNRATE for unemployment)
ECONOMIC_SERIES = ["UNRATE", "CPIAUCSL", "GDP"]