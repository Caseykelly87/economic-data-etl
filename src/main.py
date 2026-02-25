import logging
from src.extract import fetch_fred_data, fetch_bls_data
from src.config import FRED_SERIES, BLS_SERIES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    logging.info("🚀 Starting Economic Data ETL Pipeline")

    try:
        # 1. Extraction Phase
        logging.info("Extracting data from FRED...")
        for name, series_id in FRED_SERIES.items():
            fetch_fred_data(series_id)
            
        logging.info("Extracting data from BLS...")
        # BLS is called as a batch to be efficient
        fetch_bls_data(BLS_SERIES, 2021, 2026)
        
    except Exception as e:
        logging.error(f"Pipeline failed during Extraction: {e}")
        return

    logging.info("✅ Extraction Complete. Raw data stored in data/raw/")

if __name__ == "__main__":
    run_pipeline()