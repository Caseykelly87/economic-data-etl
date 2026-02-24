from src.extract import fetch_fred_data
from src.config import ECONOMIC_SERIES

def main():
    print("🚀 Starting ETL Pipeline...")
    
    for series in ECONOMIC_SERIES:
        try:
            fetch_fred_data(series)
            print(f"✅ Successfully ingested {series}")
        except Exception as e:
            print(f"❌ Failed to ingest {series}: {e}")

if __name__ == "__main__":
    main()