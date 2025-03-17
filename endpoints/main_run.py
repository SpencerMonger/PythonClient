from endpoints.main import main
from datetime import datetime
import asyncio
import time
from typing import Literal
import argparse

# List of tickers to process
tickers = ["AAPL", "AMZN", "TSLA", "NVDA", "MSFT", "GOOGL", "META", "AMD"]

# Mode configuration
TimeseriesMode = Literal["historical", "live"]

def run_data_collection(mode: TimeseriesMode = "historical", 
                       from_date: datetime | None = None,
                       to_date: datetime | None = None,
                       store_latest_only: bool = False) -> None:
    """
    Run data collection in either historical or live mode
    
    Args:
        mode: Either "historical" or "live"
        from_date: Start date for historical mode
        to_date: End date for historical mode
        store_latest_only: Whether to only store the latest row per ticker
    """
    if mode == "historical":
        if not from_date or not to_date:
            raise ValueError("Historical mode requires from_date and to_date")
            
        print(f"\nStarting historical data processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Processing {len(tickers)} tickers from {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
        
    else:  # live mode
        # Use current date for live mode
        from_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        to_date = datetime.now()
        
        print(f"\nStarting live data processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Processing {len(tickers)} tickers for current date {from_date.strftime('%Y-%m-%d')}")
    
    start_time = time.time()
    asyncio.run(main(tickers, from_date, to_date, store_latest_only))
    print(f"\nScript completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total wall clock time: {time.time() - start_time:.2f} seconds")

def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data collection in historical or live mode")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["historical", "live"],
        default="historical",
        help="Data collection mode: historical or live"
    )
    parser.add_argument(
        "--from-date",
        type=parse_date,
        help="Start date for historical mode (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--to-date",
        type=parse_date,
        help="End date for historical mode (YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    
    # Run in specified mode
    if args.mode == "historical":
        if not args.from_date or not args.to_date:
            # Use default dates if not provided
            from_date = datetime(2025, 3, 1)
            to_date = datetime(2025, 3, 14)
        else:
            from_date = args.from_date
            to_date = args.to_date
            
        run_data_collection(
            mode="historical",
            from_date=from_date,
            to_date=to_date
        )
    else:  # live mode
        run_data_collection(
            mode="live",
            store_latest_only=True
        )
