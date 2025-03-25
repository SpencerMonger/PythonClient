from endpoints.main import main
from datetime import datetime, timedelta
import asyncio
import time
from typing import Literal
import argparse
import pytz

# List of tickers to process
tickers = ["AAPL",]

# Mode configuration
TimeseriesMode = Literal["historical", "live"]

async def run_data_collection(mode: str = "historical", store_latest_only: bool = False, from_date: datetime = None, to_date: datetime = None) -> None:
    """
    Run data collection in either historical or live mode
    
    Args:
        mode: Either "historical" or "live"
        store_latest_only: Whether to only store the latest row per ticker
        from_date: Optional start date (used in live mode)
        to_date: Optional end date (used in live mode)
    """
    if mode == "historical":
        print(f"\nStarting {mode} data processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        # For historical mode, use provided dates or defaults
        if from_date is None or to_date is None:
            to_date = datetime.now()
            from_date = to_date - timedelta(days=2)  # 2 days of historical data
        print(f"Processing {len(tickers)} tickers for date range {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    else:
        # For live mode, use Eastern Time
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        print(f"\nStarting {mode} data processing at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        # For live mode, use provided dates or default to current minute
        if from_date is None or to_date is None:
            # Calculate the most recently completed minute
            current_minute = now.replace(second=0, microsecond=0)
            from_date = current_minute - timedelta(minutes=1)  # Start one minute before current
            to_date = current_minute  # End at the current minute
            
        print(f"Processing data for {from_date.strftime('%H:%M:00')} - {to_date.strftime('%H:%M:00')} ET")
    
    # Run main data collection
    await main(tickers, from_date, to_date, store_latest_only)

def parse_args():
    parser = argparse.ArgumentParser(description='Run data collection in historical or live mode')
    parser.add_argument('--mode', type=str, choices=['historical', 'live'], default='historical',
                      help='Mode to run in (historical or live)')
    parser.add_argument('--from-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                      help='Start date for historical mode (YYYY-MM-DD)')
    parser.add_argument('--to-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                      help='End date for historical mode (YYYY-MM-DD)')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run_data_collection(
        mode=args.mode,
        store_latest_only=(args.mode == "live"),
        from_date=args.from_date,
        to_date=args.to_date
    ))
