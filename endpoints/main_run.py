from endpoints.main import main
from datetime import datetime, timedelta, timezone
import asyncio
import time
from typing import Literal
import argparse
import pytz

# List of tickers to process
tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]

# Mode configuration
TimeseriesMode = Literal["historical", "live"]

async def run_data_collection(mode: str = "historical", store_latest_only: bool = False, from_date: datetime = None, to_date: datetime = None) -> None:
    """
    Run data collection in either historical or live mode
    All tickers are processed concurrently to maximize throughput
    
    Args:
        mode: Either "historical" or "live"
        store_latest_only: Whether to only store the latest row per ticker
        from_date: Optional start date (expects UTC in live mode if provided)
        to_date: Optional end date (expects UTC in live mode if provided)
    """
    start_time = time.time()
    
    log_from_date = from_date
    log_to_date = to_date
    
    if mode == "historical":
        print(f"\nStarting {mode} data processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        # For historical mode, use provided dates or defaults
        if log_from_date is None or log_to_date is None:
            log_to_date = datetime.now()
            log_from_date = log_to_date - timedelta(days=2)  # 2 days of historical data
        print(f"Processing {len(tickers)} tickers concurrently for date range {log_from_date.strftime('%Y-%m-%d')} to {log_to_date.strftime('%Y-%m-%d')}")
    elif mode == "live":
        # Live mode expects UTC dates to be passed in
        if log_from_date is None or log_to_date is None:
            # Fallback if not called from live_data.py (e.g., direct run)
            print("Warning: Live mode called without specific UTC time range, using fallback (last minute ET).")
            et_tz = pytz.timezone('US/Eastern')
            now_et = datetime.now(et_tz)
            current_minute_et = now_et.replace(second=0, microsecond=0)
            log_from_date = current_minute_et - timedelta(minutes=1)
            log_to_date = current_minute_et
            print(f"\nStarting {mode} data processing at {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')} (Fallback ET)")
            print(f"Processing {len(tickers)} tickers concurrently for fallback ET range: {log_from_date.strftime('%H:%M:%S')} - {log_to_date.strftime('%H:%M:%S')}")
        else:
            # Use the provided UTC dates
            now_utc = datetime.now(timezone.utc)
            print(f"\nStarting {mode} data processing at {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"Processing {len(tickers)} tickers concurrently for UTC range: {log_from_date.strftime('%Y-%m-%d %H:%M:%S')} - {log_to_date.strftime('%Y-%m-%d %H:%M:%S')}")
            # Log ET equivalent for reference
            et_tz = pytz.timezone('US/Eastern')
            log_from_et = log_from_date.astimezone(et_tz)
            log_to_et = log_to_date.astimezone(et_tz)
            print(f"Equivalent ET range: {log_from_et.strftime('%Y-%m-%d %H:%M:%S %Z')} - {log_to_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Use the potentially updated log_from_date and log_to_date for processing
    # These are the actual dates that will be passed to main()
    actual_from_date = log_from_date
    actual_to_date = log_to_date

    try:
        # In live mode, we need to be even more aggressive with timeouts
        # Set a stricter timeout for data collection
        # Calculate how much time we need based on date range and mode
        # Use a slightly longer base timeout for live mode to accommodate DB operations in main()
        # Use max() to ensure historical has enough time even for short ranges
        days_in_range = (actual_to_date - actual_from_date).days + 1 if actual_to_date and actual_from_date else 1
        timeout = 10.0 if mode == "live" else max(600.0, days_in_range * 120.0)  # Increased live timeout slightly

        print(f"Setting overall collection timeout to {timeout:.1f} seconds")
        
        try:
            # Run main data collection with concurrent ticker processing and a timeout
            # Pass the actual dates determined above
            await asyncio.wait_for(
                main(tickers, actual_from_date, actual_to_date, store_latest_only),
                timeout=timeout
            )
            
            elapsed = time.time() - start_time
            print(f"Data collection completed in {elapsed:.2f} seconds")
        except asyncio.TimeoutError:
            elapsed = time.time() - start_time
            print(f"Data collection timed out after {elapsed:.2f} seconds - continuing with available data")
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"Error in data collection: {str(e)} (after {elapsed:.2f} seconds)")
            # Don't re-raise to allow continued execution with partial data
    
    # Final catch-all to ensure we don't crash the main loop
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"Unexpected error in run_data_collection: {str(e)} (after {elapsed:.2f} seconds)")
        # Continue execution despite errors

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
