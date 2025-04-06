import logging
import os # Added for path joining
from endpoints.main import main
from datetime import datetime, timedelta, timezone
import asyncio
import time
from typing import Literal
import argparse
import pytz

# --- Logging Configuration ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger() # Get the root logger
logger.setLevel(logging.INFO) # Set the minimum level for the logger

# Console Handler (StreamHandler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# File Handler (FileHandler)
# Ensure the log file is in the same directory as the script
log_file_path = os.path.join(os.path.dirname(__file__), 'main_run.log') 
file_handler = logging.FileHandler(log_file_path)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)
# ---------------------------

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
        logging.info(f"Starting {mode} data processing.")
        # For historical mode, use provided dates or defaults
        if log_from_date is None or log_to_date is None:
            log_to_date = datetime.now()
            log_from_date = log_to_date - timedelta(days=2)  # 2 days of historical data
        logging.info(f"Processing {len(tickers)} tickers concurrently for date range {log_from_date.strftime('%Y-%m-%d')} to {log_to_date.strftime('%Y-%m-%d')}")
    elif mode == "live":
        # Live mode expects UTC dates to be passed in
        if log_from_date is None or log_to_date is None:
            # Fallback if not called from live_data.py (e.g., direct run)
            logging.warning("Live mode called without specific UTC time range, using fallback (last minute ET).")
            et_tz = pytz.timezone('US/Eastern')
            now_et = datetime.now(et_tz)
            current_minute_et = now_et.replace(second=0, microsecond=0)
            log_from_date = current_minute_et - timedelta(minutes=1)
            log_to_date = current_minute_et
            logging.info(f"Starting {mode} data processing at {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')} (Fallback ET)")
            logging.info(f"Processing {len(tickers)} tickers concurrently for fallback ET range: {log_from_date.strftime('%H:%M:%S')} - {log_to_date.strftime('%H:%M:%S')}")
        else:
            # Use the provided UTC dates
            now_utc = datetime.now(timezone.utc)
            logging.info(f"Starting {mode} data processing at {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            logging.info(f"Processing {len(tickers)} tickers concurrently for UTC range: {log_from_date.strftime('%Y-%m-%d %H:%M:%S')} - {log_to_date.strftime('%Y-%m-%d %H:%M:%S')}")
            # Log ET equivalent for reference
            et_tz = pytz.timezone('US/Eastern')
            log_from_et = log_from_date.astimezone(et_tz)
            log_to_et = log_to_date.astimezone(et_tz)
            logging.info(f"Equivalent ET range: {log_from_et.strftime('%Y-%m-%d %H:%M:%S %Z')} - {log_to_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Use the potentially updated log_from_date and log_to_date for processing
    # These are the actual dates that will be passed to main()
    actual_from_date = log_from_date
    actual_to_date = log_to_date

    try:
        # Removed the outer timeout calculation and asyncio.wait_for wrapper
        # Timeout and retries are handled within main() now
        # days_in_range = (actual_to_date - actual_from_date).days + 1 if actual_to_date and actual_from_date else 1
        # timeout = 10.0 if mode == "live" else max(600.0, days_in_range * 120.0)
        # logging.info(f"Setting overall collection timeout to {timeout:.1f} seconds")
        
        # Directly call main without the outer wait_for
        await main(tickers, actual_from_date, actual_to_date, store_latest_only)
        
        elapsed = time.time() - start_time
        logging.info(f"Data collection completed successfully in {elapsed:.2f} seconds")

    # Adjusted exception handling as outer timeout is removed
    except Exception as e:
        elapsed = time.time() - start_time
        # Use logging.exception to include traceback automatically
        logging.error(f"Error during data collection: {str(e)} (after {elapsed:.2f} seconds)")
        logging.exception("Detailed traceback:") # Logs exception info including traceback
        # Consider if we need to differentiate between errors from main() vs other setup steps

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
    logging.info(f"Starting main_run script in {args.mode} mode.")
    asyncio.run(run_data_collection(
        mode=args.mode,
        store_latest_only=(args.mode == "live"),
        from_date=args.from_date,
        to_date=args.to_date
    ))
