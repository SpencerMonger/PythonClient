import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List
import time
import hashlib
import pytz

from endpoints.polygon_client import get_rest_client
from endpoints.db import ClickHouseDB
from endpoints import config

# Schema for stock_bars table
BARS_SCHEMA = {
    "ticker": "String",
    "timestamp": "DateTime64(9)",  # Nanosecond precision
    "open": "Nullable(Float64)",
    "high": "Nullable(Float64)",
    "low": "Nullable(Float64)",
    "close": "Nullable(Float64)",
    "volume": "Nullable(Int64)",
    "vwap": "Nullable(Float64)",
    "transactions": "Nullable(Int64)"
}

async def fetch_bars(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch daily bar data for a ticker between dates (using UTC).
    Adjusts date range in live mode. Returns UTC timestamps.
    """
    client = get_rest_client()
    bars_list = []

    # Adjust date range for live mode if necessary (using incoming UTC dates)
    time_diff_hours = (to_date - from_date).total_seconds() / 3600
    is_live_mode = time_diff_hours < 2 # Check if range is less than 2 hours for live mode trigger

    fetch_from_date = from_date
    fetch_to_date = to_date

    if is_live_mode:
        print(f"Detected live mode for daily bars (time range: {time_diff_hours:.2f} hours)")
        # Get current date in UTC
        today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        # Fetch slightly wider range to be safe: yesterday and today according to UTC
        fetch_from_date = today_utc - timedelta(days=1)
        fetch_to_date = today_utc + timedelta(days=1) # Go up to tomorrow (exclusive)
        print(f"Adjusted date range for daily bars (UTC): {fetch_from_date.strftime('%Y-%m-%d')} to {fetch_to_date.strftime('%Y-%m-%d')}")

    # Format dates as YYYY-MM-DD for the API call
    from_str = fetch_from_date.strftime("%Y-%m-%d")
    to_str = fetch_to_date.strftime("%Y-%m-%d")
    print(f"Fetching daily bars for {ticker} from {from_str} to {to_str}")

    try:
        # Make the API call
        response = client.list_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",
            from_=from_str,
            to=to_str,
            limit=50000 # Adjust limit as needed
        )

        print(f"API response received, processing daily bars...")
        bar_count = 0
        for bar in response:
            bar_count += 1
            # Convert timestamp from milliseconds to UTC datetime
            timestamp_ms = bar.timestamp
            timestamp_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)

            # Ensure timestamp is normalized to the start of day *in UTC*
            timestamp_utc = timestamp_utc.replace(hour=0, minute=0, second=0, microsecond=0)

            # Debug: print exact timestamp
            # print(f"Processing bar for {ticker} on {timestamp_utc.strftime('%Y-%m-%d %Z')}")

            bars_list.append({
                "ticker": ticker,
                "timestamp": timestamp_utc, # Store UTC datetime
                "open": float(bar.open) if bar.open is not None else None,
                "high": float(bar.high) if bar.high is not None else None,
                "low": float(bar.low) if bar.low is not None else None,
                "close": float(bar.close) if bar.close is not None else None,
                "volume": int(bar.volume) if bar.volume is not None else None,
                "vwap": float(bar.vwap) if bar.vwap is not None else None,
                "transactions": int(bar.transactions) if bar.transactions is not None else None
            })

        print(f"Processed {bar_count} daily bars raw")
        if bars_list:
            print(f"Raw date range (UTC): {bars_list[0]['timestamp']} to {bars_list[-1]['timestamp']}")

            # Filter to only keep relevant days for live mode (yesterday/today UTC)
            if is_live_mode:
                today_utc_date = datetime.now(timezone.utc).date()
                yesterday_utc_date = today_utc_date - timedelta(days=1)

                filtered_bars = [
                    bar for bar in bars_list
                    if bar['timestamp'].date() in (yesterday_utc_date, today_utc_date)
                ]
                print(f"Filtered from {len(bars_list)} bars to {len(filtered_bars)} bars (yesterday/today UTC only)")
                bars_list = filtered_bars

    except Exception as e:
        print(f"Error fetching daily bars for {ticker}: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return []

    return bars_list

async def store_bars(db: ClickHouseDB, bars: List[Dict], mode: str = "historical") -> None:
    """
    Store daily bar data in ClickHouse using deduplication logic based on UTC date.
    Uses a temporary deduplication table. Stores UTC timestamps.

    Args:
        db: Database connection
        bars: List of bar data to store (with UTC timestamps)
        mode: Either "historical" or "live" (impacts logging only)
    """
    try:
        if not bars:
            print("No daily bars to store")
            return

        print(f"Processing {len(bars)} daily bars for storage in {mode} mode")

        # Standardize timestamps to ensure they are UTC and at midnight
        unique_dates_utc = set()
        for bar in bars:
            if isinstance(bar['timestamp'], datetime):
                 # Ensure it's UTC and at midnight
                 if bar['timestamp'].tzinfo is None:
                      bar['timestamp'] = pytz.UTC.localize(bar['timestamp']) # Assume UTC if naive
                 else:
                      bar['timestamp'] = bar['timestamp'].astimezone(pytz.UTC)
                 bar['timestamp'] = bar['timestamp'].replace(hour=0, minute=0, second=0, microsecond=0)
                 unique_dates_utc.add(bar['timestamp'].date())
            else:
                 # Handle case where timestamp might not be datetime (should not happen)
                 print(f"Warning: Invalid timestamp type for bar: {bar}")
                 continue # Skip this bar


        if not unique_dates_utc:
             print("No valid dates found in bars to process.")
             return

        unique_dates_str = [d.strftime('%Y-%m-%d') for d in sorted(list(unique_dates_utc))]
        print(f"Processing bars for UTC dates: {', '.join(unique_dates_str)}")

        # Step 1: Create a temporary table
        temp_table_name = f"temp_stock_daily_{int(time.time())}"
        # Use backticks for database and table names in SQL
        create_temp_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{db.database}`.`{temp_table_name}` AS
            `{db.database}`.`{config.TABLE_STOCK_DAILY}`
            ENGINE = Memory
        """
        db.client.command(create_temp_table_query)
        print(f"Created temporary table {temp_table_name}")

        try:
            # Step 2: Prepare data for insertion (calculate uni_id based on UTC date)
            tickers = list(set(bar['ticker'] for bar in bars))
            ticker_list_sql = ", ".join([f"'{ticker}'" for ticker in tickers])
            # Use tuple format for IN clause with dates for safety
            # Correctly generate the list of toDate strings first
            todate_parts = [f"toDate('{date_str}')" for date_str in unique_dates_str]
            date_list_sql = f"({', '.join(todate_parts)})"


            bars_to_insert = []
            for bar in bars:
                 # Generate uni_id using ticker and UTC date string
                 ticker = bar['ticker']
                 date_str_utc = bar['timestamp'].strftime('%Y-%m-%d')
                 # Consistent hash using db method (already handles string conversion)
                 # Use nanosecond epoch of the UTC timestamp for hashing consistency
                 timestamp_ns_epoch = int(bar['timestamp'].timestamp() * 1e9)
                 bar['uni_id'] = db._generate_consistent_hash(ticker, timestamp_ns_epoch) # Use ns epoch
                 bars_to_insert.append(bar)

            # Insert data into temporary table
            if bars_to_insert:
                temp_insert_start = time.time()
                await db.insert_data(temp_table_name, bars_to_insert) # insert_data now handles UTC conversion
                print(f"Inserted {len(bars_to_insert)} rows into temporary table in {time.time() - temp_insert_start:.2f} seconds")
            else:
                 print("No bars prepared for insertion into temp table.")
                 return # Exit if nothing to insert

            # Step 3: Find and delete existing records in the main table for the same tickers and UTC dates
            # Use toDate on the timestamp column which correctly extracts date from DateTime64 (UTC)
            delete_query = f"""
                ALTER TABLE `{db.database}`.`{config.TABLE_STOCK_DAILY}`
                DELETE WHERE ticker IN ({ticker_list_sql})
                AND toDate(timestamp) IN {date_list_sql}
            """
            delete_start = time.time()
            # Use command with settings for potentially long-running deletes
            db.client.command(delete_query, settings={'mutations_sync': 2}) # Wait for mutation
            print(f"Synchronously deleted existing records for specified tickers and UTC dates in {time.time() - delete_start:.2f} seconds")

            # Step 4: Copy data from temp table to main table
            copy_query = f"""
                INSERT INTO `{db.database}`.`{config.TABLE_STOCK_DAILY}`
                SELECT * FROM `{db.database}`.`{temp_table_name}`
            """
            copy_start = time.time()
            db.client.command(copy_query)
            print(f"Copied data from temporary table to main table in {time.time() - copy_start:.2f} seconds")

        finally:
            # Clean up - drop the temporary table
            drop_query = f"DROP TABLE IF EXISTS `{db.database}`.`{temp_table_name}`"
            db.client.command(drop_query)
            print(f"Dropped temporary table {temp_table_name}")

        # Verify there are no duplicates after the operation
        try:
            verify_query = f"""
                SELECT
                    ticker,
                    toDate(timestamp) as date_utc,
                    count(*) as count
                FROM `{db.database}`.`{config.TABLE_STOCK_DAILY}`
                WHERE ticker IN ({ticker_list_sql})
                AND toDate(timestamp) IN {date_list_sql}
                GROUP BY ticker, date_utc
                HAVING count > 1
                LIMIT 10
            """

            verify_result = db.client.query(verify_query)

            if verify_result.result_rows:
                print("WARNING: Duplicates still exist after daily bars operation:")
                for row in verify_result.result_rows:
                    print(f"  - Ticker: {row[0]}, Date(UTC): {row[1]}, Count: {row[2]}")
            else:
                print("No duplicates found after daily bars operation - data is clean")

        except Exception as e:
            print(f"Error verifying daily bar duplicates: {str(e)}")
            print(f"Error type: {type(e)}")

    except Exception as e:
        print(f"Error storing daily bars: {str(e)}")
        print(f"Error type: {type(e)}")
        # Don't re-raise to allow the process to continue

async def init_bars_table(db: ClickHouseDB) -> None:
    """
    Initialize the stock daily bars table (delegates to db.py).
    """
    db.create_table_if_not_exists(config.TABLE_STOCK_DAILY, BARS_SCHEMA) 