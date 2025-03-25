import asyncio
from datetime import datetime, timedelta
from typing import Dict, List
import time
import hashlib

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
    Fetch daily bar data for a ticker between dates
    
    For live data collection, this function will automatically adjust the date range
    to include yesterday and today, since the minute-by-minute time range used in live mode
    is too narrow for daily bars.
    """
    client = get_rest_client()
    bars = []
    
    # In live mode, we need to adjust our date range to fetch yesterday and today's daily bar
    # Check if the time range is very short (less than an hour), which indicates live mode
    time_diff = (to_date - from_date).total_seconds() / 60
    is_live_mode = time_diff < 60
    
    if is_live_mode:
        print(f"Detected live mode (time range: {time_diff} minutes)")
        # For live mode, specifically get yesterday and today to avoid excessive fetching
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        from_date = yesterday
        to_date = today + timedelta(days=1)  # Include the whole current day
        print(f"Adjusted date range for daily bars: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')} (yesterday and today only)")
    
    # Format dates as YYYY-MM-DD
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")
    print(f"Fetching daily bars for {ticker} from {from_str} to {to_str}")
    
    try:
        # Make the API call
        response = client.list_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",  # Using day timespan
            from_=from_str,
            to=to_str,
            limit=50000
        )
        
        # Process the response - response may be a generator, so we can't use len()
        print(f"API response received, processing daily bars...")
        
        # Convert generator to list and process
        bar_count = 0
        for bar in response:
            bar_count += 1
            # Convert timestamp from milliseconds to datetime
            timestamp = datetime.fromtimestamp(bar.timestamp / 1000.0)
            
            # Ensure timestamp is normalized to the start of day to prevent duplicates
            timestamp = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Debug: print exact timestamp
            print(f"Processing bar for {ticker} on {timestamp.strftime('%Y-%m-%d')}")
            
            # Convert any potential None values to appropriate types
            bars.append({
                "ticker": ticker,
                "timestamp": timestamp,
                "open": float(bar.open) if bar.open is not None else None,
                "high": float(bar.high) if bar.high is not None else None,
                "low": float(bar.low) if bar.low is not None else None,
                "close": float(bar.close) if bar.close is not None else None,
                "volume": int(bar.volume) if bar.volume is not None else None,
                "vwap": float(bar.vwap) if bar.vwap is not None else None,
                "transactions": int(bar.transactions) if bar.transactions is not None else None
            })
        
        print(f"Processed {bar_count} daily bars")    
        if bars:
            print(f"Date range of bars: {bars[0]['timestamp']} to {bars[-1]['timestamp']}")
            
    except Exception as e:
        print(f"Error fetching daily bars for {ticker}: {str(e)}")
        return []
        
    return bars

async def store_bars(db: ClickHouseDB, bars: List[Dict], mode: str = "historical") -> None:
    """
    Store bar data in ClickHouse using a hash-based approach to prevent duplicates.
    Uses a temporary deduplication table to ensure clean data.
    
    Args:
        db: Database connection
        bars: List of bar data to store
        mode: Either "historical" or "live" (impacts logging only)
    """
    try:
        if not bars:
            print("No bars to store")
            return
            
        print(f"Processing {len(bars)} daily bars in {mode} mode")
        
        # Standardize timestamps to ensure consistent identification
        for bar in bars:
            # Ensure timestamp is at midnight to standardize daily bars
            if isinstance(bar['timestamp'], datetime):
                bar['timestamp'] = bar['timestamp'].replace(hour=0, minute=0, second=0, microsecond=0)
        
        # For audit, display the bars we're about to process
        unique_dates = set(bar['timestamp'].strftime('%Y-%m-%d') for bar in bars)
        print(f"Processing bars for dates: {', '.join(sorted(unique_dates))}")
        
        # Step 1: Create a temporary table with the exact same schema
        temp_table_name = f"temp_stock_daily_{int(time.time())}"
        create_temp_table_query = f"""
            CREATE TABLE IF NOT EXISTS {db.database}.{temp_table_name} AS
            {db.database}.{config.TABLE_STOCK_DAILY}
            ENGINE = Memory
        """
        db.client.command(create_temp_table_query)
        print(f"Created temporary table {temp_table_name}")
        
        try:
            # Step 2: Insert all new data into the temporary table
            # Get a list of all tickers we're processing for the filter
            tickers = list(set(bar['ticker'] for bar in bars))
            ticker_list = ", ".join([f"'{ticker}'" for ticker in tickers])
            date_list = ", ".join([f"'{date}'" for date in unique_dates])
            
            # Define a consistent hash function for each bar
            for bar in bars:
                ticker = bar['ticker']
                date_str = bar['timestamp'].strftime('%Y-%m-%d')
                bar['uni_id'] = int(hashlib.md5(f"{ticker}:{date_str}".encode()).hexdigest(), 16) % (2**63 - 1)
                
            # Insert data into temporary table
            temp_insert_start = time.time()
            await db.insert_data(temp_table_name, bars)
            print(f"Inserted {len(bars)} rows into temporary table in {time.time() - temp_insert_start:.2f} seconds")
            
            # Step 3: Find and delete existing records in the main table for the same tickers and dates
            delete_query = f"""
                ALTER TABLE {db.database}.{config.TABLE_STOCK_DAILY}
                DELETE WHERE ticker IN ({ticker_list})
                AND toDate(timestamp) IN ({date_list})
            """
            delete_start = time.time()
            db.client.command(delete_query)
            print(f"Deleted existing records for specified tickers and dates in {time.time() - delete_start:.2f} seconds")
            
            # Step 4: Copy data from temp table to main table
            copy_query = f"""
                INSERT INTO {db.database}.{config.TABLE_STOCK_DAILY}
                SELECT * FROM {db.database}.{temp_table_name}
            """
            copy_start = time.time()
            db.client.command(copy_query)
            print(f"Copied data from temporary table to main table in {time.time() - copy_start:.2f} seconds")
            
        finally:
            # Clean up - drop the temporary table
            drop_query = f"DROP TABLE IF EXISTS {db.database}.{temp_table_name}"
            db.client.command(drop_query)
            print(f"Dropped temporary table {temp_table_name}")
        
        # Verify there are no duplicates after the operation
        verify_query = f"""
            SELECT 
                ticker, 
                toDate(timestamp) as date, 
                count(*) as count
            FROM {db.database}.{config.TABLE_STOCK_DAILY}
            WHERE ticker IN ({ticker_list})
            AND toDate(timestamp) IN ({date_list})
            GROUP BY ticker, date
            HAVING count > 1
            LIMIT 10
        """
        
        verify_result = db.client.command(verify_query)
        if verify_result:
            print("WARNING: Duplicates still exist after operation:")
            for row in verify_result:
                print(f"  - {row['ticker']} on {row['date']}: {row['count']} records")
        else:
            print("No duplicates found after operation - data is clean")
            
    except Exception as e:
        print(f"Error storing daily bars: {str(e)}")
        print(f"Error type: {type(e)}")
        # Don't re-raise to allow the process to continue

async def init_bars_table(db: ClickHouseDB) -> None:
    """
    Initialize the stock bars table
    """
    db.create_table_if_not_exists(config.TABLE_STOCK_DAILY, BARS_SCHEMA) 