import asyncio
import time
from datetime import datetime, timedelta
from typing import List
import pytz

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, master, bars_daily, config

def is_market_open() -> bool:
    """
    Check if the US stock market is currently open
    """
    et_tz = pytz.timezone('US/Eastern')
    now = datetime.now(et_tz)
    
    # Check if it's a weekday
    if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
    
    # Convert current time to seconds since midnight
    current_time = now.hour * 3600 + now.minute * 60 + now.second
    market_open = 9 * 3600 + 30 * 60  # 9:30 AM
    market_close = 16 * 3600  # 4:00 PM
    
    return market_open <= current_time <= market_close

def verify_env():
    """
    Verify that all required configuration values are set
    """
    print("\nChecking configuration values...")
    
    # Check each required config value
    missing = []
    if not config.CLICKHOUSE_HOST:
        missing.append("CLICKHOUSE_HOST")
    if not config.CLICKHOUSE_USER:
        missing.append("CLICKHOUSE_USER")
    if not config.CLICKHOUSE_PASSWORD:
        missing.append("CLICKHOUSE_PASSWORD")
    if not config.CLICKHOUSE_DATABASE:
        missing.append("CLICKHOUSE_DATABASE")
    if not config.POLYGON_API_KEY:
        missing.append("POLYGON_API_KEY")
    
    if missing:
        print("\nMissing configuration values:")
        for var in missing:
            print(f"- {var}")
        raise EnvironmentError(f"Missing required configuration values: {', '.join(missing)}")
    
    print("\nConfig values:")
    print(f"ClickHouse Host: {config.CLICKHOUSE_HOST}")
    print(f"ClickHouse Port: {config.CLICKHOUSE_HTTP_PORT}")
    print(f"ClickHouse Database: {config.CLICKHOUSE_DATABASE}")
    print(f"ClickHouse Secure: {config.CLICKHOUSE_SECURE}")
    print("All required configuration values are set")

async def init_tables(db: ClickHouseDB) -> None:
    """
    Verify all required tables exist
    """
    tables_to_check = [
        config.TABLE_STOCK_BARS,
        config.TABLE_STOCK_DAILY,
        config.TABLE_STOCK_TRADES,
        config.TABLE_STOCK_QUOTES,
        config.TABLE_STOCK_NEWS,
        config.TABLE_STOCK_INDICATORS,
        config.TABLE_STOCK_MASTER
    ]
    
    for table in tables_to_check:
        if not db.table_exists(table):
            raise RuntimeError(f"Required table {table} does not exist. Please run main.py first to initialize all tables.")

async def process_ticker(db: ClickHouseDB, ticker: str, from_date: datetime, to_date: datetime) -> None:
    """
    Process latest minute of data for a ticker
    """
    try:
        start_time = time.time()
        print(f"\nStarting processing for {ticker} at {datetime.now().strftime('%H:%M:%S')}")
        
        # Convert dates to UTC for API calls and ensure we're only getting 1 minute of data
        from_date_utc = from_date.astimezone(pytz.UTC)
        to_date_utc = to_date.astimezone(pytz.UTC)
        
        # Add debug info about time range
        print(f"[{ticker}] Fetching data for time range: {from_date_utc.strftime('%Y-%m-%d %H:%M:%S')} to {to_date_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        # Convert timestamps to nanoseconds for trades and quotes filtering
        from_ts = int(from_date_utc.timestamp() * 1_000_000_000)  # Convert to nanoseconds
        to_ts = int(to_date_utc.timestamp() * 1_000_000_000)  # Convert to nanoseconds
        
        # For bars, fetch today's data starting from market open
        today_open = from_date_utc.replace(hour=13, minute=30, second=0, microsecond=0)  # 9:30 AM ET = 13:30 UTC
        
        # Fetch and store bar data
        print(f"[{ticker}] Fetching bar data...")
        bar_start = time.time()
        bar_data = await bars.fetch_bars(ticker, today_open, to_date_utc)
        if bar_data and len(bar_data) > 0:
            # Get the most recent bar
            most_recent_bar = bar_data[-1]
            print(f"[{ticker}] Debug - Most recent bar timestamp: {most_recent_bar['timestamp']} (type: {type(most_recent_bar['timestamp'])})")
            print(f"[{ticker}] Debug - Most recent bar data: {most_recent_bar}")
            
            print(f"[{ticker}] Found most recent bar (took {time.time() - bar_start:.2f}s)")
            print(f"[{ticker}] Storing bar data...")
            store_start = time.time()
            await bars.store_bars(db, [most_recent_bar])
            print(f"[{ticker}] Stored bar data successfully (took {time.time() - store_start:.2f}s)")
        else:
            print(f"[{ticker}] No bar data found")
        
        # Fetch and store daily bar data - only if we're in a new day
        current_day = datetime.now(pytz.UTC).date()
        if from_date_utc.date() != current_day:
            print(f"[{ticker}] Fetching daily bar data...")
            daily_start = time.time()
            daily_bar_data = await bars_daily.fetch_bars(ticker, from_date_utc, to_date_utc)
            if daily_bar_data:
                print(f"[{ticker}] Found {len(daily_bar_data)} daily bars (took {time.time() - daily_start:.2f}s)")
                print(f"[{ticker}] Storing daily bar data...")
                store_start = time.time()
                await bars_daily.store_bars(db, daily_bar_data)
                print(f"[{ticker}] Stored daily bar data successfully (took {time.time() - store_start:.2f}s)")
            else:
                print(f"[{ticker}] No daily bar data found")
        
        # Fetch and store trade data
        print(f"[{ticker}] Fetching trade data...")
        trade_start = time.time()
        trade_data = await trades.fetch_trades(ticker, from_date_utc)
        if trade_data:
            filtered_trades = [t for t in trade_data if from_ts <= t['sip_timestamp'] <= to_ts]
            print(f"[{ticker}] Found {len(filtered_trades)} trades in time range (took {time.time() - trade_start:.2f}s)")
            if filtered_trades:
                print(f"[{ticker}] Storing trade data...")
                store_start = time.time()
                # Batch the trades if there are too many
                if len(filtered_trades) > 10000:
                    print(f"[{ticker}] Batching {len(filtered_trades)} trades for storage")
                    batch_size = 10000
                    for i in range(0, len(filtered_trades), batch_size):
                        batch = filtered_trades[i:i + batch_size]
                        await trades.store_trades(db, batch)
                        print(f"[{ticker}] Stored batch {i//batch_size + 1} of {(len(filtered_trades) + batch_size - 1)//batch_size}")
                else:
                    await trades.store_trades(db, filtered_trades)
                print(f"[{ticker}] Stored trade data successfully (took {time.time() - store_start:.2f}s)")
        else:
            print(f"[{ticker}] No trade data found")
        
        # Fetch and store quote data
        print(f"[{ticker}] Fetching quote data...")
        quote_start = time.time()
        quote_data = await quotes.fetch_quotes(ticker, from_date_utc)
        if quote_data:
            filtered_quotes = [q for q in quote_data if from_ts <= q['sip_timestamp'] <= to_ts]
            print(f"[{ticker}] Found {len(filtered_quotes)} quotes in time range (took {time.time() - quote_start:.2f}s)")
            if filtered_quotes:
                print(f"[{ticker}] Storing quote data...")
                store_start = time.time()
                # Batch the quotes if there are too many
                if len(filtered_quotes) > 10000:
                    print(f"[{ticker}] Batching {len(filtered_quotes)} quotes for storage")
                    batch_size = 10000
                    for i in range(0, len(filtered_quotes), batch_size):
                        batch = filtered_quotes[i:i + batch_size]
                        await quotes.store_quotes(db, batch)
                        print(f"[{ticker}] Stored batch {i//batch_size + 1} of {(len(filtered_quotes) + batch_size - 1)//batch_size}")
                else:
                    await quotes.store_quotes(db, filtered_quotes)
                print(f"[{ticker}] Stored quote data successfully (took {time.time() - store_start:.2f}s)")
        else:
            print(f"[{ticker}] No quote data found")
        
        # Fetch and store technical indicators
        print(f"[{ticker}] Fetching technical indicators...")
        indicator_start = time.time()
        indicator_data = await indicators.fetch_all_indicators(ticker, today_open, to_date_utc)  # Use same time range as bars
        if indicator_data:
            # Group indicators by type to get the latest value for each
            latest_indicators = {}
            for indicator in indicator_data:
                indicator_type = indicator['indicator_type']
                if indicator_type not in latest_indicators or indicator['timestamp'] > latest_indicators[indicator_type]['timestamp']:
                    latest_indicators[indicator_type] = indicator
            
            # Convert to list and sort by timestamp
            latest_indicator_list = list(latest_indicators.values())
            latest_indicator_list.sort(key=lambda x: x['timestamp'])
            
            if latest_indicator_list:
                print(f"[{ticker}] Debug - Latest indicator timestamp: {latest_indicator_list[-1]['timestamp']} (type: {type(latest_indicator_list[-1]['timestamp'])})")
                print(f"[{ticker}] Debug - Latest indicator data: {latest_indicator_list[-1]}")
                
                print(f"[{ticker}] Found {len(latest_indicator_list)} latest indicators (took {time.time() - indicator_start:.2f}s)")
                print(f"[{ticker}] Storing indicator data...")
                store_start = time.time()
                await indicators.store_indicators(db, latest_indicator_list)
                print(f"[{ticker}] Stored indicator data successfully (took {time.time() - store_start:.2f}s)")
            else:
                print(f"[{ticker}] No indicators found in time range")
        else:
            print(f"[{ticker}] No indicator data found")
            
        total_time = time.time() - start_time
        print(f"\n[{ticker}] Completed all operations in {total_time:.2f} seconds")
            
    except Exception as e:
        print(f"[{ticker}] Error processing ticker: {str(e)}")
        print(f"[{ticker}] Error type: {type(e).__name__}")
        print(f"[{ticker}] Error occurred at {datetime.now().strftime('%H:%M:%S')}")
        raise e

async def run_live_data(tickers: List[str]) -> None:
    """
    Main function to continuously process latest minute of data for multiple tickers
    """
    # Verify environment variables before connecting to the database
    verify_env()
    
    # Test database connection
    try:
        db = ClickHouseDB()
        # Try a simple query to verify connection
        db.client.command('SELECT 1')
        print("Successfully connected to ClickHouse database")
    except Exception as e:
        print(f"Failed to connect to database: {str(e)}")
        print(f"Host: {config.CLICKHOUSE_HOST}")
        print(f"Port: {config.CLICKHOUSE_HTTP_PORT}")
        print(f"Database: {config.CLICKHOUSE_DATABASE}")
        print(f"Secure: {config.CLICKHOUSE_SECURE}")
        raise
    
    try:
        # Verify tables exist
        print("\nVerifying required tables exist...")
        await init_tables(db)
        print("All required tables exist")
        
        # Use Eastern Time for market hours
        et_tz = pytz.timezone('US/Eastern')
        
        while True:
            try:
                # Get current time in Eastern Time
                now = datetime.now(et_tz)
                
                # Calculate time until next run (10 seconds past the next minute)
                current_minute = now.replace(second=0, microsecond=0)
                next_minute = current_minute + timedelta(minutes=1)
                target_time = next_minute.replace(second=10)
                
                # Calculate wait time until target time
                wait_time = (target_time - now).total_seconds()
                if wait_time <= 0:
                    # If we've passed the target time, wait for the next minute
                    target_time = target_time + timedelta(minutes=1)
                    wait_time = (target_time - now).total_seconds()
                
                print(f"\nWaiting {wait_time:.2f} seconds until {target_time.strftime('%H:%M:%S')} ET...")
                await asyncio.sleep(wait_time)
                
                # Check if market is open after waiting
                if not is_market_open():
                    now = datetime.now(et_tz)
                    print(f"\nMarket is closed at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    continue
                
                # Calculate the minute we want to process (the previous minute)
                now = datetime.now(et_tz)
                current_minute = now.replace(second=0, microsecond=0)
                previous_minute = current_minute - timedelta(minutes=1)
                
                print(f"\n{'='*50}")
                print(f"Processing data for minute: {previous_minute.strftime('%Y-%m-%d %H:%M %Z')}")
                
                # Process each ticker for the previous minute
                for ticker in tickers:
                    await process_ticker(db, ticker, previous_minute, current_minute)
                    print(f"Completed processing {ticker} for {previous_minute.strftime('%H:%M')}")
                
                print('='*50)
                    
            except Exception as e:
                print(f"Error in processing cycle: {str(e)}")
                # Wait until the next minute + 10 seconds before retrying
                await asyncio.sleep(60)
                
    except KeyboardInterrupt:
        print("\nStopping live data collection...")
    finally:
        print("\nClosing database connection...")
        db.close()
        print("Database connection closed")

if __name__ == "__main__":
    # Use the same tickers as in main_run.py
    tickers = ["AAPL", "AMZN", "TSLA", "NVDA", "MSFT", "GOOGL", "META", "AMD"]
    
    print(f"Starting live data collection for tickers: {', '.join(tickers)}")
    asyncio.run(run_live_data(tickers)) 