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
        # Convert dates to UTC for API calls
        from_date_utc = from_date.astimezone(pytz.UTC)
        to_date_utc = to_date.astimezone(pytz.UTC)
        
        # Fetch and store bar data
        print(f"\nFetching bar data for {ticker}...")
        bar_data = await bars.fetch_bars(ticker, from_date_utc, to_date_utc)
        if bar_data:
            print(f"Found {len(bar_data)} bars")
            await bars.store_bars(db, bar_data)
        
        # Fetch and store daily bar data (needed for master table calculations)
        print(f"\nFetching daily bar data for {ticker}...")
        daily_bar_data = await bars_daily.fetch_bars(ticker, from_date_utc, to_date_utc)
        if daily_bar_data:
            print(f"Found {len(daily_bar_data)} daily bars")
            await bars_daily.store_bars(db, daily_bar_data)
        
        # Fetch and store trade data
        print(f"\nFetching trade data for {ticker}...")
        trade_data = await trades.fetch_trades(ticker, from_date_utc)
        if trade_data:
            print(f"Found {len(trade_data)} trades")
            await trades.store_trades(db, trade_data)
        
        # Fetch and store quote data
        print(f"\nFetching quote data for {ticker}...")
        quote_data = await quotes.fetch_quotes(ticker, from_date_utc)
        if quote_data:
            print(f"Found {len(quote_data)} quotes")
            await quotes.store_quotes(db, quote_data)
        
        # Fetch and store technical indicators
        print(f"\nFetching technical indicators for {ticker}...")
        indicator_data = await indicators.fetch_all_indicators(ticker, from_date_utc, to_date_utc)
        if indicator_data:
            print(f"Found {len(indicator_data)} indicators")
            await indicators.store_indicators(db, indicator_data)
            
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
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
                # Check if market is open
                if not is_market_open():
                    now = datetime.now(et_tz)
                    print(f"\nMarket is closed at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    # Wait 1 minute before checking again
                    await asyncio.sleep(60)
                    continue
                
                # Get current time in Eastern Time
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
                
                # Calculate wait time until 10 seconds after the next minute
                next_minute = current_minute + timedelta(minutes=1)
                wait_time = (next_minute - now).total_seconds() + 10
                
                if wait_time > 0:
                    print(f"\nWaiting {wait_time:.2f} seconds until next update...")
                    await asyncio.sleep(wait_time)
                    
            except Exception as e:
                print(f"Error in processing cycle: {str(e)}")
                # Wait 30 seconds before retrying on error
                await asyncio.sleep(30)
                
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