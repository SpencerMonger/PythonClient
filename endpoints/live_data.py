import asyncio
import time
from datetime import datetime, timedelta
from typing import List
import pytz

from endpoints.db import ClickHouseDB
from endpoints import config
from endpoints.main_run import run_data_collection, tickers

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

async def run_live_data() -> None:
    """
    Main function to continuously process latest minute of data for multiple tickers
    """
    # Verify environment variables before starting
    verify_env()
    
    # Test database connection
    try:
        db = ClickHouseDB()
        # Try a simple query to verify connection
        db.client.command('SELECT 1')
        print("Successfully connected to ClickHouse database")
        db.close()
    except Exception as e:
        print(f"Failed to connect to database: {str(e)}")
        print(f"Host: {config.CLICKHOUSE_HOST}")
        print(f"Port: {config.CLICKHOUSE_HTTP_PORT}")
        print(f"Database: {config.CLICKHOUSE_DATABASE}")
        print(f"Secure: {config.CLICKHOUSE_SECURE}")
        raise
    
    try:
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
                    # Wait for 1 minute before checking again
                    await asyncio.sleep(60)
                    continue
                
                # Run data collection in live mode
                run_data_collection(mode="live", store_latest_only=True)
                    
            except Exception as e:
                print(f"Error in processing cycle: {str(e)}")
                # Wait until the next minute + 10 seconds before retrying
                await asyncio.sleep(60)
                
    except KeyboardInterrupt:
        print("\nStopping live data collection...")

if __name__ == "__main__":
    print(f"Starting live data collection for tickers: {', '.join(tickers)}")
    asyncio.run(run_live_data()) 