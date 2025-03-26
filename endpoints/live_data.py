import asyncio
import time
from datetime import datetime, timedelta
from typing import List
import pytz

from endpoints.db import ClickHouseDB
from endpoints import config
from endpoints.main_run import run_data_collection, tickers
from endpoints.model_feed import run_model_feed
from endpoints import master_v2

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
    Main function to continuously process latest minute of data for multiple tickers concurrently
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
                
                # Calculate time until next run (5 seconds past the next minute)
                current_minute = now.replace(second=0, microsecond=0)
                next_minute = current_minute + timedelta(minutes=1)
                target_time = next_minute.replace(second=5)
                
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
                
                # After waiting, calculate the previous minute's time range
                now = datetime.now(et_tz)
                last_minute_end = now.replace(second=0, microsecond=0) - timedelta(minutes=1)  # Previous minute end
                last_minute_start = last_minute_end - timedelta(minutes=1)  # Previous minute start
                
                print(f"\nStarting live data processing at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                print(f"Processing {len(tickers)} tickers concurrently for {last_minute_start.strftime('%H:%M:00')} - {last_minute_end.strftime('%H:%M:00')} ET")
                
                # Run data collection in live mode with the previous minute's time range
                # This now processes all tickers concurrently thanks to the updated main function
                await run_data_collection(
                    mode="live", 
                    store_latest_only=True,
                    from_date=last_minute_start,
                    to_date=last_minute_end
                )
                
                # Update master tables with latest data after all tickers are processed
                db = ClickHouseDB()
                try:
                    print("\n=== Updating master tables with latest data ===")
                    print(f"Time range: {last_minute_start.strftime('%Y-%m-%d %H:%M:%S')} - {last_minute_end.strftime('%Y-%m-%d %H:%M:%S')} ET")
                    
                    # First, check if the tables exist
                    master_exists = db.table_exists(config.TABLE_STOCK_MASTER)
                    normalized_exists = db.table_exists(config.TABLE_STOCK_NORMALIZED)
                    
                    if not master_exists or not normalized_exists:
                        print("Master and/or normalized tables don't exist, initializing them first...")
                        await master_v2.init_master_v2(db)
                    
                    # Now update the tables with latest data
                    await master_v2.insert_latest_data(db, last_minute_start, last_minute_end)
                    print("Master tables updated successfully")
                except Exception as e:
                    print(f"Warning: Error updating master tables: {str(e)}")
                    print("Continuing with execution despite master table update error")
                finally:
                    db.close()
                
                # Run model feed after data collection
                print("\n=== Triggering model predictions ===")
                try:
                    await run_model_feed()
                    print("Model predictions completed successfully")
                except Exception as e:
                    print(f"Warning: Error in model predictions: {str(e)}")
                    print("Continuing with execution despite prediction error")
                    
            except Exception as e:
                print(f"Error in processing cycle: {str(e)}")
                # Wait until the next minute + 5 seconds before retrying
                await asyncio.sleep(60)
                
    except KeyboardInterrupt:
        print("\nStopping live data collection...")

if __name__ == "__main__":
    print(f"Starting live data collection with concurrent processing for {len(tickers)} tickers: {', '.join(tickers)}")
    asyncio.run(run_live_data()) 