import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import List
import pytz

from endpoints.db import ClickHouseDB
from endpoints import config
from endpoints.main_run import run_data_collection, tickers
from endpoints.model_feed import run_model_feed
from endpoints import master_v2
from endpoints.polygon_client import close_session, get_aiohttp_session

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
        # Use Eastern Time for market hours check ONLY
        et_tz = pytz.timezone('US/Eastern')
        
        # Initialize all session pools once before the loop
        for ticker in tickers:
            await get_aiohttp_session(ticker)
        await get_aiohttp_session()  # Default session
            
        while True:
            try:
                # Get current time in UTC
                now_utc = datetime.now(timezone.utc)
                
                # Calculate time until next run (at the beginning of the next minute in UTC)
                current_minute_utc = now_utc.replace(second=0, microsecond=0)
                next_minute_utc = current_minute_utc + timedelta(minutes=1)
                target_time_utc = next_minute_utc.replace(second=1) # 1 second past the minute UTC
                
                # Calculate wait time until target time
                wait_time = (target_time_utc - now_utc).total_seconds()

                if wait_time <= 0:
                    # If we've passed the target time, wait for the next minute
                    target_time_utc = target_time_utc + timedelta(minutes=1)
                    wait_time = (target_time_utc - now_utc).total_seconds()
                
                # Convert target UTC time to ET for logging purposes
                target_time_et = target_time_utc.astimezone(et_tz)
                print(f"\nWaiting {wait_time:.2f} seconds until {target_time_et.strftime('%Y-%m-%d %H:%M:%S %Z')} ({target_time_utc.strftime('%Y-%m-%d %H:%M:%S %Z')})...")
                await asyncio.sleep(wait_time)
                
                # Check if market is open *after* waiting, using ET time
                if not is_market_open():
                    now_et = datetime.now(et_tz)
                    print(f"\nMarket is closed at {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    # Wait for 1 minute before checking again
                    await asyncio.sleep(60)
                    continue
                
                # After waiting, calculate the previous minute's time range in UTC
                now_utc = datetime.now(timezone.utc)
                # Use floor division for robustness around minute boundary
                current_minute_utc = now_utc.replace(second=0, microsecond=0)
                last_minute_end_utc = current_minute_utc # End of the *previous* minute interval (exclusive for Polygon)
                last_minute_start_utc = last_minute_end_utc - timedelta(minutes=1) # Start of the previous minute interval
                
                print(f"\nStarting live data processing at {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                print(f"Processing {len(tickers)} tickers concurrently for UTC range: {last_minute_start_utc.strftime('%Y-%m-%d %H:%M:%S')} - {last_minute_end_utc.strftime('%Y-%m-%d %H:%M:%S')}")
                # Log ET range as well for comparison
                print(f"Equivalent ET range: {last_minute_start_utc.astimezone(et_tz).strftime('%Y-%m-%d %H:%M:%S')} - {last_minute_end_utc.astimezone(et_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

                # Start timing the execution
                execution_start = time.time()
                
                # Run data collection in live mode with the previous minute's UTC time range
                data_collection_task = asyncio.create_task(
                    run_data_collection(
                        mode="live",
                        store_latest_only=True,
                        from_date=last_minute_start_utc, # Pass UTC time
                        to_date=last_minute_end_utc      # Pass UTC time
                    )
                )
                
                try:
                    # INCREASED overall timeout significantly
                    await asyncio.wait_for(data_collection_task, timeout=25.0)
                    print(f"Data collection completed successfully")
                except asyncio.TimeoutError:
                     # This timeout should be less likely now, but keep the handling
                    print(f"Data collection timeout - continuing with available data")
                except Exception as e:
                    print(f"Data collection error: {str(e)}")
                
                # Log execution time for the data collection phase
                data_collection_time = time.time() - execution_start
                print(f"Data collection phase took {data_collection_time:.2f} seconds")
                
                # Adjust budget for model predictions based on new expected time
                # Target completion within ~30 seconds total? Let's give model feed ~5 seconds if possible.
                time_left = 28.0 - data_collection_time # Use 28s target for safety margin before next minute
                if time_left > 1.5:
                    print("\n=== Triggering model predictions ===")
                    try:
                        # Ensure run_model_feed uses UTC internally as well
                        await asyncio.wait_for(run_model_feed(), timeout=time_left)
                        print("Model predictions completed successfully")
                    except asyncio.TimeoutError:
                        print(f"Model predictions timed out after {time_left:.2f} seconds")
                    except Exception as e:
                        print(f"Warning: Error in model predictions: {str(e)}")
                else:
                    print(f"Skipping model predictions due to time constraints ({time_left:.2f} seconds left)")
                
                # Log total execution time
                total_time = time.time() - execution_start
                print(f"\nTotal execution time: {total_time:.2f} seconds")
                
            except Exception as e:
                print(f"Error in processing cycle: {str(e)}")
                # Wait a short time before retrying
                print("Waiting briefly before retrying...")
                await asyncio.sleep(5)
                
    except KeyboardInterrupt:
        print("\nStopping live data collection...")
        # Close sessions cleanly on manual stop
        await close_session()

if __name__ == "__main__":
    print(f"Starting live data collection with concurrent processing for {len(tickers)} tickers: {', '.join(tickers)}")
    asyncio.run(run_live_data()) 