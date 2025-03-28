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
from endpoints.polygon_client import close_session, get_aiohttp_session

def is_market_open() -> bool:
    """
    Always returns True since market hours are controlled externally
    """
    return True

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
        
        # Initialize all session pools once before the loop
        for ticker in tickers:
            await get_aiohttp_session(ticker)
        await get_aiohttp_session()  # Default session
            
        while True:
            try:
                # Get current time in Eastern Time
                now = datetime.now(et_tz)
                print(f"\n[DEBUG LIVE] Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                
                # Calculate time until next run (at the beginning of the next minute)
                # This gives us more processing time
                current_minute = now.replace(second=0, microsecond=0)
                next_minute = current_minute + timedelta(minutes=1)
                target_time = next_minute.replace(second=1)  # Just 1 second past the minute
                
                # Calculate wait time until target time
                wait_time = (target_time - now).total_seconds()
                if wait_time <= 0:
                    # If we've passed the target time, wait for the next minute
                    target_time = target_time + timedelta(minutes=1)
                    wait_time = (target_time - now).total_seconds()
                
                print(f"\nWaiting {wait_time:.2f} seconds until {target_time.strftime('%H:%M:%S')} ET...")
                await asyncio.sleep(wait_time)
                
                # After waiting, calculate the previous minute's time range
                now = datetime.now(et_tz)
                last_minute_end = now.replace(second=0, microsecond=0) - timedelta(minutes=1)  # Previous minute end
                last_minute_start = last_minute_end - timedelta(minutes=1)  # Previous minute start
                
                # Add debug logs for exact timestamps
                print(f"\n[DEBUG LIVE] Processing minute range:")
                print(f"[DEBUG LIVE] From: {last_minute_start.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                print(f"[DEBUG LIVE] To: {last_minute_end.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                print(f"[DEBUG LIVE] From (UTC): {last_minute_start.astimezone(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                print(f"[DEBUG LIVE] To (UTC): {last_minute_end.astimezone(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                
                print(f"\nStarting live data processing at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                print(f"Processing {len(tickers)} tickers concurrently for {last_minute_start.strftime('%H:%M:00')} - {last_minute_end.strftime('%H:%M:00')} ET")
                
                # Start timing the execution
                execution_start = time.time()
                
                # Run data collection in live mode with the previous minute's time range
                # Set a hard timeout for the entire operation
                data_collection_task = asyncio.create_task(
                    run_data_collection(
                        mode="live", 
                        store_latest_only=True,
                        from_date=last_minute_start,
                        to_date=last_minute_end
                    )
                )
                
                try:
                    await asyncio.wait_for(data_collection_task, timeout=7.0)  # 7-second hard timeout
                    print(f"Data collection completed successfully")
                except asyncio.TimeoutError:
                    print(f"Data collection timeout - continuing with available data")
                except Exception as e:
                    print(f"Data collection error: {str(e)}")
                
                # Log execution time for the data collection phase
                data_collection_time = time.time() - execution_start
                print(f"Data collection phase took {data_collection_time:.2f} seconds")
                
                # If we have time left from our 10-second budget, run model predictions
                time_left = 9.0 - data_collection_time  # Use 9 seconds as the target to be safe
                if time_left > 1.5:  # Only if we have at least 1.5 seconds left
                    print("\n=== Triggering model predictions ===")
                    try:
                        await asyncio.wait_for(run_model_feed(), timeout=time_left)
                        print("Model predictions completed successfully")
                    except asyncio.TimeoutError:
                        print(f"Model predictions timed out after {time_left:.2f} seconds")
                    except Exception as e:
                        print(f"Warning: Error in model predictions: {str(e)}")
                else:
                    print(f"Skipping model predictions due to time constraints ({time_left:.2f} seconds left)")
                
                # Close any active aiohttp sessions to free up resources
                await close_session()
                
                # Log total execution time
                total_time = time.time() - execution_start
                print(f"\nTotal execution time: {total_time:.2f} seconds")
                
                # Reinitialize session pool for next run
                for ticker in tickers:
                    await get_aiohttp_session(ticker)
                await get_aiohttp_session()
                    
            except Exception as e:
                print(f"Error in processing cycle: {str(e)}")
                # Close any active sessions on error
                await close_session()
                # Initialize new sessions
                for ticker in tickers:
                    await get_aiohttp_session(ticker)
                await get_aiohttp_session()
                # Wait a short time before retrying
                await asyncio.sleep(5)
                
    except KeyboardInterrupt:
        print("\nStopping live data collection...")
        await close_session()

if __name__ == "__main__":
    print(f"Starting live data collection with concurrent processing for {len(tickers)} tickers: {', '.join(tickers)}")
    asyncio.run(run_live_data()) 