import asyncio
import time
from datetime import datetime, timedelta
from typing import List

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, master, bars_daily, master_v2
from endpoints.process_ticker import process_ticker_with_connection
from endpoints.polygon_client import close_session

# Limit concurrent ticker processing
MAX_CONCURRENT_TICKERS = 3 # Limit to 3 simultaneous tickers

# Helper function to manage semaphore for ticker processing
async def process_ticker_with_semaphore(sem, ticker, from_date, to_date, store_latest_only):
    async with sem: # Acquire semaphore before processing
        # print(f"Acquired semaphore for {ticker}") # Debug print
        try:
            # process_ticker_with_connection handles its own DB connection
            await process_ticker_with_connection(ticker, from_date, to_date, store_latest_only)
        finally:
            # print(f"Released semaphore for {ticker}") # Debug print
            pass # Semaphore is released automatically by 'async with'

async def init_tables(db: ClickHouseDB) -> None:
    """
    Initialize all tables in ClickHouse
    """
    start_time = time.time()
    # First initialize core tables
    await bars.init_bars_table(db)
    await bars_daily.init_bars_table(db)  # Initialize daily bars table
    await trades.init_trades_table(db)
    await quotes.init_quotes_table(db)
    await news.init_news_table(db)
    await indicators.init_indicators_table(db)
    print(f"Table initialization completed in {time.time() - start_time:.2f} seconds")

async def init_master_only(db: ClickHouseDB, from_date: datetime = None, to_date: datetime = None, store_latest_only: bool = False) -> None:
    """
    Initialize only the master table, assuming other tables already exist
    """
    try:
        print("\nInitializing master table...")
        start_time = time.time()
        if store_latest_only and from_date and to_date:
            # For live mode, only insert latest data using master_v2
            print(f"Live mode: Using master_v2 to update data from {from_date.strftime('%Y-%m-%d %H:%M')} to {to_date.strftime('%Y-%m-%d %H:%M')}")
            await master_v2.insert_latest_data(db, from_date, to_date)
        else:
            # For historical mode, do full initialization with master.py
            print("Historical mode: Using master.py for full table initialization")
            await master.init_master_table(db)
        print(f"Master table initialized successfully in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        print(f"Error initializing master table: {str(e)}")
        raise e

async def main(tickers: List[str], from_date: datetime, to_date: datetime, store_latest_only: bool = False) -> None:
    """
    Main function to process data for multiple tickers concurrently, limited by semaphore.
    
    Args:
        tickers: List of ticker symbols
        from_date: Start date
        to_date: End date
        store_latest_only: Whether to only store the latest row per ticker
    """
    total_start_time = time.time()
    db = ClickHouseDB()
    
    try:
        # Initialize core tables first (not master table)
        print("\nInitializing core tables...")
        start_time = time.time()
        await init_tables(db)
        print(f"Core tables initialized successfully in {time.time() - start_time:.2f} seconds")
        
        # Process tickers concurrently, limited by semaphore
        print(f"\nProcessing {len(tickers)} tickers with max concurrency {MAX_CONCURRENT_TICKERS}...")
        print(f"UTC Time range: {from_date.strftime('%Y-%m-%d %H:%M:%S %Z')} to {to_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TICKERS)
        start_time = time.time()
        tasks = [
            # Create tasks using the semaphore wrapper
            process_ticker_with_semaphore(semaphore, ticker, from_date, to_date, store_latest_only)
            for ticker in tickers
        ]
        # Gather the semaphore-wrapped tasks
        await asyncio.gather(*tasks)
        print(f"All tickers processed (with semaphore) in {time.time() - start_time:.2f} seconds")
            
        # Update or initialize master table using the local db connection
        if store_latest_only:
            print("\nUpdating master table with latest data...")
            start_time = time.time()
            await master_v2.insert_latest_data(db, from_date, to_date)
            print(f"Master table updated in {time.time() - start_time:.2f} seconds")
        else:
            # Initialize master table after all data is fetched (historical mode)
            print("\nInitializing master table...")
            start_time = time.time()
            await init_master_only(db, from_date, to_date, store_latest_only)
            print(f"Master table initialized successfully in {time.time() - start_time:.2f} seconds")
        
        print(f"\nTotal execution time for this batch: {time.time() - total_start_time:.2f} seconds")
            
    except Exception as e:
        print(f"Error in main process: {str(e)}")
        import traceback
        print(traceback.format_exc()) # Print traceback for main errors
    finally:
        print("\nClosing main database connection...")
        db.close() # Close the main DB connection

if __name__ == "__main__":
    # Example usage for creating just the master table
    # This block seems intended for historical init, not live running.
    # It should probably manage its own sessions and connections cleanly.
    db_main = ClickHouseDB()
    try:
        start_time = time.time()
        # Assuming init_master_only uses the passed db connection
        asyncio.run(init_master_only(db_main))
        print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")
    finally:
        db_main.close()
        # Close HTTP sessions *only* at the very end of this specific script run
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
             loop.run_until_complete(close_session())
        finally:
             loop.close()
        print("Connections closed for __main__ block.") 