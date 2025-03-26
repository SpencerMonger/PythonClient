import asyncio
import time
from datetime import datetime, timedelta
from typing import List
import pytz

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, master, bars_daily, master_v2
from endpoints.process_ticker import process_ticker_with_connection, process_ticker
from endpoints.polygon_client import close_session

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
    Main function to process data for multiple tickers concurrently
    
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
        
        # Process all tickers concurrently
        print(f"\nProcessing {len(tickers)} tickers concurrently...")
        print(f"Time range: {from_date.strftime('%Y-%m-%d %H:%M:%S')} to {to_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Each ticker gets its own connection and processing
        start_time = time.time()
        ticker_tasks = [
            process_ticker_with_connection(ticker, from_date, to_date, store_latest_only)
            for ticker in tickers
        ]
        await asyncio.gather(*ticker_tasks)
        print(f"All tickers processed concurrently in {time.time() - start_time:.2f} seconds")
            
        # For live mode, we don't need to reinitialize the entire master table
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
        
        # Close any remaining aiohttp sessions
        await close_session()
        
        print(f"\nTotal execution time: {time.time() - total_start_time:.2f} seconds")
            
    except Exception as e:
        print(f"Error in main process: {str(e)}")
    finally:
        print("\nClosing database connection...")
        db.close()
        await close_session()  # Ensure sessions are closed
        print("Database connection and HTTP sessions closed")

if __name__ == "__main__":
    # Example usage for creating just the master table
    db = ClickHouseDB()
    try:
        start_time = time.time()
        asyncio.run(init_master_only(db))
        print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")
    finally:
        db.close()
        # Close HTTP sessions in a new event loop since we're in __main__
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(close_session())
        loop.close() 