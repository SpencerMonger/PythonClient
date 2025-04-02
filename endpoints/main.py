import asyncio
import time
from datetime import datetime, timedelta
from typing import List

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, master, bars_daily, master_v2
from endpoints.process_ticker import process_ticker_with_connection
from endpoints.polygon_client import close_session

# Limit concurrent ticker processing
MAX_CONCURRENT_TICKERS = 1 # Limit to 1 simultaneous ticker

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
            # In historical mode, we auto-detect the date range from stock_bars table
            print(f"Historical mode: Using master.py for full table initialization (auto-detecting dates)")
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
        
        # For historical mode (not store_latest_only), split large date ranges into chunks
        # to avoid overwhelming the API server
        if not store_latest_only:
            MAX_DAYS_PER_FETCH = 4  # Maximum days to fetch in one request for historical mode
            days_total = (to_date - from_date).days + 1
            
            if days_total > MAX_DAYS_PER_FETCH:
                print(f"Historical mode with {days_total} days detected. Processing in chunks of {MAX_DAYS_PER_FETCH} days.")
                
                # Process in chunks
                current_from = from_date
                max_chunk_retries = 3  # Maximum retries per chunk
                chunk_backoff_base = 300.0  # 5 minutes base backoff between retries
                
                while current_from < to_date:
                    # Calculate chunk end date (not exceeding the overall to_date)
                    current_to = min(current_from + timedelta(days=MAX_DAYS_PER_FETCH-1), to_date)
                    chunk_retries = 0
                    chunk_success = False
                    
                    while not chunk_success and chunk_retries < max_chunk_retries:
                        if chunk_retries > 0:
                            # Calculate exponential backoff with some randomness
                            backoff = chunk_backoff_base * (1.5 ** chunk_retries)
                            print(f"\nRetrying chunk {current_from.strftime('%Y-%m-%d')} to {current_to.strftime('%Y-%m-%d')} "
                                  f"(attempt {chunk_retries + 1}/{max_chunk_retries}) after {backoff:.1f}s backoff...")
                            await asyncio.sleep(backoff)
                    
                        print(f"\nProcessing chunk: {current_from.strftime('%Y-%m-%d')} to {current_to.strftime('%Y-%m-%d')}")
                        
                        # Process this chunk
                        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TICKERS)
                        start_time = time.time()
                        
                        # Calculate timeout based on chunk size and number of tickers
                        # More generous timeout since we're processing one ticker at a time
                        chunk_days = (current_to - current_from).days + 1
                        timeout_per_day = 1200.0  # 20 minutes per day
                        chunk_timeout = max(3600.0, chunk_days * timeout_per_day)  # At least 1 hour, scales with days
                        
                        try:
                            tasks = [
                                process_ticker_with_semaphore(semaphore, ticker, current_from, current_to, store_latest_only)
                                for ticker in tickers
                            ]
                            await asyncio.wait_for(asyncio.gather(*tasks), timeout=chunk_timeout)
                            print(f"Chunk processed successfully in {time.time() - start_time:.2f} seconds")
                            chunk_success = True  # Mark as successful to move to next chunk
                            
                        except asyncio.TimeoutError:
                            chunk_retries += 1
                            if chunk_retries >= max_chunk_retries:
                                print(f"ERROR: Chunk processing failed after {max_chunk_retries} attempts - data for "
                                      f"{current_from.strftime('%Y-%m-%d')} to {current_to.strftime('%Y-%m-%d')} may be incomplete!")
                            else:
                                print(f"Chunk processing timed out after {chunk_timeout:.2f} seconds - will retry "
                                      f"({chunk_retries}/{max_chunk_retries} attempts made)")
                        except Exception as e:
                            chunk_retries += 1
                            print(f"Error processing chunk: {str(e)}")
                            if chunk_retries >= max_chunk_retries:
                                print(f"ERROR: Chunk processing failed after {max_chunk_retries} attempts due to errors - data for "
                                      f"{current_from.strftime('%Y-%m-%d')} to {current_to.strftime('%Y-%m-%d')} may be incomplete!")
                            
                    # Only move to next chunk if we succeeded or exhausted retries
                    if chunk_success or chunk_retries >= max_chunk_retries:
                        current_from = current_to + timedelta(days=1)
                    
                print(f"All chunks processed - check logs for any chunks that may have failed after retries")
            else:
                # Process the entire date range at once
                semaphore = asyncio.Semaphore(MAX_CONCURRENT_TICKERS)
                start_time = time.time()
                
                # Calculate timeout for the entire range
                timeout = max(3600.0, days_total * 1200.0)  # At least 1 hour, 20 minutes per day
                max_retries = 3
                retry_count = 0
                success = False
                
                while not success and retry_count < max_retries:
                    if retry_count > 0:
                        backoff = 300.0 * (1.5 ** retry_count)  # 5 minutes base backoff
                        print(f"\nRetrying full range (attempt {retry_count + 1}/{max_retries}) after {backoff:.1f}s backoff...")
                        await asyncio.sleep(backoff)
                        
                    try:
                        tasks = [
                            process_ticker_with_semaphore(semaphore, ticker, from_date, to_date, store_latest_only)
                            for ticker in tickers
                        ]
                        await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
                        print(f"All tickers processed successfully in {time.time() - start_time:.2f} seconds")
                        success = True
                    except asyncio.TimeoutError:
                        retry_count += 1
                        if retry_count >= max_retries:
                            print(f"ERROR: Processing failed after {max_retries} attempts - data may be incomplete!")
                        else:
                            print(f"Processing timed out after {timeout:.2f} seconds - will retry ({retry_count}/{max_retries} attempts made)")
                    except Exception as e:
                        retry_count += 1
                        print(f"Error processing data: {str(e)}")
                        if retry_count >= max_retries:
                            print(f"ERROR: Processing failed after {max_retries} attempts due to errors - data may be incomplete!")
        else:
            # For live mode, process the entire range at once
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_TICKERS)
            start_time = time.time()
            tasks = [
                process_ticker_with_semaphore(semaphore, ticker, from_date, to_date, store_latest_only)
                for ticker in tickers
            ]
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
        
        # Ensure polygon client session is closed
        try:
            await close_session()
        except Exception as e:
            print(f"Warning: Error closing polygon client session: {str(e)}")

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