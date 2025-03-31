"""
Module for handling individual ticker processing with proper connection handling for concurrent execution.
This module extracts the process_ticker function from main.py to ensure consistent DB connection usage.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, bars_daily
from endpoints.polygon_client import close_session

async def process_ticker_with_connection(ticker: str, from_date: datetime, to_date: datetime, store_latest_only: bool = False) -> None:
    """
    Process a single ticker with its own dedicated database connection.
    Passes the connection down to the main processing function.
    """
    ticker_db = None # Initialize
    try:
        ticker_db = ClickHouseDB() # Create connection once per ticker task
        print(f"\n{'='*50}")
        print(f"Processing {ticker}...")
        print(f"UTC Time range: {from_date.strftime('%Y-%m-%d %H:%M:%S %Z')} to {to_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        start_time = time.time()

        # Pass the created ticker_db connection to process_ticker
        await process_ticker(ticker_db, ticker, from_date, to_date, store_latest_only)

        print(f"Finished processing {ticker} in {time.time() - start_time:.2f} seconds")
        print('='*50)
    except Exception as e:
        print(f"Error processing ticker {ticker}: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        # Make sure to close the DB connection when done or if error occurs
        if ticker_db:
            ticker_db.close()
            # print(f"Closed DB connection for {ticker}") # Debug

async def process_ticker(db: ClickHouseDB, ticker: str, from_date: datetime, to_date: datetime, store_latest_only: bool = False) -> None:
    """
    Process all data for a ticker using a passed DB connection.
    Fetches data concurrently, then stores sequentially using the single connection.
    Relies on outer timeout control.
    """
    if not db or not db.client: # Check if db connection is valid
         print(f"Error: Invalid database connection passed to process_ticker for {ticker}")
         return

    try:
        ticker_start_time = time.time()

        fetch_from_utc = from_date
        fetch_to_utc = to_date

        # Create tasks for all data fetching operations
        fetch_tasks_dict = {
            'bars': asyncio.create_task(bars.fetch_bars(ticker, fetch_from_utc, fetch_to_utc)),
            'trades': asyncio.create_task(trades.fetch_trades(ticker, fetch_from_utc, fetch_to_utc)),
            'quotes': asyncio.create_task(quotes.fetch_quotes(ticker, fetch_from_utc, fetch_to_utc)),
            'indicators': asyncio.create_task(indicators.fetch_all_indicators(ticker, fetch_from_utc, fetch_to_utc))
        }
        task_keys = ['bars', 'trades', 'quotes', 'indicators'] # Order for gather

        if not store_latest_only:
            fetch_tasks_dict['daily_bars'] = asyncio.create_task(bars_daily.fetch_bars(ticker, fetch_from_utc, fetch_to_utc))
            fetch_tasks_dict['news'] = asyncio.create_task(news.fetch_news(ticker, fetch_from_utc, fetch_to_utc))
            task_keys.extend(['daily_bars', 'news'])

        # Gather all fetch results concurrently, handle errors
        print(f"Gathering fetch results for {ticker}...")
        results = await asyncio.gather(
            *(fetch_tasks_dict[key] for key in task_keys),
            return_exceptions=True
        )
        print(f"Finished gathering fetch results for {ticker}.")

        # Assign results, handling potential exceptions from gather
        results_dict = dict(zip(task_keys, results))

        def get_result(key):
            res = results_dict.get(key)
            if isinstance(res, Exception):
                print(f"Error fetching {key} for {ticker}: {type(res).__name__} - {res}")
                return None
            return res

        bar_data = get_result('bars')
        trade_data = get_result('trades')
        quote_data = get_result('quotes')
        indicator_data = get_result('indicators')
        daily_bar_data = get_result('daily_bars') if not store_latest_only else None
        news_data = get_result('news') if not store_latest_only else None


        # --- Storage (using the single passed DB connection) ---
        print(f"Starting storage for {ticker}...")
        storage_start_time = time.time()

        if bar_data:
            if store_latest_only and len(bar_data) > 0: bar_data = [bar_data[-1]]
            try: await bars.store_bars(db, bar_data) # Use passed db
            except Exception as e: print(f"Error storing bars for {ticker}: {e}")

        if trade_data:
            try: await trades.store_trades(db, trade_data) # Use passed db
            except Exception as e: print(f"Error storing trades for {ticker}: {e}")

        if quote_data:
            try: await quotes.store_quotes(db, quote_data) # Use passed db
            except Exception as e: print(f"Error storing quotes for {ticker}: {e}")

        if indicator_data:
            try: await indicators.store_indicators(db, indicator_data) # Use passed db
            except Exception as e: print(f"Error storing indicators for {ticker}: {e}")

        if not store_latest_only:
            if daily_bar_data:
                try: await bars_daily.store_bars(db, daily_bar_data, "historical") # Use passed db
                except Exception as e: print(f"Error storing daily bars for {ticker}: {e}")

            if news_data:
                try: await news.store_news(db, news_data) # Use passed db
                except Exception as e: print(f"Error storing news for {ticker}: {e}")

        print(f"Finished storage for {ticker} in {time.time() - storage_start_time:.2f}s")
        print(f"Finished processing {ticker} in {time.time() - ticker_start_time:.2f}s") # Renamed log variable

    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
        import traceback
        print(traceback.format_exc()) # Print traceback for errors within process_ticker 