"""
Module for handling individual ticker processing with proper connection handling for concurrent execution.
This module extracts the process_ticker function from main.py to ensure consistent DB connection usage.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
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
        
        # Determine mode based on store_latest_only flag
        is_live_mode = store_latest_only
        mode_str = "live" if is_live_mode else "historical"
        
        # Set task timeout based on mode
        task_timeout = 15.0 if is_live_mode else 240.0
        print(f"Running process_ticker for {ticker} in {mode_str} mode with {task_timeout}s task timeout.")

        fetch_tasks_dict = {}
        task_keys = []

        # Create tasks based on mode
        if is_live_mode:
            # Live Mode: Fetch bars, trades, quotes, indicators concurrently for the short interval
            fetch_tasks_dict = {
                'bars': asyncio.create_task(bars.fetch_bars(ticker, fetch_from_utc, fetch_to_utc)),
                'trades': asyncio.create_task(trades.fetch_trades(ticker, fetch_from_utc, fetch_to_utc)),
                'quotes': asyncio.create_task(quotes.fetch_quotes(ticker, fetch_from_utc, fetch_to_utc)),
                'indicators': asyncio.create_task(indicators.fetch_all_indicators(ticker, fetch_from_utc, fetch_to_utc))
            }
            task_keys = ['bars', 'trades', 'quotes', 'indicators']
        else:
            # Historical Mode: Fetch bars, indicators, daily_bars, news. Trades/Quotes handled later.
            fetch_tasks_dict = {
                'bars': asyncio.create_task(bars.fetch_bars(ticker, fetch_from_utc, fetch_to_utc)),
                'indicators': asyncio.create_task(indicators.fetch_all_indicators(ticker, fetch_from_utc, fetch_to_utc)),
                'daily_bars': asyncio.create_task(bars_daily.fetch_bars(ticker, fetch_from_utc, fetch_to_utc)),
                'news': asyncio.create_task(news.fetch_news(ticker, fetch_from_utc, fetch_to_utc))
            }
            task_keys = ['bars', 'indicators', 'daily_bars', 'news']
            # Trades and quotes will be fetched day-by-day later in historical mode

        # Helper function for fetching with timeout
        async def fetch_with_timeout(task_name):
            if task_name not in fetch_tasks_dict:
                return None # Should not happen
            try:
                return await asyncio.wait_for(fetch_tasks_dict[task_name], timeout=task_timeout)
            except asyncio.TimeoutError:
                print(f"Timeout fetching {task_name} for {ticker} after {task_timeout:.1f}s")
                return None
            except Exception as e:
                print(f"Error fetching {task_name} for {ticker}: {type(e).__name__} - {e}")
                return None

        # Execute initial fetch tasks concurrently
        print(f"Gathering initial fetch results for {ticker} ({mode_str} mode)...")
        results = await asyncio.gather(
            *(fetch_with_timeout(key) for key in task_keys),
            return_exceptions=False  # Don't treat exceptions as results
        )
        results_dict = dict(zip(task_keys, results))

        # --- Handle Trades and Quotes Fetching (Historical Mode Only) ---
        trade_data = []
        quote_data = []

        if not is_live_mode: # Only run this complex fetching for historical mode
            print(f"Fetching trades and quotes day by day for {ticker} (historical mode)...")
            days = (fetch_to_utc - fetch_from_utc).days + 1
            api_semaphore = asyncio.Semaphore(2) # Limit concurrent API calls
            max_retries = 3

            async def fetch_with_retry(fetch_fn, day_start, day_end, data_type="trades"):
                retries = 0
                backoff = 1.0
                while retries < max_retries:
                    try:
                        async with api_semaphore:
                            print(f"Fetching {data_type} for {ticker} - {day_start.strftime('%Y-%m-%d')} (Retry {retries})...", end=' ')
                            timeout_per_attempt = 60.0 # Increased timeout per historical day attempt
                            result = await asyncio.wait_for(
                                fetch_fn(ticker, day_start, day_end),
                                timeout=timeout_per_attempt
                            )
                            if result:
                                print(f"OK ({len(result)} records)")
                                return result
                            else:
                                print(f"OK (0 records)")
                                return []
                    except asyncio.TimeoutError:
                         print(f"TIMEOUT after {timeout_per_attempt}s")
                         retries += 1
                         backoff = min(15, backoff * 1.5)
                         if retries < max_retries: await asyncio.sleep(backoff)
                    except Exception as e:
                        print(f"ERROR ({type(e).__name__})")
                        retries += 1
                        # Handle potential 502 Bad Gateway with longer backoff
                        if isinstance(e, aiohttp.ClientResponseError) and e.status == 502:
                            backoff = min(30, backoff * 2) # Longer backoff for 502
                            print(f"Received 502 Bad Gateway, longer backoff {backoff:.1f}s")
                        else:
                            backoff = min(15, backoff * 1.5)
                        if retries < max_retries: await asyncio.sleep(backoff)
                        else: print(f"Failed final retry for {data_type} on {day_start.strftime('%Y-%m-%d')}: {type(e).__name__}")
                return []

            day_tasks = []
            for day_offset in range(days):
                day_start = (fetch_from_utc + timedelta(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
                day_end = day_start + timedelta(days=1)
                # Skip weekends
                if day_start.weekday() >= 5: continue

                trade_task = asyncio.create_task(fetch_with_retry(trades.fetch_trades, day_start, day_end, "trades"))
                quote_task = asyncio.create_task(fetch_with_retry(quotes.fetch_quotes, day_start, day_end, "quotes"))
                day_tasks.append((trade_task, quote_task))

            # Process tasks, allowing some concurrency controlled by semaphore in fetch_with_retry
            all_trade_tasks = [t[0] for t in day_tasks]
            all_quote_tasks = [t[1] for t in day_tasks]

            trade_results_daily = await asyncio.gather(*all_trade_tasks)
            quote_results_daily = await asyncio.gather(*all_quote_tasks)

            trade_data = [item for sublist in trade_results_daily if sublist for item in sublist]
            quote_data = [item for sublist in quote_results_daily if sublist for item in sublist]
            print(f"Finished historical fetching for {ticker}: Got {len(trade_data)} trades, {len(quote_data)} quotes total.")
        else:
            # Live Mode: Use results gathered initially
            trade_data = results_dict.get('trades')
            quote_data = results_dict.get('quotes')

        # --- Assign other results ---
        bar_data = results_dict.get('bars')
        indicator_data = results_dict.get('indicators')
        daily_bar_data = results_dict.get('daily_bars') # Will be None in live mode
        news_data = results_dict.get('news') # Will be None in live mode

        print(f"Finished gathering all fetch results for {ticker}.")

        # --- Storage (using the single passed DB connection) ---
        print(f"Starting storage for {ticker}...")
        storage_start_time = time.time()

        # Store bars (handle store_latest_only here)
        if bar_data:
            if store_latest_only and len(bar_data) > 0:
                # Ensure we take the *most recent* bar if multiple exist for the minute
                bar_data.sort(key=lambda x: x.get('timestamp', datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
                bar_data = [bar_data[0]]
                print(f"Storing latest 1 bar for {ticker} (live mode)")
            try:
                await bars.store_bars(db, bar_data)
            except Exception as e:
                print(f"Error storing bars for {ticker}: {e}")
            else:
                print(f"Successfully stored {len(bar_data)} bars for {ticker}")

        if trade_data:
            try:
                await trades.store_trades(db, trade_data)
            except Exception as e:
                print(f"Error storing trades for {ticker}: {e}")
            else:
                print(f"Successfully stored {len(trade_data)} trades for {ticker}")

        if quote_data:
            try:
                await quotes.store_quotes(db, quote_data)
            except Exception as e:
                print(f"Error storing quotes for {ticker}: {e}")
            else:
                print(f"Successfully stored {len(quote_data)} quotes for {ticker}")

        if indicator_data:
            try:
                await indicators.store_indicators(db, indicator_data)
            except Exception as e:
                print(f"Error storing indicators for {ticker}: {e}")
            else:
                print(f"Successfully stored {len(indicator_data)} indicators for {ticker}")

        # Only store daily bars and news in historical mode
        if not is_live_mode:
            if daily_bar_data:
                try:
                    await bars_daily.store_bars(db, daily_bar_data, "historical")
                except Exception as e:
                    print(f"Error storing daily bars for {ticker}: {e}")
                else:
                    print(f"Successfully stored {len(daily_bar_data)} daily bars for {ticker}")

            if news_data:
                try:
                    await news.store_news(db, news_data)
                except Exception as e:
                    print(f"Error storing news for {ticker}: {e}")
                else:
                    print(f"Successfully stored {len(news_data)} news items for {ticker}")

        print(f"Finished storage for {ticker} in {time.time() - storage_start_time:.2f}s")
        print(f"Finished processing {ticker} in {time.time() - ticker_start_time:.2f}s") # Renamed log variable

    except Exception as e:
        print(f"Error in process_ticker for {ticker}: {str(e)}")
        import traceback
        print(traceback.format_exc()) # Print traceback for errors within process_ticker 