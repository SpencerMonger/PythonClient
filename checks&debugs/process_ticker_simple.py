"""
Module for handling individual ticker processing with proper connection handling for concurrent execution.
Uses optimized concurrency patterns to ensure true parallelism across tickers.
"""

import asyncio
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pytz
import concurrent.futures

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, bars_daily

# Define constants for performance monitoring
MIN_LOG_DURATION = 0.1  # Only log operations that take more than 0.1 seconds

# Create a shared thread pool executor for concurrent operations
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=16)

# Semaphore to limit concurrent API calls across all tickers
API_SEMAPHORE = asyncio.Semaphore(20)  # Allow up to 20 simultaneous API calls

def log_time(start_time: float, operation: str) -> None:
    """Log timing information for an operation if it took longer than MIN_LOG_DURATION"""
    duration = time.time() - start_time
    if duration > MIN_LOG_DURATION:
        print(f"[PERF] {operation} took {duration:.2f}s")

# Connection pool for database connections
_db_connection_pool = {}

def get_db_connection(purpose: str) -> ClickHouseDB:
    """Get a database connection from the pool or create a new one"""
    if purpose not in _db_connection_pool:
        _db_connection_pool[purpose] = ClickHouseDB()
    return _db_connection_pool[purpose]

def close_db_connections():
    """Close all database connections in the pool"""
    for purpose, conn in _db_connection_pool.items():
        conn.close()
    _db_connection_pool.clear()

async def process_ticker_with_connection(ticker: str, from_date: datetime, to_date: datetime, store_latest_only: bool = False) -> None:
    """
    Process a single ticker with its own dedicated database connection.
    
    Args:
        ticker: Ticker symbol
        from_date: Start date
        to_date: End date
        store_latest_only: Whether to only store the latest row
    """
    # Add a small random delay (0-500ms) to spread out initial API calls
    await asyncio.sleep(random.random() * 0.5)
    
    # Create main connection for this ticker
    ticker_start_time = time.time()
    ticker_db = ClickHouseDB()
    
    try:
        print(f"\n{'='*25} PROCESSING {ticker} {'='*25}")
        
        # Process the ticker with the dedicated connection
        await process_ticker(ticker_db, ticker, from_date, to_date, store_latest_only)
        
        log_time(ticker_start_time, f"Total ticker processing for {ticker}")
    except Exception as e:
        print(f"Error processing ticker {ticker}: {str(e)}")
    finally:
        # Make sure to close the DB connection when done
        ticker_db.close()

async def process_ticker(db: ClickHouseDB, ticker: str, from_date: datetime, to_date: datetime, store_latest_only: bool = False) -> None:
    """
    Process all data for a ticker concurrently using optimized async I/O
    
    Args:
        db: Database connection (used only for initialization and checking)
        ticker: Ticker symbol
        from_date: Start date
        to_date: End date
        store_latest_only: Whether to only store the latest row
    """
    try:
        ticker_start_time = time.time()
        
        # Ensure dates are timezone-aware
        et_tz = pytz.timezone('US/Eastern')
        tz_aware_from_date = from_date if from_date.tzinfo is not None else et_tz.localize(from_date)
        tz_aware_to_date = to_date if to_date.tzinfo is not None else et_tz.localize(to_date)
        
        # For live mode, adjust trade and quote time range
        trade_from = tz_aware_from_date
        trade_to = tz_aware_to_date
        
        if store_latest_only:
            trade_from = datetime(tz_aware_to_date.year, tz_aware_to_date.month, tz_aware_to_date.day, 
                              tz_aware_to_date.hour, tz_aware_to_date.minute, 0, 
                              tzinfo=tz_aware_to_date.tzinfo)
            trade_to = trade_from + timedelta(minutes=1)
        
        # Define optimized fetch functions with rate limiting
        
        async def fetch_and_store_bars():
            # Use semaphore to control API request rate
            async with API_SEMAPHORE:
                bars_db = ClickHouseDB()
                try:
                    fetch_start_time = time.time()
                    bar_data = await bars.fetch_bars(ticker, from_date, to_date)
                    
                    if bar_data and len(bar_data) > 0:
                        log_time(fetch_start_time, f"[{ticker}] Bar fetch ({len(bar_data)} records)")
                        
                        if store_latest_only:
                            bar_data = [bar_data[-1]]
                        
                        await bars.store_bars(bars_db, bar_data)
                        return True
                    return False
                except Exception as e:
                    print(f"Error processing bars for {ticker}: {str(e)}")
                    return False
                finally:
                    bars_db.close()
            
        async def fetch_and_store_daily_bars():
            # Use semaphore to control API request rate
            async with API_SEMAPHORE:
                daily_bars_db = ClickHouseDB()
                try:
                    fetch_start_time = time.time()
                    daily_bar_data = await bars_daily.fetch_bars(ticker, from_date, to_date)
                    
                    if daily_bar_data and len(daily_bar_data) > 0:
                        log_time(fetch_start_time, f"[{ticker}] Daily bars fetch ({len(daily_bar_data)} records)")
                        
                        if store_latest_only:
                            daily_bar_data = [daily_bar_data[-1]]
                        
                        await bars_daily.store_bars(daily_bars_db, daily_bar_data, "live" if store_latest_only else "historical")
                        return True
                    return False
                except Exception as e:
                    print(f"Error processing daily bars for {ticker}: {str(e)}")
                    return False
                finally:
                    daily_bars_db.close()
            
        async def fetch_and_store_trades():
            # Use semaphore to control API request rate
            async with API_SEMAPHORE:
                trades_db = ClickHouseDB()
                try:
                    fetch_start_time = time.time()
                    # Process trades in smaller time chunks to avoid memory issues
                    chunk_size = timedelta(days=1)  # Process 24 hours at a time
                    current_from = trade_from
                    total_trades = 0
                    
                    while current_from < trade_to:
                        current_to = min(current_from + chunk_size, trade_to)
                        
                        chunk_trades = await trades.fetch_trades(ticker, current_from, current_to)
                        
                        if chunk_trades and len(chunk_trades) > 0:
                            total_trades += len(chunk_trades)
                            await trades.store_trades(trades_db, chunk_trades)
                        
                        current_from = current_to
                    
                    if total_trades > 0:
                        log_time(fetch_start_time, f"[{ticker}] Trades processing ({total_trades} records)")
                        return True
                    return False
                except Exception as e:
                    print(f"Error processing trades for {ticker}: {str(e)}")
                    return False
                finally:
                    trades_db.close()
                
        async def fetch_and_store_quotes():
            # Use semaphore to control API request rate
            async with API_SEMAPHORE:
                quotes_db = ClickHouseDB()
                try:
                    fetch_start_time = time.time()
                    # Process quotes in smaller time chunks to avoid memory issues
                    chunk_size = timedelta(days=1)  # Process 24 hours at a time
                    current_from = trade_from  # Reuse the same time range as trades
                    quote_to = trade_to        # Reuse the same time range as trades
                    total_quotes = 0
                    
                    while current_from < quote_to:
                        current_to = min(current_from + chunk_size, quote_to)
                        
                        chunk_quotes = await quotes.fetch_quotes(ticker, current_from, current_to)
                        
                        if chunk_quotes and len(chunk_quotes) > 0:
                            total_quotes += len(chunk_quotes)
                            await quotes.store_quotes(quotes_db, chunk_quotes)
                        
                        current_from = current_to
                    
                    if total_quotes > 0:
                        log_time(fetch_start_time, f"[{ticker}] Quotes processing ({total_quotes} records)")
                        return True
                    return False
                except Exception as e:
                    print(f"Error processing quotes for {ticker}: {str(e)}")
                    return False
                finally:
                    quotes_db.close()
                
        async def fetch_and_store_news():
            # Use semaphore to control API request rate
            async with API_SEMAPHORE:
                news_db = ClickHouseDB()
                try:
                    fetch_start_time = time.time()
                    news_data = await news.fetch_news(ticker, from_date, to_date)
                    
                    if news_data and len(news_data) > 0:
                        log_time(fetch_start_time, f"[{ticker}] News fetch ({len(news_data)} records)")
                        
                        if store_latest_only:
                            news_data = [news_data[-1]]
                        
                        await news.store_news(news_db, news_data)
                        return True
                    return False
                except Exception as e:
                    print(f"Error processing news for {ticker}: {str(e)}")
                    return False
                finally:
                    news_db.close()
                
        async def fetch_and_store_indicators():
            # Use semaphore to control API request rate
            async with API_SEMAPHORE:
                indicators_db = ClickHouseDB()
                try:
                    fetch_start_time = time.time()
                    indicator_data = await indicators.fetch_all_indicators(ticker, from_date, to_date)
                    
                    if indicator_data and len(indicator_data) > 0:
                        log_time(fetch_start_time, f"[{ticker}] Indicators fetch ({len(indicator_data)} records)")
                        
                        if store_latest_only:
                            # Group by indicator type and keep latest for each
                            latest_indicators = {}
                            for indicator in indicator_data:
                                indicator_type = indicator['indicator_type']
                                if indicator_type not in latest_indicators or indicator['timestamp'] > latest_indicators[indicator_type]['timestamp']:
                                    latest_indicators[indicator_type] = indicator
                            indicator_data = list(latest_indicators.values())
                        
                        await indicators.store_indicators(indicators_db, indicator_data)
                        return True
                    return False
                except Exception as e:
                    print(f"Error processing indicators for {ticker}: {str(e)}")
                    return False
                finally:
                    indicators_db.close()
        
        # Start all fetch and store operations concurrently with minimized output
        print(f"Starting parallel data collection for {ticker}...")
        
        # Create all tasks at once
        all_tasks = [
            fetch_and_store_bars(),
            fetch_and_store_daily_bars(),
            fetch_and_store_trades(),
            fetch_and_store_quotes(),
            fetch_and_store_news(),
            fetch_and_store_indicators()
        ]
        
        # Use gather with return_exceptions for concurrent execution
        results = await asyncio.gather(*all_tasks, return_exceptions=True)
        
        # Count successful results
        successes = sum(1 for r in results if r is True)
        
        log_time(ticker_start_time, f"[{ticker}] Parallel data collection")
        print(f"✓ {ticker}: Processed {successes}/6 data types")
            
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
        raise e 