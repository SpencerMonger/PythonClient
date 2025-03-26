"""
Module for handling individual ticker processing with proper connection handling for concurrent execution.
This module extracts the process_ticker function from main.py to ensure consistent DB connection usage.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pytz

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, bars_daily
from endpoints.polygon_client import close_session

async def process_ticker_with_connection(ticker: str, from_date: datetime, to_date: datetime, store_latest_only: bool = False) -> None:
    """
    Process a single ticker with its own dedicated database connection to enable concurrent processing.
    
    Args:
        ticker: Ticker symbol
        from_date: Start date
        to_date: End date
        store_latest_only: Whether to only store the latest row
    """
    ticker_db = ClickHouseDB()
    try:
        print(f"\n{'='*50}")
        print(f"Processing {ticker}...")
        print(f"Time range: {from_date.strftime('%Y-%m-%d %H:%M:%S')} to {to_date.strftime('%Y-%m-%d %H:%M:%S')}")
        start_time = time.time()
        
        # Process the ticker with the dedicated connection
        await process_ticker(ticker_db, ticker, from_date, to_date, store_latest_only)
        
        print(f"Finished processing {ticker} in {time.time() - start_time:.2f} seconds")
        print('='*50)
    except Exception as e:
        print(f"Error processing ticker {ticker}: {str(e)}")
    finally:
        # Make sure to close the DB connection when done
        ticker_db.close()

async def process_ticker(db: ClickHouseDB, ticker: str, from_date: datetime, to_date: datetime, store_latest_only: bool = False) -> None:
    """
    Process all data for a ticker between dates
    
    Args:
        db: Database connection
        ticker: Ticker symbol
        from_date: Start date
        to_date: End date
        store_latest_only: Whether to only store the latest row
    """
    try:
        ticker_start_time = time.time()
        
        # For live mode, adjust to last minute for trades and quotes
        et_tz = pytz.timezone('US/Eastern')
        trade_from = from_date if from_date.tzinfo is not None else et_tz.localize(from_date)
        trade_to = to_date if to_date.tzinfo is not None else et_tz.localize(to_date)
        
        if store_latest_only:
            trade_from = datetime(trade_to.year, trade_to.month, trade_to.day, 
                                trade_to.hour, trade_to.minute, 0, 
                                tzinfo=trade_to.tzinfo)
            trade_to = trade_from + timedelta(minutes=1)
        
        # Create tasks for all data fetching operations to run in parallel
        # Focus on bars and trades which are the most important for live mode
        fetch_tasks = {
            'bars': asyncio.create_task(asyncio.wait_for(
                bars.fetch_bars(ticker, from_date, to_date),
                timeout=3.0  # 3 second timeout
            )),
            'trades': asyncio.create_task(asyncio.wait_for(
                trades.fetch_trades(ticker, trade_from, trade_to),
                timeout=3.0  # 3 second timeout
            )),
            'quotes': asyncio.create_task(asyncio.wait_for(
                quotes.fetch_quotes(ticker, trade_from, trade_to),
                timeout=3.0  # 3 second timeout
            ))
        }
        
        # Create less critical tasks with lower priority
        if not store_latest_only:  # Historical mode loads all data
            fetch_tasks['daily_bars'] = asyncio.create_task(asyncio.wait_for(
                bars_daily.fetch_bars(ticker, from_date, to_date),
                timeout=3.0
            ))
            fetch_tasks['news'] = asyncio.create_task(asyncio.wait_for(
                news.fetch_news(ticker, from_date, to_date),
                timeout=2.0
            ))
            fetch_tasks['indicators'] = asyncio.create_task(asyncio.wait_for(
                indicators.fetch_all_indicators(ticker, from_date, to_date),
                timeout=2.0
            ))
        
        # Process and store critical data first - SEQUENTIALLY to avoid concurrent DB access errors
        # Each store operation gets its own database connection
        try:
            bar_data = await fetch_tasks['bars']
            if bar_data:
                if store_latest_only and bar_data:
                    bar_data = [bar_data[-1]]  # Keep only the last bar
                # Create a dedicated connection for this operation
                bar_db = ClickHouseDB()
                try:
                    await bars.store_bars(bar_db, bar_data)
                finally:
                    bar_db.close()
        except (asyncio.TimeoutError, Exception) as e:
            print(f"Error with bars for {ticker}: {type(e).__name__}")
        
        try:
            trade_data = await fetch_tasks['trades']
            if trade_data:
                # Create a dedicated connection for this operation
                trade_db = ClickHouseDB()
                try:
                    await trades.store_trades(trade_db, trade_data)
                finally:
                    trade_db.close()
        except (asyncio.TimeoutError, Exception) as e:
            print(f"Error with trades for {ticker}: {type(e).__name__}")
        
        try:
            quote_data = await fetch_tasks['quotes']
            if quote_data:
                # Create a dedicated connection for this operation
                quote_db = ClickHouseDB()
                try:
                    await quotes.store_quotes(quote_db, quote_data)
                finally:
                    quote_db.close()
        except (asyncio.TimeoutError, Exception) as e:
            print(f"Error with quotes for {ticker}: {type(e).__name__}")
        
        # Only process less critical data if we're not in live mode
        if not store_latest_only:
            try:
                daily_bar_data = await fetch_tasks['daily_bars']
                if daily_bar_data:
                    # Create a dedicated connection for this operation
                    daily_db = ClickHouseDB()
                    try:
                        await bars_daily.store_bars(daily_db, daily_bar_data, "historical")
                    finally:
                        daily_db.close()
            except (asyncio.TimeoutError, Exception) as e:
                print(f"Error with daily bars for {ticker}: {type(e).__name__}")
            
            try:
                news_data = await fetch_tasks['news']
                if news_data:
                    # Create a dedicated connection for this operation
                    news_db = ClickHouseDB()
                    try:
                        await news.store_news(news_db, news_data)
                    finally:
                        news_db.close()
            except (asyncio.TimeoutError, Exception) as e:
                print(f"Error with news for {ticker}: {type(e).__name__}")
            
            try:
                indicator_data = await fetch_tasks['indicators']
                if indicator_data:
                    # Group by indicator type and keep latest for each
                    latest_indicators = {}
                    for indicator in indicator_data:
                        indicator_type = indicator['indicator_type']
                        if indicator_type not in latest_indicators or indicator['timestamp'] > latest_indicators[indicator_type]['timestamp']:
                            latest_indicators[indicator_type] = indicator
                    indicator_data = list(latest_indicators.values())
                    
                    # Create a dedicated connection for this operation
                    indicators_db = ClickHouseDB()
                    try:
                        await indicators.store_indicators(indicators_db, indicator_data)
                    finally:
                        indicators_db.close()
            except (asyncio.TimeoutError, Exception) as e:
                print(f"Error with indicators for {ticker}: {type(e).__name__}")
        
        print(f"Processed {ticker} in {time.time() - ticker_start_time:.2f}s")
        
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
        # Don't raise to continue with other tickers 