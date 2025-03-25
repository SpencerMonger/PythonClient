import asyncio
import time
from datetime import datetime, timedelta
from typing import List
import pytz

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, master, bars_daily, master_v2

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
        
        # Fetch and store bar data
        print(f"\nFetching bar data for {ticker}...")
        fetch_start_time = time.time()
        bar_data = await bars.fetch_bars(ticker, from_date, to_date)
        fetch_time = time.time() - fetch_start_time
        
        if bar_data:
            print(f"Found {len(bar_data)} bars (fetched in {fetch_time:.2f} seconds)")
            print("Storing bar data...")
            print(f"Sample bar data structure: {bar_data[0]}")  # Debug print
            print(f"Timestamp type: {type(bar_data[0]['timestamp'])}")  # Debug print
            
            store_start_time = time.time()
            # If store_latest_only is True, only store the most recent bar
            if store_latest_only and bar_data:
                bar_data = [bar_data[-1]]  # Keep only the last bar
                
            await bars.store_bars(db, bar_data)
            store_time = time.time() - store_start_time
            print(f"Bar data stored successfully in {store_time:.2f} seconds")
            print(f"Bar data processing ratio: {store_time/fetch_time:.2f}x slower than fetch")  # Debug print
        else:
            print(f"No bar data found (checked in {fetch_time:.2f} seconds)")
        
        # Fetch and store daily bar data
        print(f"\nFetching daily bar data for {ticker}...")
        daily_start_time = time.time()
        daily_bar_data = await bars_daily.fetch_bars(ticker, from_date, to_date)
        daily_fetch_time = time.time() - daily_start_time
        
        if daily_bar_data:
            print(f"Found {len(daily_bar_data)} daily bars (fetched in {daily_fetch_time:.2f} seconds)")
            print("Storing daily bar data...")
            store_start_time = time.time()
            # If store_latest_only is True, only store the most recent daily bar
            if store_latest_only and daily_bar_data:
                daily_bar_data = [daily_bar_data[-1]]
                
            await bars_daily.store_bars(db, daily_bar_data, "live" if store_latest_only else "historical")
            print(f"Daily bar data stored successfully in {time.time() - store_start_time:.2f} seconds")
        else:
            print(f"No daily bar data found (checked in {daily_fetch_time:.2f} seconds)")
        
        # Fetch and store trade data
        print(f"\nFetching trade data for {ticker}...")
        trade_start_time = time.time()
        
        # Ensure dates are timezone-aware
        et_tz = pytz.timezone('US/Eastern')
        trade_from = from_date if from_date.tzinfo is not None else et_tz.localize(from_date)
        trade_to = to_date if to_date.tzinfo is not None else et_tz.localize(to_date)
        
        # For live mode, adjust to last minute
        if store_latest_only:
            trade_from = datetime(trade_to.year, trade_to.month, trade_to.day, 
                                trade_to.hour, trade_to.minute, 0, 
                                tzinfo=trade_to.tzinfo)
            trade_to = trade_from + timedelta(minutes=1)
            
        try:
            # Process trades in smaller time chunks to avoid memory issues
            chunk_size = timedelta(days=1)  # Process 24 hours at a time
            current_from = trade_from
            total_trades = 0
            first_chunk = True  # Flag to print sample data only for first chunk
            
            while current_from < trade_to:
                current_to = min(current_from + chunk_size, trade_to)
                print(f"Fetching trades for {ticker} from {current_from.strftime('%H:%M:%S')} to {current_to.strftime('%H:%M:%S')} ET...")
                
                chunk_start_time = time.time()
                chunk_trades = await trades.fetch_trades(ticker, current_from, current_to)
                chunk_fetch_time = time.time() - chunk_start_time
                
                if chunk_trades:
                    total_trades += len(chunk_trades)
                    print(f"Found {len(chunk_trades)} trades in current chunk (fetched in {chunk_fetch_time:.2f} seconds)")
                    print("Storing trade chunk...")
                    
                    if first_chunk:
                        print(f"Sample trade data structure: {chunk_trades[0]}")  # Debug print
                        print(f"Timestamp type: {type(chunk_trades[0]['sip_timestamp'])}")  # Debug print
                        first_chunk = False
                    
                    store_start_time = time.time()
                    await trades.store_trades(db, chunk_trades)
                    store_time = time.time() - store_start_time
                    print(f"Trade chunk stored successfully in {store_time:.2f} seconds")
                    print(f"Trade chunk processing ratio: {store_time/chunk_fetch_time:.2f}x slower than fetch")  # Debug print
                
                current_from = current_to
            
            trade_fetch_time = time.time() - trade_start_time
            if total_trades > 0:
                print(f"Successfully processed {total_trades} total trades in {trade_fetch_time:.2f} seconds")
            else:
                print(f"No trade data found (checked in {trade_fetch_time:.2f} seconds)")
                
        except Exception as e:
            print(f"Error fetching trades for {ticker}: {str(e)}")
            print(f"No trade data found (checked in {time.time() - trade_start_time:.2f} seconds)")
        
        # Fetch and store quote data
        print(f"\nFetching quote data for {ticker}...")
        quote_start_time = time.time()
        
        # Use the same time range as trades
        quote_from = trade_from
        quote_to = trade_to
        
        try:
            # Process quotes in smaller time chunks to avoid memory issues
            chunk_size = timedelta(days=1)  # Process 24 hours at a time
            current_from = quote_from
            total_quotes = 0
            first_chunk = True  # Flag to print sample data only for first chunk
            
            while current_from < quote_to:
                current_to = min(current_from + chunk_size, quote_to)
                print(f"Fetching quotes for {ticker} from {current_from.strftime('%H:%M:%S')} to {current_to.strftime('%H:%M:%S')} ET...")
                
                chunk_start_time = time.time()
                chunk_quotes = await quotes.fetch_quotes(ticker, current_from, current_to)
                chunk_fetch_time = time.time() - chunk_start_time
                
                if chunk_quotes:
                    total_quotes += len(chunk_quotes)
                    print(f"Found {len(chunk_quotes)} quotes in current chunk (fetched in {chunk_fetch_time:.2f} seconds)")
                    print("Storing quote chunk...")
                    
                    if first_chunk:
                        print(f"Sample quote data structure: {chunk_quotes[0]}")  # Debug print
                        print(f"Timestamp type: {type(chunk_quotes[0]['sip_timestamp'])}")  # Debug print
                        first_chunk = False
                    
                    store_start_time = time.time()
                    await quotes.store_quotes(db, chunk_quotes)
                    store_time = time.time() - store_start_time
                    print(f"Quote chunk stored successfully in {store_time:.2f} seconds")
                    print(f"Quote chunk processing ratio: {store_time/chunk_fetch_time:.2f}x slower than fetch")  # Debug print
                
                current_from = current_to
            
            quote_fetch_time = time.time() - quote_start_time
            if total_quotes > 0:
                print(f"Successfully processed {total_quotes} total quotes in {quote_fetch_time:.2f} seconds")
            else:
                print(f"No quote data found (checked in {quote_fetch_time:.2f} seconds)")
                
        except Exception as e:
            print(f"Error fetching quotes for {ticker}: {str(e)}")
            print(f"No quote data found (checked in {time.time() - quote_start_time:.2f} seconds)")
        
        # Fetch and store news data
        print(f"\nFetching news data for {ticker}...")
        start_time = time.time()
        news_data = await news.fetch_news(ticker, from_date, to_date)
        fetch_time = time.time() - start_time
        if news_data:
            print(f"Found {len(news_data)} news items (fetched in {fetch_time:.2f} seconds)")
            print("Storing news data...")
            start_time = time.time()
            # If store_latest_only is True, only store the most recent news
            if store_latest_only and news_data:
                news_data = [news_data[-1]]
                
            await news.store_news(db, news_data)
            print(f"News data stored successfully in {time.time() - start_time:.2f} seconds")
        else:
            print(f"No news data found (checked in {fetch_time:.2f} seconds)")
        
        # Fetch and store technical indicators
        print(f"\nFetching technical indicators for {ticker}...")
        start_time = time.time()
        indicator_data = await indicators.fetch_all_indicators(ticker, from_date, to_date)
        fetch_time = time.time() - start_time
        if indicator_data:
            print(f"Found {len(indicator_data)} indicators (fetched in {fetch_time:.2f} seconds)")
            print("Storing indicator data...")
            start_time = time.time()
            # If store_latest_only is True, only store the most recent indicators
            if store_latest_only and indicator_data:
                # Group by indicator type and keep latest for each
                latest_indicators = {}
                for indicator in indicator_data:
                    indicator_type = indicator['indicator_type']
                    if indicator_type not in latest_indicators or indicator['timestamp'] > latest_indicators[indicator_type]['timestamp']:
                        latest_indicators[indicator_type] = indicator
                indicator_data = list(latest_indicators.values())
                
            await indicators.store_indicators(db, indicator_data)
            print(f"Indicator data stored successfully in {time.time() - start_time:.2f} seconds")
        else:
            print(f"No indicator data found (checked in {fetch_time:.2f} seconds)")
            
        print(f"\nTotal processing time for {ticker}: {time.time() - ticker_start_time:.2f} seconds")
            
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
        raise e

async def main(tickers: List[str], from_date: datetime, to_date: datetime, store_latest_only: bool = False) -> None:
    """
    Main function to process data for multiple tickers
    
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
        
        # Process each ticker to fetch and store data
        for ticker in tickers:
            print(f"\n{'='*50}")
            print(f"Processing {ticker}...")
            print(f"Time range: {from_date.strftime('%Y-%m-%d %H:%M:%S')} to {to_date.strftime('%Y-%m-%d %H:%M:%S')}")
            start_time = time.time()
            await process_ticker(db, ticker, from_date, to_date, store_latest_only)
            print(f"Finished processing {ticker} in {time.time() - start_time:.2f} seconds")
            print('='*50)
            
        # For live mode, we don't need to reinitialize the entire master table
        if store_latest_only:
            print("\nSkipping full master table initialization in live mode...")
        else:
            # Initialize master table after all data is fetched (historical mode)
            print("\nInitializing master table...")
            start_time = time.time()
            await init_master_only(db, from_date, to_date, store_latest_only)
            print(f"Master table initialized successfully in {time.time() - start_time:.2f} seconds")
        
        print(f"\nTotal execution time: {time.time() - total_start_time:.2f} seconds")
            
    except Exception as e:
        print(f"Error in main process: {str(e)}")
    finally:
        print("\nClosing database connection...")
        db.close()
        print("Database connection closed")

if __name__ == "__main__":
    # Example usage for creating just the master table
    db = ClickHouseDB()
    try:
        start_time = time.time()
        asyncio.run(init_master_only(db))
        print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")
    finally:
        db.close() 