import asyncio
import time
from datetime import datetime, timedelta
from typing import List

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, master, bars_daily

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

async def init_master_only(db: ClickHouseDB) -> None:
    """
    Initialize only the master table, assuming other tables already exist
    """
    try:
        print("\nInitializing master table...")
        start_time = time.time()
        await master.init_master_table(db)
        print(f"Master table initialized successfully in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        print(f"Error initializing master table: {str(e)}")
        raise e

async def process_ticker(db: ClickHouseDB, ticker: str, from_date: datetime, to_date: datetime) -> None:
    """
    Process all data for a ticker between dates
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
            await bars_daily.store_bars(db, daily_bar_data)
            store_time = time.time() - store_start_time
            print(f"Daily bar data stored successfully in {store_time:.2f} seconds")
        else:
            print(f"No daily bar data found (checked in {daily_fetch_time:.2f} seconds)")
        
        # Fetch and store trade data
        print(f"\nFetching trade data for {ticker}...")
        trade_start_time = time.time()
        trade_data = await trades.fetch_trades(ticker, from_date, to_date)
        trade_fetch_time = time.time() - trade_start_time
        
        if trade_data:
            print(f"Found {len(trade_data)} trades (fetched in {trade_fetch_time:.2f} seconds)")
            print("Storing trade data...")
            print(f"Sample trade data structure: {trade_data[0]}")  # Debug print
            print(f"Timestamp type: {type(trade_data[0]['sip_timestamp'])}")  # Debug print
            
            store_start_time = time.time()
            await trades.store_trades(db, trade_data)
            store_time = time.time() - store_start_time
            print(f"Trade data stored successfully in {store_time:.2f} seconds")
            print(f"Trade data processing ratio: {store_time/trade_fetch_time:.2f}x slower than fetch")  # Debug print
        else:
            print(f"No trade data found (checked in {trade_fetch_time:.2f} seconds)")
        
        # Fetch and store quote data
        print(f"\nFetching quote data for {ticker}...")
        quote_start_time = time.time()
        quote_data = await quotes.fetch_quotes(ticker, from_date, to_date)
        quote_fetch_time = time.time() - quote_start_time
        
        if quote_data:
            print(f"Found {len(quote_data)} quotes (fetched in {quote_fetch_time:.2f} seconds)")
            print("Storing quote data...")
            print(f"Sample quote data structure: {quote_data[0]}")  # Debug print
            print(f"Timestamp type: {type(quote_data[0]['sip_timestamp'])}")  # Debug print
            
            store_start_time = time.time()
            await quotes.store_quotes(db, quote_data)
            store_time = time.time() - store_start_time
            print(f"Quote data stored successfully in {store_time:.2f} seconds")
            print(f"Quote data processing ratio: {store_time/quote_fetch_time:.2f}x slower than fetch")  # Debug print
        else:
            print(f"No quote data found (checked in {quote_fetch_time:.2f} seconds)")
        
        # Fetch and store news data
        print(f"\nFetching news data for {ticker}...")
        start_time = time.time()
        news_data = await news.fetch_news(ticker, from_date, to_date)
        fetch_time = time.time() - start_time
        if news_data:
            print(f"Found {len(news_data)} news items (fetched in {fetch_time:.2f} seconds)")
            print("Storing news data...")
            start_time = time.time()
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
            await indicators.store_indicators(db, indicator_data)
            print(f"Indicator data stored successfully in {time.time() - start_time:.2f} seconds")
        else:
            print(f"No indicator data found (checked in {fetch_time:.2f} seconds)")
            
        print(f"\nTotal processing time for {ticker}: {time.time() - ticker_start_time:.2f} seconds")
            
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
        raise e

async def main(tickers: List[str], from_date: datetime, to_date: datetime) -> None:
    """
    Main function to process data for multiple tickers
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
            print(f"Time range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
            start_time = time.time()
            await process_ticker(db, ticker, from_date, to_date)
            print(f"Finished processing {ticker} in {time.time() - start_time:.2f} seconds")
            print('='*50)
            
        # Initialize master table after all data is fetched
        print("\nInitializing master table...")
        start_time = time.time()
        await init_master_only(db)
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