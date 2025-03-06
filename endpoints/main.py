import asyncio
from datetime import datetime, timedelta
from typing import List

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators, master

async def init_tables(db: ClickHouseDB) -> None:
    """
    Initialize all tables in ClickHouse
    """
    await bars.init_bars_table(db)
    await trades.init_trades_table(db)
    await quotes.init_quotes_table(db)
    await news.init_news_table(db)
    await indicators.init_indicators_table(db)
    await master.init_master_table(db)  # Initialize the master table last

async def init_master_only(db: ClickHouseDB) -> None:
    """
    Initialize only the master table, assuming other tables already exist
    """
    try:
        print("\nInitializing master table...")
        await master.init_master_table(db)
        print("Master table initialized successfully")
    except Exception as e:
        print(f"Error initializing master table: {str(e)}")
        raise e

async def process_ticker(db: ClickHouseDB, ticker: str, from_date: datetime, to_date: datetime) -> None:
    """
    Process all data for a ticker between dates
    """
    try:
        # Fetch and store bar data
        print(f"\nFetching bar data for {ticker}...")
        bar_data = await bars.fetch_bars(ticker, from_date, to_date)
        if bar_data:
            print(f"Found {len(bar_data)} bars")
            print("Storing bar data...")
            await bars.store_bars(db, bar_data)
            print("Bar data stored successfully")
        else:
            print("No bar data found")
        
        # Fetch and store trade data
        print(f"\nFetching trade data for {ticker}...")
        trade_data = await trades.fetch_trades(ticker, from_date)
        if trade_data:
            print(f"Found {len(trade_data)} trades")
            print("Storing trade data...")
            await trades.store_trades(db, trade_data)
            print("Trade data stored successfully")
        else:
            print("No trade data found")
        
        # Fetch and store quote data
        print(f"\nFetching quote data for {ticker}...")
        quote_data = await quotes.fetch_quotes(ticker, from_date)
        if quote_data:
            print(f"Found {len(quote_data)} quotes")
            print("Storing quote data...")
            await quotes.store_quotes(db, quote_data)
            print("Quote data stored successfully")
        else:
            print("No quote data found")
        
        # Fetch and store news data
        print(f"\nFetching news data for {ticker}...")
        news_data = await news.fetch_news(ticker)
        if news_data:
            print(f"Found {len(news_data)} news items")
            print("Storing news data...")
            await news.store_news(db, news_data)
            print("News data stored successfully")
        else:
            print("No news data found")
        
        # Fetch and store technical indicators
        print(f"\nFetching technical indicators for {ticker}...")
        indicator_data = await indicators.fetch_all_indicators(ticker, from_date, to_date)
        if indicator_data:
            print(f"Found {len(indicator_data)} indicators")
            print("Storing indicator data...")
            await indicators.store_indicators(db, indicator_data)
            print("Indicator data stored successfully")
        else:
            print("No indicator data found")
            
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
        raise e

async def main(tickers: List[str], from_date: datetime, to_date: datetime) -> None:
    """
    Main function to process data for multiple tickers
    """
    db = ClickHouseDB()
    
    try:
        # Initialize tables
        print("\nInitializing tables...")
        await init_tables(db)
        print("Tables initialized successfully")
        
        # Process each ticker
        for ticker in tickers:
            print(f"\n{'='*50}")
            print(f"Processing {ticker}...")
            print(f"Time range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
            await process_ticker(db, ticker, from_date, to_date)
            print(f"Finished processing {ticker}")
            print('='*50)
            
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
        asyncio.run(init_master_only(db))
    finally:
        db.close() 