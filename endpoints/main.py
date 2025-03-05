import asyncio
from datetime import datetime, timedelta
from typing import List

from endpoints.db import ClickHouseDB
from endpoints import bars, trades, quotes, news, indicators

async def init_tables(db: ClickHouseDB) -> None:
    """
    Initialize all tables in ClickHouse
    """
    await bars.init_bars_table(db)
    await trades.init_trades_table(db)
    await quotes.init_quotes_table(db)
    await news.init_news_table(db)
    await indicators.init_indicators_table(db)

async def process_ticker(db: ClickHouseDB, ticker: str, from_date: datetime, to_date: datetime) -> None:
    """
    Process all data for a single ticker
    """
    try:
        # Fetch and store bar data
        print(f"Fetching bar data for {ticker}...")
        bar_data = await bars.fetch_bars(ticker, from_date, to_date)
        if bar_data:
            print(f"Found {len(bar_data)} bars for {ticker}")
            print("Storing bar data...")
            await bars.store_bars(db, bar_data)
            print("Bar data stored successfully")
        else:
            print(f"No bar data found for {ticker}")
    
        # Process each day in the date range for trades and quotes
        current_date = from_date
        while current_date <= to_date:
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"\nProcessing data for {ticker} on {date_str}")
            
            # Fetch and store trades
            print("Fetching trades...")
            trade_data = await trades.fetch_trades(ticker, current_date)
            if trade_data:
                print(f"Found {len(trade_data)} trades")
                print("Storing trade data...")
                await trades.store_trades(db, trade_data)
                print("Trade data stored successfully")
            else:
                print("No trade data found")
            
            # Fetch and store quotes
            print("Fetching quotes...")
            quote_data = await quotes.fetch_quotes(ticker, current_date)
            if quote_data:
                print(f"Found {len(quote_data)} quotes")
                print("Storing quote data...")
                await quotes.store_quotes(db, quote_data)
                print("Quote data stored successfully")
            else:
                print("No quote data found")
                
            current_date += timedelta(days=1)
        
        # Fetch and store news
        print(f"\nFetching news for {ticker}...")
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
    # Example usage
    tickers = ["AAPL", "MSFT", "GOOGL"]  # Add your tickers here
    from_date = datetime(2024, 1, 1)
    to_date = datetime(2024, 3, 15)
    
    asyncio.run(main(tickers, from_date, to_date)) 