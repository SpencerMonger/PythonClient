from datetime import datetime, timedelta
import asyncio
from endpoints.db import ClickHouseDB
from endpoints import indicators
import time

async def process_chunk(db: ClickHouseDB, ticker: str, start_date: datetime, end_date: datetime, retries: int = 3):
    """Process a single chunk of data with retries"""
    for attempt in range(retries):
        try:
            print(f"Processing {ticker} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} (Attempt {attempt + 1})")
            indicator_data = await indicators.fetch_all_indicators(ticker, start_date, end_date)
            if indicator_data:
                print(f"Found {len(indicator_data)} indicators")
                await indicators.store_indicators(db, indicator_data)
                print(f"Stored successfully")
                return True
            else:
                print(f"No data found")
                time.sleep(2)  # Wait before retry
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {str(e)}")
            if attempt < retries - 1:
                time.sleep(5)  # Longer wait between retries
            else:
                print(f"Failed after {retries} attempts")
    return False

async def update_indicators():
    db = ClickHouseDB()
    
    # Initialize indicators table
    await indicators.init_indicators_table(db)
    
    # Define parameters
    tickers = ["AAPL", "AMZN", "TSLA", "NVDA", "MSFT", "GOOGL", "META", "AMD"]
    
    # Process in smaller chunks
    chunk_size = timedelta(days=7)  # Process 7 days at a time
    from_date = datetime(2025, 1, 2)
    final_date = datetime(2025, 3, 1)
    
    try:
        for ticker in tickers:
            print(f"\nProcessing {ticker}...")
            
            # Process each chunk
            chunk_start = from_date
            while chunk_start < final_date:
                chunk_end = min(chunk_start + chunk_size, final_date)
                success = await process_chunk(db, ticker, chunk_start, chunk_end)
                
                if not success:
                    print(f"Warning: Failed to process {ticker} for period {chunk_start} to {chunk_end}")
                
                # Move to next chunk
                chunk_start = chunk_end
                
                # Small delay between chunks to avoid rate limits
                await asyncio.sleep(1)
            
            # Delay between tickers
            await asyncio.sleep(2)
                
    except Exception as e:
        print(f"Error updating indicators: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(update_indicators()) 