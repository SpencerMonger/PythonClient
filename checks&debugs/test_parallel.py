import asyncio
import time
from datetime import datetime, timedelta
import pytz

from endpoints.process_ticker import process_ticker_with_connection

# Define test tickers
test_tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]

async def run_test():
    """Run a test of our parallel data collection system"""
    # Set up test time range (use last hour)
    et_tz = pytz.timezone('US/Eastern')
    now = datetime.now(et_tz)
    from_date = now - timedelta(minutes=60)
    to_date = now
    
    print(f"Starting parallel test with {len(test_tickers)} tickers")
    print(f"Time range: {from_date.strftime('%H:%M:%S')} - {to_date.strftime('%H:%M:%S')}")
    
    # Start timing
    start_time = time.time()
    
    # Create tasks for all tickers
    tasks = []
    for ticker in test_tickers:
        tasks.append(process_ticker_with_connection(ticker, from_date, to_date, store_latest_only=True))
    
    # Run all tasks in parallel
    await asyncio.gather(*tasks)
    
    # Print total time
    total_time = time.time() - start_time
    print(f"\nAll tickers processed in {total_time:.2f} seconds")
    
    # For comparison, run tickers sequentially
    print(f"\nNow testing sequential processing for comparison...")
    sequential_start = time.time()
    
    for ticker in test_tickers:
        print(f"\nProcessing {ticker} sequentially...")
        ticker_start = time.time()
        await process_ticker_with_connection(ticker, from_date, to_date, store_latest_only=True)
        print(f"Processed {ticker} in {time.time() - ticker_start:.2f} seconds")
    
    sequential_time = time.time() - sequential_start
    print(f"\nAll tickers processed sequentially in {sequential_time:.2f} seconds")
    
    # Calculate speedup
    speedup = sequential_time / total_time
    print(f"Parallel speedup: {speedup:.2f}x faster")

if __name__ == "__main__":
    print("Starting test of parallel vs sequential processing...")
    asyncio.run(run_test()) 