from endpoints.main import main
from datetime import datetime
import asyncio
import time

tickers = ["AAPL", "AMZN", "TSLA", "NVDA", "MSFT", "GOOGL", "META", "AMD"]  # Add your tickers
from_date = datetime(2025, 3, 4)
to_date = datetime(2025, 3, 5)

print(f"\nStarting data processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Processing {len(tickers)} tickers from {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")

start_time = time.time()
asyncio.run(main(tickers, from_date, to_date))
print(f"\nScript completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total wall clock time: {time.time() - start_time:.2f} seconds")
