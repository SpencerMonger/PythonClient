from endpoints.main import main
from datetime import datetime
import asyncio

tickers = ["AAPL", "AMZN", "TSLA", "NVDA", "MSFT", "GOOGL", "META", "AMD"]  # Add your tickers
from_date = datetime(2025, 1, 2)
to_date = datetime(2025, 2, 28)

asyncio.run(main(tickers, from_date, to_date))
