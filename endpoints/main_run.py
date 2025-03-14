from endpoints.main import main
from datetime import datetime
import asyncio

tickers = ["AAPL", "AMZN", "TSLA", "NVDA", "MSFT", "GOOGL", "META", "AMD"]  # Add your tickers
from_date = datetime(2025, 3, 6)
to_date = datetime(2025, 3, 7)

asyncio.run(main(tickers, from_date, to_date))
