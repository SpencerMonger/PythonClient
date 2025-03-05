from endpoints.main import main
from datetime import datetime
import asyncio

tickers = ["AAPL", "AMZN"]  # Add your tickers
from_date = datetime(2025, 1, 1)
to_date = datetime(2025, 1, 30)

asyncio.run(main(tickers, from_date, to_date))
