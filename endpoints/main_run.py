from endpoints.main import main
from datetime import datetime
import asyncio

tickers = ["AAPL", "AMZN"]  # Add your tickers
from_date = datetime(2025, 2, 3)
to_date = datetime(2025, 2, 7)

asyncio.run(main(tickers, from_date, to_date))
