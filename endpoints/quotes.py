import asyncio
from datetime import datetime, timezone
from typing import Dict, List

from endpoints.polygon_client import get_rest_client, get_aiohttp_session
from endpoints.db import ClickHouseDB
from endpoints import config
import aiohttp

# Schema for stock_quotes table
QUOTES_SCHEMA = {
    "ticker": "String",
    "sip_timestamp": "DateTime64(9)",  # Using SIP timestamp as primary timestamp
    "ask_exchange": "Nullable(Int32)",
    "ask_price": "Nullable(Float64)",
    "ask_size": "Nullable(Float64)",
    "bid_exchange": "Nullable(Int32)",
    "bid_price": "Nullable(Float64)",
    "bid_size": "Nullable(Float64)",
    "conditions": "Array(Int32)",
    "indicators": "Array(Int32)",
    "participant_timestamp": "Nullable(DateTime64(9))",
    "sequence_number": "Nullable(Int64)",
    "tape": "Nullable(Int32)",
    "trf_timestamp": "Nullable(DateTime64(9))"
}

async def fetch_quotes(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch quotes for a ticker between dates using async HTTP.
    Uses nanosecond timestamps for API query and returns UTC datetime objects.
    """
    quotes_list = []

    try:
        session = await get_aiohttp_session(ticker)
        timeout = aiohttp.ClientTimeout(total=5)

        # Polygon v3 uses nanosecond timestamps
        from_ns = int(from_date.timestamp() * 1_000_000_000)
        to_ns = int(to_date.timestamp() * 1_000_000_000)

        url = f"{config.POLYGON_API_URL}/v3/quotes/{ticker}"
        params = {
            "timestamp.gte": from_ns,
            "timestamp.lt": to_ns, # lt is exclusive
            "limit": 50000,
            "order": "asc",
            "sort": "timestamp"
        }

        # Reduced limit for live mode (less than 2 mins range)
        if (to_date - from_date).total_seconds() < 120:
            params["limit"] = 100

        async with session.get(url, params=params, timeout=timeout) as response:
            if response.status != 200:
                 print(f"Error fetching quotes for {ticker}: HTTP {response.status} URL: {response.url}")
                 return []

            data = await response.json()

            if 'results' not in data:
                return []

            for quote in data.get('results', []):
                 # Convert nanosecond timestamps to UTC datetime objects
                 sip_timestamp_utc = datetime.fromtimestamp(quote.get('sip_timestamp') / 1e9, tz=timezone.utc) if quote.get('sip_timestamp') else None
                 participant_timestamp_utc = datetime.fromtimestamp(quote.get('participant_timestamp') / 1e9, tz=timezone.utc) if quote.get('participant_timestamp') else None
                 trf_timestamp_utc = datetime.fromtimestamp(quote.get('trf_timestamp') / 1e9, tz=timezone.utc) if quote.get('trf_timestamp') else None

                 quotes_list.append({
                    "ticker": ticker,
                    "sip_timestamp": sip_timestamp_utc, # UTC datetime
                    "ask_exchange": int(quote.get('ask_exchange')) if quote.get('ask_exchange') is not None else None,
                    "ask_price": float(quote.get('ask_price')) if quote.get('ask_price') is not None else None,
                    "ask_size": float(quote.get('ask_size')) if quote.get('ask_size') is not None else None,
                    "bid_exchange": int(quote.get('bid_exchange')) if quote.get('bid_exchange') is not None else None,
                    "bid_price": float(quote.get('bid_price')) if quote.get('bid_price') is not None else None,
                    "bid_size": float(quote.get('bid_size')) if quote.get('bid_size') is not None else None,
                    "conditions": quote.get('conditions', []),
                    "indicators": quote.get('indicators', []),
                    "participant_timestamp": participant_timestamp_utc, # UTC datetime
                    "sequence_number": int(quote.get('sequence_number')) if quote.get('sequence_number') is not None else None,
                    "tape": int(quote.get('tape')) if quote.get('tape') is not None else None,
                    "trf_timestamp": trf_timestamp_utc # UTC datetime
                 })

    except asyncio.TimeoutError:
        print(f"Timeout fetching quotes for {ticker}")
        return []
    except Exception as e:
        print(f"Error fetching quotes for {ticker}: {type(e).__name__} - {str(e)}")
        return []

    return quotes_list

async def store_quotes(db: ClickHouseDB, quotes: List[Dict]) -> None:
    """
    Store quote data in ClickHouse
    """
    try:
        await db.insert_data(config.TABLE_STOCK_QUOTES, quotes)
    except Exception as e:
        print(f"Error storing quotes: {str(e)}")

async def init_quotes_table(db: ClickHouseDB) -> None:
    """
    Initialize the stock quotes table
    """
    db.create_table_if_not_exists(config.TABLE_STOCK_QUOTES, QUOTES_SCHEMA) 