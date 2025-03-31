import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List
import json
import aiohttp

from endpoints.polygon_client import get_rest_client, get_aiohttp_session
from endpoints.db import ClickHouseDB
from endpoints import config

# Schema for stock_bars table
BARS_SCHEMA = {
    "ticker": "String",
    "timestamp": "DateTime64(9)",  # Nanosecond precision
    "open": "Nullable(Float64)",
    "high": "Nullable(Float64)",
    "low": "Nullable(Float64)",
    "close": "Nullable(Float64)",
    "volume": "Nullable(Int64)",
    "vwap": "Nullable(Float64)",
    "transactions": "Nullable(Int64)"
}

async def fetch_bars(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch minute bar data for a ticker between dates using async HTTP.
    Uses precise nanosecond timestamps for timezone-aware inputs (expected UTC).
    """
    bars_list = []

    try:
        # Use a ticker-specific session
        session = await get_aiohttp_session(ticker)
        # Set a shorter timeout for the specific request
        timeout = aiohttp.ClientTimeout(total=5)

        # Construct URL and params based on whether dates are timezone-aware
        # Polygon Aggs v2 uses timestamp query params: timestamp.gte/gt/lte/lt
        # It supports nanoseconds epoch or YYYY-MM-DDTHH:MM:SSZ format
        base_url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/{config.TIMESPAN}"
        params = {"limit": 50000, "sort": "asc"} # Sort ascending to get chronological order

        if from_date.tzinfo is not None and to_date.tzinfo is not None:
            # Use nanosecond timestamps for precise range (expected UTC)
            # Polygon uses [gte, lte] inclusive range based on testing Aggs v2
            # Let's use gte and lt for consistency with trades/quotes v3
            from_ns = int(from_date.timestamp() * 1_000_000_000)
            to_ns = int(to_date.timestamp() * 1_000_000_000)
            params["timestamp.gte"] = from_ns
            params["timestamp.lt"] = to_ns
            # Construct final URL with range path placeholders not needed when using timestamp params
            url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/{config.TIMESPAN}/{from_date.strftime('%Y-%m-%d')}/{to_date.strftime('%Y-%m-%d')}"
            # ^^^ Let's correct this URL construction. Range path is not needed with timestamp params.
            url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/{config.TIMESPAN}/{from_date.strftime('%Y-%m-%d')}/{to_date.strftime('%Y-%m-%d')}"
            # No, the range path *is* needed. Let's re-read Polygon docs.
            # Docs V2 Aggregates: /v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}
            # It seems {from} and {to} are mandatory path params.
            # Let's use the original date strings for the path, but add the precise timestamp filters
            from_str_path = from_date.strftime("%Y-%m-%d")
            to_str_path = to_date.strftime("%Y-%m-%d")
            url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/{config.TIMESPAN}/{from_str_path}/{to_str_path}"
            print(f"Fetching bars for {ticker} using URL: {url} and precise UTC params: {params}") # Debug print

        else:
            # Fallback for naive dates (historical mode - less precise)
            from_str = from_date.strftime("%Y-%m-%d")
            to_str = to_date.strftime("%Y-%m-%d")
            url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/{config.TIMESPAN}/{from_str}/{to_str}"
            params["limit"] = 50000 # Ensure limit is set for historical too
            print(f"Fetching bars for {ticker} using URL: {url} and params: {params}") # Debug print


        async with session.get(url, params=params, timeout=timeout) as response:
            if response.status != 200:
                print(f"Error fetching bars for {ticker}: HTTP {response.status} - URL: {response.url}")
                # Attempt to read error body
                try:
                    error_body = await response.text()
                    print(f"Error body: {error_body[:500]}") # Print first 500 chars
                except Exception:
                    pass # Ignore errors reading the error body
                return []

            data = await response.json()

            if data.get('status') != 'OK' or 'results' not in data:
                # Polygon sometimes returns 200 OK with an error status internally
                print(f"Error in API response for {ticker} bars: {data.get('message', data.get('error', 'Unknown error'))}")
                return []

            for bar in data['results']:
                # Convert timestamp from milliseconds to nanoseconds for DateTime64(9)
                # Polygon 't' is Unix Msec timestamp UTC of the start of the aggregate window.
                timestamp_ms = bar['t']
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc) # Create UTC datetime

                # We store UTC, db.py will handle insertion
                bars_list.append({
                    "ticker": ticker,
                    "timestamp": timestamp_dt, # Store the UTC datetime object
                    "open": float(bar['o']) if 'o' in bar and bar['o'] is not None else None,
                    "high": float(bar['h']) if 'h' in bar and bar['h'] is not None else None,
                    "low": float(bar['l']) if 'l' in bar and bar['l'] is not None else None,
                    "close": float(bar['c']) if 'c' in bar and bar['c'] is not None else None,
                    "volume": int(bar['v']) if 'v' in bar and bar['v'] is not None else None,
                    "vwap": float(bar['vw']) if 'vw' in bar and bar['vw'] is not None else None,
                    "transactions": int(bar['n']) if 'n' in bar and bar['n'] is not None else None
                })
    except asyncio.TimeoutError:
        print(f"Timeout fetching bars for {ticker}")
        return []
    except aiohttp.ClientConnectorError as e:
         print(f"Connection error fetching bars for {ticker}: {e}")
         return []
    except Exception as e:
        print(f"General error fetching bars for {ticker}: {type(e).__name__} - {str(e)}")
        import traceback
        print(traceback.format_exc()) # Print full traceback for unexpected errors
        return []

    return bars_list

async def store_bars(db: ClickHouseDB, bars: List[Dict]) -> None:
    """
    Store bar data in ClickHouse
    """
    try:
        await db.insert_data(config.TABLE_STOCK_BARS, bars)
    except Exception as e:
        print(f"Error storing bars: {str(e)}")

async def init_bars_table(db: ClickHouseDB) -> None:
    """
    Initialize the stock bars table
    """
    db.create_table_if_not_exists(config.TABLE_STOCK_BARS, BARS_SCHEMA) 