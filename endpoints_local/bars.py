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
        # Determine if we're in live mode (short time range)
        is_live_mode = (to_date - from_date).total_seconds() < 120
        
        # Use a ticker-specific session
        session = await get_aiohttp_session(ticker)
        
        # Set timeout based on mode - much longer for historical
        timeout = aiohttp.ClientTimeout(total=5 if is_live_mode else 60)
        
        # Default params
        params = {
            "adjusted": "true",
            "sort": "asc"
        }
        
        # For precise time-specific calls in live mode:
        if is_live_mode and from_date.tzinfo is not None and to_date.tzinfo is not None:
            # Calculate precise timestamps - add 1 ns to end for inclusive range
            from_timestamp_ns = int(from_date.timestamp() * 1e9)
            to_timestamp_ns = int(to_date.timestamp() * 1e9) + 1
            
            # Format in ISO format
            params["timestamp.gte"] = str(from_timestamp_ns)
            params["timestamp.lt"] = str(to_timestamp_ns)
            
            # Add precision parameters
            params["precision"] = 1  # Nanosecond precision
            params["limit"] = 50000 # Ensure high limit for live mode too
            
            # Use the v2 aggregates endpoint with precise ISO timestamps
            from_str_path = from_date.strftime("%Y-%m-%d")
            to_str_path = to_date.strftime("%Y-%m-%d")
            url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/{config.TIMESPAN}/{from_str_path}/{to_str_path}"
            print(f"Fetching bars for {ticker} using URL: {url} and precise UTC params: {params}")

        else:
            # Fallback for naive dates (historical mode - less precise)
            from_str = from_date.strftime("%Y-%m-%d")
            to_str = to_date.strftime("%Y-%m-%d")
            url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/{config.TIMESPAN}/{from_str}/{to_str}"
            params["limit"] = 50000 # Ensure limit is set for historical too
            print(f"Fetching bars for {ticker} using URL: {url} and params: {params}")

        print(f"Using timeout: {timeout.total} seconds (live mode: {is_live_mode})")
        
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