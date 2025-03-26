import asyncio
from datetime import datetime, timedelta
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
    Fetch minute bar data for a ticker between dates using async HTTP
    """
    # Format dates as YYYY-MM-DD
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")
    
    url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/{config.TIMESPAN}/{from_str}/{to_str}?limit=50000"
    
    try:
        # Use a ticker-specific session
        session = await get_aiohttp_session(ticker)
        
        # Set a shorter timeout for the specific request
        timeout = aiohttp.ClientTimeout(total=5)
        
        async with session.get(url, timeout=timeout) as response:
            if response.status != 200:
                print(f"Error fetching bars for {ticker}: HTTP {response.status}")
                return []
                
            data = await response.json()
            
            if data.get('status') != 'OK' or 'results' not in data:
                print(f"Error in API response for {ticker} bars: {data.get('error', 'Unknown error')}")
                return []
                
            bars = []
            for bar in data['results']:
                # Convert timestamp from milliseconds to nanoseconds for DateTime64(9)
                timestamp = bar['t'] * 1_000_000  # ms to ns
                
                # Add sub-millisecond precision to match trades/quotes format
                timestamp = int(timestamp + (timestamp % 1000))
                
                # Debug prints for the first bar only (disabled for performance)
                # if len(bars) == 0:
                #     print(f"\nTimestamp debug for first bar:")
                #     print(f"Original timestamp (ms): {bar['t']}")
                #     print(f"Converted timestamp (ns): {timestamp}")
                #     print(f"Timestamp length: {len(str(timestamp))} digits")
                
                # Convert any potential None values to appropriate types
                bars.append({
                    "ticker": ticker,
                    "timestamp": timestamp,
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
    except Exception as e:
        print(f"Error fetching bars for {ticker}: {str(e)}")
        return []
        
    return bars

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