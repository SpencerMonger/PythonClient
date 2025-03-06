import asyncio
from datetime import datetime, timedelta
from typing import Dict, List

from endpoints.polygon_client import get_rest_client
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
    Fetch daily bar data for a ticker between dates
    """
    client = get_rest_client()
    bars = []
    
    # Format dates as YYYY-MM-DD
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")
    
    try:
        for bar in client.list_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",  # Using day timespan
            from_=from_str,
            to=to_str,
            limit=50000
        ):
            # Convert timestamp from milliseconds to datetime
            timestamp = datetime.fromtimestamp(bar.timestamp / 1000.0)
            
            # Convert any potential None values to appropriate types
            bars.append({
                "ticker": ticker,
                "timestamp": timestamp,
                "open": float(bar.open) if bar.open is not None else None,
                "high": float(bar.high) if bar.high is not None else None,
                "low": float(bar.low) if bar.low is not None else None,
                "close": float(bar.close) if bar.close is not None else None,
                "volume": int(bar.volume) if bar.volume is not None else None,
                "vwap": float(bar.vwap) if bar.vwap is not None else None,
                "transactions": int(bar.transactions) if bar.transactions is not None else None
            })
    except Exception as e:
        print(f"Error fetching bars for {ticker}: {str(e)}")
        return []
        
    return bars

async def store_bars(db: ClickHouseDB, bars: List[Dict]) -> None:
    """
    Store bar data in ClickHouse
    """
    try:
        await db.insert_data(config.TABLE_STOCK_DAILY, bars)
    except Exception as e:
        print(f"Error storing bars: {str(e)}")

async def init_bars_table(db: ClickHouseDB) -> None:
    """
    Initialize the stock bars table
    """
    db.create_table_if_not_exists(config.TABLE_STOCK_DAILY, BARS_SCHEMA) 