import asyncio
from datetime import datetime
from typing import Dict, List
import aiohttp

from endpoints.polygon_client import get_rest_client, get_aiohttp_session
from endpoints.db import ClickHouseDB
from endpoints import config

# Schema for stock_trades table
TRADES_SCHEMA = {
    "ticker": "String",
    "sip_timestamp": "DateTime64(9)",  # Nanosecond precision
    "participant_timestamp": "Nullable(DateTime64(9))",
    "trf_timestamp": "Nullable(DateTime64(9))",
    "sequence_number": "Nullable(Int64)",
    "price": "Nullable(Float64)",
    "size": "Nullable(Int32)",
    "conditions": "Array(Int32)",
    "exchange": "Nullable(Int32)",
    "tape": "Nullable(Int32)"
}

async def fetch_trades(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch trades for a ticker between dates using async HTTP
    """
    trades = []
    
    try:
        # Use a ticker-specific session
        session = await get_aiohttp_session(ticker)
        
        # Set a shorter timeout
        timeout = aiohttp.ClientTimeout(total=5)
        
        # If the dates are timezone-aware (live mode), use nanosecond timestamps
        if from_date.tzinfo is not None and to_date.tzinfo is not None:
            from_ns = int(from_date.timestamp() * 1_000_000_000)
            to_ns = int(to_date.timestamp() * 1_000_000_000)
            
            url = f"{config.POLYGON_API_URL}/v3/trades/{ticker}"
            params = {
                "timestamp.gte": from_ns,
                "timestamp.lt": to_ns,
                "limit": 50000,
                "order": "asc",
                "sort": "timestamp"
            }
            
            # For live mode, we only need a few records for the last minute
            if (to_date - from_date).total_seconds() < 120:  # Less than 2 minutes
                params["limit"] = 100  # Reduced limit for live mode
            
            async with session.get(url, params=params, timeout=timeout) as response:
                if response.status != 200:
                    print(f"Error fetching trades for {ticker}: HTTP {response.status}")
                    return []
                    
                data = await response.json()
                
                if 'results' not in data:
                    # Return empty list without error to avoid cluttering logs
                    return []
                
                for trade in data.get('results', []):
                    trades.append({
                        "ticker": ticker,
                        "sip_timestamp": trade.get('sip_timestamp'),
                        "participant_timestamp": trade.get('participant_timestamp'),
                        "trf_timestamp": trade.get('trf_timestamp'),
                        "sequence_number": trade.get('sequence_number'),
                        "price": float(trade.get('price')) if trade.get('price') is not None else None,
                        "size": int(trade.get('size')) if trade.get('size') is not None else None,
                        "conditions": trade.get('conditions', []),
                        "exchange": int(trade.get('exchange')) if trade.get('exchange') is not None else None,
                        "tape": int(trade.get('tape')) if trade.get('tape') is not None else None
                    })
        else:
            # For historical mode, use date strings
            from_str = from_date.strftime("%Y-%m-%d")
            to_str = to_date.strftime("%Y-%m-%d")
            
            url = f"{config.POLYGON_API_URL}/v3/trades/{ticker}"
            params = {
                "timestamp.gte": from_str,
                "timestamp.lt": to_str,
                "limit": 50000,
                "order": "asc",
                "sort": "timestamp"
            }
            
            async with session.get(url, params=params, timeout=timeout) as response:
                if response.status != 200:
                    print(f"Error fetching trades for {ticker}: HTTP {response.status}")
                    return []
                    
                data = await response.json()
                
                if 'results' not in data:
                    # Return empty list without error to avoid cluttering logs
                    return []
                
                for trade in data.get('results', []):
                    trades.append({
                        "ticker": ticker,
                        "sip_timestamp": trade.get('sip_timestamp'),
                        "participant_timestamp": trade.get('participant_timestamp'),
                        "trf_timestamp": trade.get('trf_timestamp'),
                        "sequence_number": trade.get('sequence_number'),
                        "price": float(trade.get('price')) if trade.get('price') is not None else None,
                        "size": int(trade.get('size')) if trade.get('size') is not None else None,
                        "conditions": trade.get('conditions', []),
                        "exchange": int(trade.get('exchange')) if trade.get('exchange') is not None else None,
                        "tape": int(trade.get('tape')) if trade.get('tape') is not None else None
                    })
    except asyncio.TimeoutError:
        print(f"Timeout fetching trades for {ticker}")
        return []
    except Exception as e:
        print(f"Error fetching trades for {ticker}: {str(e)}")
        return []
        
    return trades

async def store_trades(db: ClickHouseDB, trades: List[Dict]) -> None:
    """
    Store trade data in ClickHouse
    """
    try:
        await db.insert_data(config.TABLE_STOCK_TRADES, trades)
    except Exception as e:
        print(f"Error storing trades: {str(e)}")

async def init_trades_table(db: ClickHouseDB) -> None:
    """
    Initialize the stock trades table
    """
    db.create_table_if_not_exists(config.TABLE_STOCK_TRADES, TRADES_SCHEMA) 