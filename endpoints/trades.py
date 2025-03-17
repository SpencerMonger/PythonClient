import asyncio
from datetime import datetime
from typing import Dict, List

from endpoints.polygon_client import get_rest_client
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
    Fetch trades for a ticker between dates
    """
    client = get_rest_client()
    trades = []
    
    try:
        # If the dates are timezone-aware (live mode), use nanosecond timestamps
        if from_date.tzinfo is not None and to_date.tzinfo is not None:
            from_ns = int(from_date.timestamp() * 1_000_000_000)
            to_ns = int(to_date.timestamp() * 1_000_000_000)
            print(f"Fetching trades for {ticker} from {from_date.strftime('%H:%M:00')} to {to_date.strftime('%H:%M:00')} ET...")
            for trade in client.list_trades(
                ticker=ticker,
                timestamp_gte=from_ns,
                timestamp_lt=to_ns,
                limit=50000
            ):
                trades.append({
                    "ticker": ticker,
                    "sip_timestamp": trade.sip_timestamp,
                    "participant_timestamp": trade.participant_timestamp,
                    "trf_timestamp": trade.trf_timestamp,
                    "sequence_number": trade.sequence_number,
                    "price": float(trade.price) if trade.price is not None else None,
                    "size": int(trade.size) if trade.size is not None else None,
                    "conditions": trade.conditions or [],
                    "exchange": int(trade.exchange) if trade.exchange is not None else None,
                    "tape": int(trade.tape) if trade.tape is not None else None
                })
        else:
            # For historical mode, use date strings
            from_str = from_date.strftime("%Y-%m-%d")
            to_str = to_date.strftime("%Y-%m-%d")
            print(f"Fetching trades for {ticker} from {from_str} to {to_str}...")
            for trade in client.list_trades(
                ticker=ticker,
                timestamp_gte=from_str,
                timestamp_lt=to_str,
                limit=50000
            ):
                trades.append({
                    "ticker": ticker,
                    "sip_timestamp": trade.sip_timestamp,
                    "participant_timestamp": trade.participant_timestamp,
                    "trf_timestamp": trade.trf_timestamp,
                    "sequence_number": trade.sequence_number,
                    "price": float(trade.price) if trade.price is not None else None,
                    "size": int(trade.size) if trade.size is not None else None,
                    "conditions": trade.conditions or [],
                    "exchange": int(trade.exchange) if trade.exchange is not None else None,
                    "tape": int(trade.tape) if trade.tape is not None else None
                })
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