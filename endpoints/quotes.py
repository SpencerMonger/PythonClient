import asyncio
from datetime import datetime
from typing import Dict, List

from endpoints.polygon_client import get_rest_client
from endpoints.db import ClickHouseDB
from endpoints import config

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
    Fetch quotes for a ticker between dates
    """
    client = get_rest_client()
    quotes = []
    
    # Format dates as YYYY-MM-DD
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")
    
    try:
        for quote in client.list_quotes(
            ticker=ticker,
            timestamp_gte=from_str,
            timestamp_lt=to_str,
            limit=50000
        ):
            quotes.append({
                "ticker": ticker,
                "sip_timestamp": quote.sip_timestamp,
                "ask_exchange": int(quote.ask_exchange) if quote.ask_exchange is not None else None,
                "ask_price": float(quote.ask_price) if quote.ask_price is not None else None,
                "ask_size": float(quote.ask_size) if quote.ask_size is not None else None,
                "bid_exchange": int(quote.bid_exchange) if quote.bid_exchange is not None else None,
                "bid_price": float(quote.bid_price) if quote.bid_price is not None else None,
                "bid_size": float(quote.bid_size) if quote.bid_size is not None else None,
                "conditions": quote.conditions or [],
                "indicators": quote.indicators or [],
                "participant_timestamp": quote.participant_timestamp,
                "sequence_number": int(quote.sequence_number) if quote.sequence_number is not None else None,
                "tape": int(quote.tape) if quote.tape is not None else None,
                "trf_timestamp": quote.trf_timestamp
            })
    except Exception as e:
        print(f"Error fetching quotes for {ticker}: {str(e)}")
        return []
        
    return quotes

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