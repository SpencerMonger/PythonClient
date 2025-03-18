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

async def store_bars(db: ClickHouseDB, bars: List[Dict], mode: str = "historical") -> None:
    """
    Store bar data in ClickHouse, avoiding duplicates for daily data in live mode only
    
    Args:
        db: Database connection
        bars: List of bar data to store
        mode: Either "historical" or "live". Duplicate checking only happens in live mode.
    """
    try:
        if not bars:
            return
            
        if mode == "live":
            # For live mode, check for duplicates
            filtered_bars = []
            for bar in bars:
                # Format the timestamp for the query
                check_date = bar['timestamp'].strftime('%Y-%m-%d')
                
                # Query to check if we already have data for this ticker and date
                query = f"""
                    SELECT 1
                    FROM {db.database}.{config.TABLE_STOCK_DAILY}
                    WHERE ticker = '{bar['ticker']}'
                    AND toDate(timestamp) = toDate('{check_date}')
                    LIMIT 1
                """
                
                try:
                    result = db.client.command(query)
                    if not result:  # No existing data found for this ticker and date
                        filtered_bars.append(bar)
                    else:
                        print(f"Skipping existing daily bar for {bar['ticker']} on {check_date}")
                except Exception as e:
                    print(f"Error checking for existing data: {str(e)}")
                    # If there's an error checking, assume we should insert the data
                    filtered_bars.append(bar)
            
            if filtered_bars:
                await db.insert_data(config.TABLE_STOCK_DAILY, filtered_bars)
                print(f"Stored {len(filtered_bars)} new daily bars (filtered out {len(bars) - len(filtered_bars)} existing bars)")
            else:
                print("No new daily bars to store (all bars already exist)")
        else:
            # For historical mode, store all bars without checking
            await db.insert_data(config.TABLE_STOCK_DAILY, bars)
            print(f"Stored {len(bars)} daily bars in historical mode")
            
    except Exception as e:
        print(f"Error storing bars: {str(e)}")

async def init_bars_table(db: ClickHouseDB) -> None:
    """
    Initialize the stock bars table
    """
    db.create_table_if_not_exists(config.TABLE_STOCK_DAILY, BARS_SCHEMA) 