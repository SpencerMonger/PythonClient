import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from endpoints.polygon_client import get_rest_client
from endpoints.db import ClickHouseDB
from endpoints import config

# Schema for stock_indicators table
INDICATORS_SCHEMA = {
    "ticker": "String",
    "timestamp": "DateTime64(9)",  # Nanosecond precision
    "indicator_type": "String",  # 'SMA', 'EMA', 'MACD', 'RSI'
    "window": "Int32",  # For SMA/EMA/RSI
    "value": "Float64",
    "signal": "Nullable(Float64)",  # For MACD
    "histogram": "Nullable(Float64)"  # For MACD
}

# Define window sizes for SMA and EMA
SMA_WINDOWS = [5, 9, 12, 20, 50, 100, 200]
EMA_WINDOWS = [9, 12, 20]

async def fetch_sma(ticker: str, window: int, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch Simple Moving Average for a ticker with specific window size
    """
    client = get_rest_client()
    indicators = []
    
    try:
        # Format dates as YYYY-MM-DD
        from_str = from_date.strftime("%Y-%m-%d")
        to_str = to_date.strftime("%Y-%m-%d")
        current_from = from_date
        
        while current_from < to_date:
            # Calculate the next date range
            current_to = min(current_from + timedelta(days=1), to_date)
            current_from_str = current_from.strftime("%Y-%m-%d")
            current_to_str = current_to.strftime("%Y-%m-%d")
            
            try:
                sma = client.get_sma(
                    ticker=ticker,
                    timespan="minute",
                    adjusted=True,
                    window=window,
                    series_type="close",
                    timestamp_gte=current_from_str,
                    timestamp_lte=current_to_str,
                    order="asc",
                    limit=5000  # Polygon's maximum limit
                )
                
                for value in sma.values:
                    # Convert timestamp from milliseconds to datetime
                    timestamp = datetime.fromtimestamp(value.timestamp / 1000.0)
                    indicators.append({
                        "ticker": ticker,
                        "timestamp": timestamp,
                        "indicator_type": f"SMA_{window}",
                        "window": window,
                        "value": float(value.value) if value.value is not None else 0.0,
                        "signal": None,
                        "histogram": None
                    })
                
                print(f"Fetched {len(sma.values)} SMA_{window} values for {current_from_str}")
                
            except Exception as e:
                print(f"Error fetching SMA_{window} for {ticker} on {current_from_str}: {str(e)}")
            
            # Move to next day
            current_from = current_to
            
    except Exception as e:
        print(f"Error in SMA_{window} pagination for {ticker}: {str(e)}")
        return []
        
    return indicators

async def fetch_ema(ticker: str, window: int, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch Exponential Moving Average for a ticker with specific window size
    """
    client = get_rest_client()
    indicators = []
    
    try:
        # Format dates as YYYY-MM-DD
        from_str = from_date.strftime("%Y-%m-%d")
        to_str = to_date.strftime("%Y-%m-%d")
        current_from = from_date
        
        while current_from < to_date:
            # Calculate the next date range
            current_to = min(current_from + timedelta(days=1), to_date)
            current_from_str = current_from.strftime("%Y-%m-%d")
            current_to_str = current_to.strftime("%Y-%m-%d")
            
            try:
                ema = client.get_ema(
                    ticker=ticker,
                    timespan="minute",
                    adjusted=True,
                    window=window,
                    series_type="close",
                    timestamp_gte=current_from_str,
                    timestamp_lte=current_to_str,
                    order="asc",
                    limit=5000  # Polygon's maximum limit
                )
                
                for value in ema.values:
                    # Convert timestamp from milliseconds to datetime
                    timestamp = datetime.fromtimestamp(value.timestamp / 1000.0)
                    indicators.append({
                        "ticker": ticker,
                        "timestamp": timestamp,
                        "indicator_type": f"EMA_{window}",
                        "window": window,
                        "value": float(value.value) if value.value is not None else 0.0,
                        "signal": None,
                        "histogram": None
                    })
                
                print(f"Fetched {len(ema.values)} EMA_{window} values for {current_from_str}")
                
            except Exception as e:
                print(f"Error fetching EMA_{window} for {ticker} on {current_from_str}: {str(e)}")
            
            # Move to next day
            current_from = current_to
            
    except Exception as e:
        print(f"Error in EMA_{window} pagination for {ticker}: {str(e)}")
        return []
        
    return indicators

async def fetch_macd(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch MACD for a ticker
    """
    client = get_rest_client()
    indicators = []
    
    try:
        # Format dates as YYYY-MM-DD
        from_str = from_date.strftime("%Y-%m-%d")
        to_str = to_date.strftime("%Y-%m-%d")
        current_from = from_date
        
        while current_from < to_date:
            # Calculate the next date range
            current_to = min(current_from + timedelta(days=1), to_date)
            current_from_str = current_from.strftime("%Y-%m-%d")
            current_to_str = current_to.strftime("%Y-%m-%d")
            
            try:
                macd = client.get_macd(
                    ticker=ticker,
                    timespan="minute",
                    adjusted=True,
                    short_window=12,  # Standard MACD parameters
                    long_window=26,
                    signal_window=9,
                    series_type="close",
                    timestamp_gte=current_from_str,
                    timestamp_lte=current_to_str,
                    order="asc",
                    limit=5000  # Polygon's maximum limit
                )
                
                for value in macd.values:
                    # Convert timestamp from milliseconds to datetime
                    timestamp = datetime.fromtimestamp(value.timestamp / 1000.0)
                    indicators.append({
                        "ticker": ticker,
                        "timestamp": timestamp,
                        "indicator_type": "MACD",
                        "window": 0,  # Not applicable for MACD
                        "value": float(value.value) if value.value is not None else 0.0,
                        "signal": float(value.signal) if value.signal is not None else 0.0,
                        "histogram": float(value.histogram) if value.histogram is not None else 0.0
                    })
                
                print(f"Fetched {len(macd.values)} MACD values for {current_from_str}")
                
            except Exception as e:
                print(f"Error fetching MACD for {ticker} on {current_from_str}: {str(e)}")
            
            # Move to next day
            current_from = current_to
            
    except Exception as e:
        print(f"Error in MACD pagination for {ticker}: {str(e)}")
        return []
        
    return indicators

async def fetch_rsi(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch RSI for a ticker
    """
    client = get_rest_client()
    indicators = []
    
    try:
        # Format dates as YYYY-MM-DD
        from_str = from_date.strftime("%Y-%m-%d")
        to_str = to_date.strftime("%Y-%m-%d")
        current_from = from_date
        
        while current_from < to_date:
            # Calculate the next date range
            current_to = min(current_from + timedelta(days=1), to_date)
            current_from_str = current_from.strftime("%Y-%m-%d")
            current_to_str = current_to.strftime("%Y-%m-%d")
            
            try:
                rsi = client.get_rsi(
                    ticker=ticker,
                    timespan="minute",
                    adjusted=True,
                    window=14,  # Standard RSI window
                    series_type="close",
                    timestamp_gte=current_from_str,
                    timestamp_lte=current_to_str,
                    order="asc",
                    limit=5000  # Polygon's maximum limit
                )
                
                for value in rsi.values:
                    # Convert timestamp from milliseconds to datetime
                    timestamp = datetime.fromtimestamp(value.timestamp / 1000.0)
                    indicators.append({
                        "ticker": ticker,
                        "timestamp": timestamp,
                        "indicator_type": "RSI",
                        "window": 14,
                        "value": float(value.value) if value.value is not None else 0.0,
                        "signal": None,
                        "histogram": None
                    })
                
                print(f"Fetched {len(rsi.values)} RSI values for {current_from_str}")
                
            except Exception as e:
                print(f"Error fetching RSI for {ticker} on {current_from_str}: {str(e)}")
            
            # Move to next day
            current_from = current_to
            
    except Exception as e:
        print(f"Error in RSI pagination for {ticker}: {str(e)}")
        return []
        
    return indicators

async def fetch_all_indicators(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch all technical indicators for a ticker
    """
    indicators = []
    
    # Fetch SMA for different windows
    for window in SMA_WINDOWS:
        print(f"Fetching SMA_{window} for {ticker}...")
        sma_data = await fetch_sma(ticker, window, from_date, to_date)
        indicators.extend(sma_data)
        if sma_data:
            print(f"Found {len(sma_data)} SMA_{window} values")
    
    # Fetch EMA for different windows
    for window in EMA_WINDOWS:
        print(f"Fetching EMA_{window} for {ticker}...")
        ema_data = await fetch_ema(ticker, window, from_date, to_date)
        indicators.extend(ema_data)
        if ema_data:
            print(f"Found {len(ema_data)} EMA_{window} values")
    
    # Fetch MACD
    print(f"Fetching MACD for {ticker}...")
    macd_data = await fetch_macd(ticker, from_date, to_date)
    indicators.extend(macd_data)
    if macd_data:
        print(f"Found {len(macd_data)} MACD values")
    
    # Fetch RSI
    print(f"Fetching RSI for {ticker}...")
    rsi_data = await fetch_rsi(ticker, from_date, to_date)
    indicators.extend(rsi_data)
    if rsi_data:
        print(f"Found {len(rsi_data)} RSI values")
    
    return indicators

async def store_indicators(db: ClickHouseDB, indicators: List[Dict]) -> None:
    """
    Store technical indicators data in ClickHouse
    """
    try:
        await db.insert_data(config.TABLE_STOCK_INDICATORS, indicators)
    except Exception as e:
        print(f"Error storing indicators: {str(e)}")

async def init_indicators_table(db: ClickHouseDB) -> None:
    """
    Initialize the stock indicators table
    """
    db.recreate_table(config.TABLE_STOCK_INDICATORS, INDICATORS_SCHEMA) 