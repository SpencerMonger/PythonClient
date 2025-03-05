import asyncio
from datetime import datetime
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

async def fetch_sma(ticker: str, window: int) -> List[Dict]:
    """
    Fetch Simple Moving Average for a ticker
    """
    client = get_rest_client()
    indicators = []
    
    try:
        sma = client.get_sma(
            ticker=ticker,
            timespan="minute",  # Changed to minute intervals
            window=window,
            series_type=config.SERIES_TYPE
        )
        
        for value in sma.values:
            # Convert timestamp from milliseconds to datetime
            timestamp = datetime.fromtimestamp(value.timestamp / 1000.0)
            indicators.append({
                "ticker": ticker,
                "timestamp": timestamp,
                "indicator_type": "SMA",
                "window": window,
                "value": value.value,
                "signal": None,
                "histogram": None
            })
    except Exception as e:
        print(f"Error fetching SMA for {ticker} with window {window}: {str(e)}")
        return []
        
    return indicators

async def fetch_ema(ticker: str, window: int) -> List[Dict]:
    """
    Fetch Exponential Moving Average for a ticker
    """
    client = get_rest_client()
    indicators = []
    
    try:
        ema = client.get_ema(
            ticker=ticker,
            timespan="minute",  # Changed to minute intervals
            window=window,
            series_type=config.SERIES_TYPE
        )
        
        for value in ema.values:
            # Convert timestamp from milliseconds to datetime
            timestamp = datetime.fromtimestamp(value.timestamp / 1000.0)
            indicators.append({
                "ticker": ticker,
                "timestamp": timestamp,
                "indicator_type": "EMA",
                "window": window,
                "value": value.value,
                "signal": None,
                "histogram": None
            })
    except Exception as e:
        print(f"Error fetching EMA for {ticker} with window {window}: {str(e)}")
        return []
        
    return indicators

async def fetch_macd(ticker: str) -> List[Dict]:
    """
    Fetch MACD for a ticker
    """
    client = get_rest_client()
    indicators = []
    
    try:
        macd = client.get_macd(
            ticker=ticker,
            timespan="minute",  # Changed to minute intervals
            short_window=12,  # Standard MACD parameters
            long_window=26,
            signal_window=9,
            series_type=config.SERIES_TYPE
        )
        
        for value in macd.values:
            # Convert timestamp from milliseconds to datetime
            timestamp = datetime.fromtimestamp(value.timestamp / 1000.0)
            indicators.append({
                "ticker": ticker,
                "timestamp": timestamp,
                "indicator_type": "MACD",
                "window": 0,  # Not applicable for MACD
                "value": value.value,
                "signal": value.signal,
                "histogram": value.histogram
            })
    except Exception as e:
        print(f"Error fetching MACD for {ticker}: {str(e)}")
        return []
        
    return indicators

async def fetch_rsi(ticker: str) -> List[Dict]:
    """
    Fetch RSI for a ticker
    """
    client = get_rest_client()
    indicators = []
    
    try:
        rsi = client.get_rsi(
            ticker=ticker,
            timespan="minute",  # Changed to minute intervals
            window=14,  # Standard RSI window
            series_type=config.SERIES_TYPE
        )
        
        for value in rsi.values:
            # Convert timestamp from milliseconds to datetime
            timestamp = datetime.fromtimestamp(value.timestamp / 1000.0)
            indicators.append({
                "ticker": ticker,
                "timestamp": timestamp,
                "indicator_type": "RSI",
                "window": 14,
                "value": value.value,
                "signal": None,
                "histogram": None
            })
    except Exception as e:
        print(f"Error fetching RSI for {ticker}: {str(e)}")
        return []
        
    return indicators

async def fetch_all_indicators(ticker: str) -> List[Dict]:
    """
    Fetch all technical indicators for a ticker
    """
    indicators = []
    
    # Fetch SMA for different windows
    for window in config.SMA_WINDOWS:
        sma_data = await fetch_sma(ticker, window)
        indicators.extend(sma_data)
    
    # Fetch EMA for different windows
    for window in config.EMA_WINDOWS:
        ema_data = await fetch_ema(ticker, window)
        indicators.extend(ema_data)
    
    # Fetch MACD
    macd_data = await fetch_macd(ticker)
    indicators.extend(macd_data)
    
    # Fetch RSI
    rsi_data = await fetch_rsi(ticker)
    indicators.extend(rsi_data)
    
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
    db.create_table_if_not_exists(config.TABLE_STOCK_INDICATORS, INDICATORS_SCHEMA) 