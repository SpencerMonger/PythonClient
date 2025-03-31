import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import aiohttp # Import aiohttp

from endpoints.polygon_client import get_rest_client, get_aiohttp_session
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
    """ Fetch SMA. Relies on limit=1 and timespan='minute' for live mode. Verifies timestamp of the result. """
    client = get_rest_client() # Keep for historical mode
    indicators = []
    is_live_mode = (to_date - from_date) < timedelta(minutes=2) # Check if interval is short

    try:
        if is_live_mode:
            # Live Mode: Use direct aiohttp call with simplified params
            session = await get_aiohttp_session(ticker)
            timeout = aiohttp.ClientTimeout(total=5) # Use a reasonable timeout for direct call
            url = f"{config.POLYGON_API_URL}/v1/indicators/sma/{ticker}"
            # SIMPLIFIED PARAMS: Rely on timespan and limit, filter by timestamp in Python
            params = {
                "timespan": "minute", "adjusted": "true", "window": window, "series_type": "close",
                "limit": 5 # Get a few recent points
                # Removed timestamp.gte, timestamp.lt, order
            }
            # print(f"Fetching SMA_{window} (Live/aiohttp/Simple) for {ticker} URL: {url} PARAMS: {params}") # Debug
            try:
                async with session.get(url, params=params, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data and 'results' in data and 'values' in data['results'] and data['results']['values']:
                            # Loop through the few values returned
                            for value_data in data['results']['values']:
                                timestamp_ms = value_data.get('timestamp')
                                value_val = value_data.get('value')
                                if timestamp_ms is not None and value_val is not None:
                                    timestamp_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                                    # Check if this value is for the target minute
                                    if from_date <= timestamp_utc < to_date:
                                        print(f"Got valid SMA_{window} for {ticker} at {timestamp_utc}") # Debug success
                                        indicators.append({
                                            "ticker": ticker, "timestamp": timestamp_utc, "indicator_type": f"SMA_{window}",
                                            "window": window, "value": float(value_val),
                                            "signal": None, "histogram": None
                                        })
                                        # Found the value for this minute, no need to check older ones
                                        break
                    # else: print(f"Non-200 status for SMA_{window} {ticker}: {response.status}") # Debug

            except asyncio.TimeoutError: print(f"Timeout fetching SMA_{window} (Live/aiohttp) for {ticker}")
            except Exception as e: print(f"Error fetching SMA_{window} (Live/aiohttp) for {ticker}: {type(e).__name__} - {e}")

        else: # Historical Mode logic remains the same (using polygon-api-client)
             current_from = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
             loop_end_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
             while current_from < loop_end_date:
                api_from_date_str = current_from.strftime("%Y-%m-%d")
                api_to_date_str = api_from_date_str
                params = {"timespan": "minute", "adjusted": "true", "window": window, "series_type": "close", "timestamp_gte": api_from_date_str, "timestamp_lte": api_to_date_str, "order": "asc", "limit": 5000 }
                try:
                    sma_result = client.get_sma(ticker=ticker, **params)
                    for value in sma_result.values:
                         timestamp_ms = value.timestamp
                         timestamp_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                         indicators.append({ "ticker": ticker, "timestamp": timestamp_utc, "indicator_type": f"SMA_{window}", "window": window, "value": float(value.value) if value.value is not None else 0.0, "signal": None, "histogram": None })
                except Exception as e:
                    print(f"Error fetching SMA_{window} (Hist) for {ticker} on {api_from_date_str}: {str(e)}")
                current_from = current_from + timedelta(days=1)

    except Exception as e: print(f"General error in fetch_sma for {ticker}: {str(e)}")
    return indicators

async def fetch_ema(ticker: str, window: int, from_date: datetime, to_date: datetime) -> List[Dict]:
    """ Fetch EMA. Uses aiohttp with simplified params for live mode. """
    client = get_rest_client() # Keep for historical
    indicators = []
    is_live_mode = (to_date - from_date) < timedelta(minutes=2)
    try:
        if is_live_mode:
            session = await get_aiohttp_session(ticker)
            timeout = aiohttp.ClientTimeout(total=5)
            url = f"{config.POLYGON_API_URL}/v1/indicators/ema/{ticker}"
            params = {"timespan": "minute", "adjusted": "true", "window": window, "series_type": "close", "limit": 5}
            try:
                async with session.get(url, params=params, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data and 'results' in data and 'values' in data['results'] and data['results']['values']:
                            for value_data in data['results']['values']:
                                timestamp_ms = value_data.get('timestamp'); value_val = value_data.get('value')
                                if timestamp_ms is not None and value_val is not None:
                                    ts_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                                    if from_date <= ts_utc < to_date:
                                         print(f"Got valid EMA_{window} for {ticker} at {ts_utc}")
                                         indicators.append({ "ticker": ticker, "timestamp": ts_utc, "indicator_type": f"EMA_{window}", "window": window, "value": float(value_val), "signal": None, "histogram": None })
                                         break
            except asyncio.TimeoutError: print(f"Timeout fetching EMA_{window} (Live/aiohttp) for {ticker}")
            except Exception as e: print(f"Error fetching EMA_{window} (Live/aiohttp) for {ticker}: {type(e).__name__} - {e}")
        else: # Historical mode...
            current_from = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
            loop_end_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            while current_from < loop_end_date:
                 api_from_date_str = current_from.strftime("%Y-%m-%d"); api_to_date_str = api_from_date_str
                 params = {"timespan": "minute", "adjusted": "true", "window": window, "series_type": "close", "timestamp_gte": api_from_date_str, "timestamp_lte": api_to_date_str, "order": "asc", "limit": 5000}
                 try:
                     ema_result = client.get_ema(ticker=ticker, **params) # Historical uses client
                     for value in ema_result.values:
                          timestamp_ms = value.timestamp; ts_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                          indicators.append({ "ticker": ticker, "timestamp": ts_utc, "indicator_type": f"EMA_{window}", "window": window, "value": float(value.value) if value.value is not None else 0.0, "signal": None, "histogram": None })
                 except Exception as e: print(f"Error fetching EMA_{window} (Hist) for {ticker} on {api_from_date_str}: {str(e)}")
                 current_from = current_from + timedelta(days=1)
    except Exception as e: print(f"General error in fetch_ema for {ticker}: {str(e)}")
    return indicators

async def fetch_macd(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """ Fetch MACD. Uses aiohttp with simplified params for live mode. """
    client = get_rest_client() # Keep for historical
    indicators = []
    is_live_mode = (to_date - from_date) < timedelta(minutes=2)
    try:
        if is_live_mode:
             session = await get_aiohttp_session(ticker)
             timeout = aiohttp.ClientTimeout(total=5)
             url = f"{config.POLYGON_API_URL}/v1/indicators/macd/{ticker}"
             params = {"timespan": "minute", "adjusted": "true", "short_window":12, "long_window":26, "signal_window":9, "series_type": "close", "limit": 5}
             try:
                 async with session.get(url, params=params, timeout=timeout) as response:
                     if response.status == 200:
                         data = await response.json()
                         if data and 'results' in data and 'values' in data['results'] and data['results']['values']:
                             for value_data in data['results']['values']:
                                 timestamp_ms = value_data.get('timestamp'); value_val = value_data.get('value'); signal_val = value_data.get('signal'); hist_val = value_data.get('histogram')
                                 if timestamp_ms is not None and value_val is not None:
                                     ts_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                                     if from_date <= ts_utc < to_date:
                                         print(f"Got valid MACD for {ticker} at {ts_utc}")
                                         indicators.append({ "ticker": ticker, "timestamp": ts_utc, "indicator_type": "MACD", "window": 0, "value": float(value_val), "signal": float(signal_val) if signal_val is not None else None, "histogram": float(hist_val) if hist_val is not None else None })
                                         break
             except asyncio.TimeoutError: print(f"Timeout fetching MACD (Live/aiohttp) for {ticker}")
             except Exception as e: print(f"Error fetching MACD (Live/aiohttp) for {ticker}: {type(e).__name__} - {e}")
        else: # Historical mode...
            current_from = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
            loop_end_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            while current_from < loop_end_date:
                 api_from_date_str = current_from.strftime("%Y-%m-%d"); api_to_date_str = api_from_date_str
                 params = {"timespan": "minute", "adjusted": "true", "short_window":12, "long_window":26, "signal_window":9, "series_type": "close", "timestamp_gte": api_from_date_str, "timestamp_lte": api_to_date_str, "order": "asc", "limit": 5000}
                 try:
                     macd_result = client.get_macd(ticker=ticker, **params) # Historical uses client
                     for value in macd_result.values:
                         timestamp_ms = value.timestamp; ts_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                         indicators.append({ "ticker": ticker, "timestamp": ts_utc, "indicator_type": "MACD", "window": 0, "value": float(value.value) if value.value is not None else 0.0, "signal": float(value.signal) if value.signal is not None else 0.0, "histogram": float(value.histogram) if value.histogram is not None else 0.0 })
                 except Exception as e: print(f"Error fetching MACD (Hist) for {ticker} on {api_from_date_str}: {str(e)}")
                 current_from = current_from + timedelta(days=1)
    except Exception as e: print(f"General error in fetch_macd for {ticker}: {str(e)}")
    return indicators

async def fetch_rsi(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """ Fetch RSI. Uses aiohttp with simplified params for live mode. """
    client = get_rest_client() # Keep for historical
    indicators = []
    is_live_mode = (to_date - from_date) < timedelta(minutes=2)
    try:
        if is_live_mode:
             session = await get_aiohttp_session(ticker)
             timeout = aiohttp.ClientTimeout(total=5)
             url = f"{config.POLYGON_API_URL}/v1/indicators/rsi/{ticker}"
             params = {"timespan": "minute", "adjusted": "true", "window":14, "series_type": "close", "limit": 5}
             try:
                 async with session.get(url, params=params, timeout=timeout) as response:
                      if response.status == 200:
                          data = await response.json()
                          if data and 'results' in data and 'values' in data['results'] and data['results']['values']:
                              for value_data in data['results']['values']:
                                  timestamp_ms = value_data.get('timestamp'); value_val = value_data.get('value')
                                  if timestamp_ms is not None and value_val is not None:
                                       ts_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                                       if from_date <= ts_utc < to_date:
                                            print(f"Got valid RSI for {ticker} at {ts_utc}")
                                            indicators.append({ "ticker": ticker, "timestamp": ts_utc, "indicator_type": "RSI", "window": 14, "value": float(value_val), "signal": None, "histogram": None })
                                            break
             except asyncio.TimeoutError: print(f"Timeout fetching RSI (Live/aiohttp) for {ticker}")
             except Exception as e: print(f"Error fetching RSI (Live/aiohttp) for {ticker}: {type(e).__name__} - {e}")
        else: # Historical mode...
            current_from = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
            loop_end_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            while current_from < loop_end_date:
                 api_from_date_str = current_from.strftime("%Y-%m-%d"); api_to_date_str = api_from_date_str
                 params = {"timespan": "minute", "adjusted": "true", "window":14, "series_type": "close", "timestamp_gte": api_from_date_str, "timestamp_lte": api_to_date_str, "order": "asc", "limit": 5000}
                 try:
                     rsi_result = client.get_rsi(ticker=ticker, **params) # Historical uses client
                     for value in rsi_result.values:
                           timestamp_ms = value.timestamp; ts_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                           indicators.append({ "ticker": ticker, "timestamp": ts_utc, "indicator_type": "RSI", "window": 14, "value": float(value.value) if value.value is not None else 0.0, "signal": None, "histogram": None })
                 except Exception as e: print(f"Error fetching RSI (Hist) for {ticker} on {api_from_date_str}: {str(e)}")
                 current_from = current_from + timedelta(days=1)
    except Exception as e: print(f"General error in fetch_rsi for {ticker}: {str(e)}")
    return indicators

async def fetch_all_indicators(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch all technical indicators concurrently for a ticker using asyncio.gather.
    (Restored concurrent fetching as outer concurrency is now limited)
    """
    all_indicators_data = []
    tasks = []
    # print(f"Creating concurrent indicator fetch tasks for {ticker}...") # Debug

    # Create tasks for fetching SMAs
    for window in SMA_WINDOWS:
        tasks.append(fetch_sma(ticker, window, from_date, to_date))

    # Create tasks for fetching EMAs
    for window in EMA_WINDOWS:
        tasks.append(fetch_ema(ticker, window, from_date, to_date))

    # Create task for fetching MACD
    tasks.append(fetch_macd(ticker, from_date, to_date))

    # Create task for fetching RSI
    tasks.append(fetch_rsi(ticker, from_date, to_date))

    # Gather results from all tasks concurrently
    print(f"Gathering all indicator results concurrently for {ticker}...")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    print(f"Finished gathering indicators for {ticker}.")

    # Process results, handling potential exceptions
    for result in results:
        if isinstance(result, Exception):
            print(f"Error gathering indicator result for {ticker}: {type(result).__name__} - {result}")
        elif isinstance(result, list):
            all_indicators_data.extend(result)

    if all_indicators_data:
         print(f"Found total {len(all_indicators_data)} indicator values concurrently for {ticker}")
    else:
         print(f"Found no indicator values concurrently for {ticker}")


    return all_indicators_data

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