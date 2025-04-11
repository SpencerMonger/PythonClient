import asyncio
from datetime import datetime, timezone
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
    Fetch trades for a ticker between dates using async HTTP.
    Uses nanosecond timestamps for API query and returns UTC datetime objects.
    Includes retry logic and better error handling.
    """
    trades_list = []
    
    try:
        # Use a ticker-specific session
        session = await get_aiohttp_session(ticker)
        
        # Determine if we're in live mode (short time range)
        is_live_mode = (to_date - from_date).total_seconds() < 120
        
        # Set timeout based on mode - much longer for historical
        timeout = aiohttp.ClientTimeout(total=5 if is_live_mode else 180)
        
        # Polygon v3 uses nanosecond timestamps
        from_ns = int(from_date.timestamp() * 1_000_000_000)
        to_ns = int(to_date.timestamp() * 1_000_000_000)
        
        url = f"{config.POLYGON_API_URL}/v3/trades/{ticker}"
        params = {
            "timestamp.gte": from_ns,
            "timestamp.lt": to_ns,
            "limit": 50000, # Keep limit high for both modes
            "order": "asc",
            "sort": "timestamp"
        }
        
        # Removed reduced limit for live mode
        if is_live_mode:
            print(f"Fetching trades for {ticker} in live mode with {timeout.total}s timeout (no pagination)")
        else:
            print(f"Fetching trades for {ticker} in historical mode with {timeout.total}s timeout (pagination enabled)")
        
        # For pagination control in historical mode
        next_url = None
        page_count = 0
        max_pages = 50 # Increased historical max_pages to 50
        
        # Make the first request
        current_url = url
        current_params = params
        
        while True: # Loop structure changed for clarity
            page_count += 1
            
            # Limit pages only in historical mode
            if not is_live_mode and page_count > max_pages:
                print(f"Stopping historical trades fetch for {ticker} after {max_pages} pages.")
                break
                
            try:
                async with session.get(current_url, params=current_params, timeout=timeout) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        print(f"Error fetching trades for {ticker} (page {page_count}): HTTP {response.status} URL: {response.url}")
                        print(f"Error response: {error_text[:200]}...")  # Print first 200 chars of error
                        break  # Exit the loop on error
                        
                    data = await response.json()
                    
                    if 'results' not in data or not data['results']:
                        if page_count == 1:
                            print(f"No results found in trades response for {ticker} (page {page_count})")
                        else:
                            print(f"No more results found in trades response for {ticker} (page {page_count})")
                        break # Exit loop if no results or empty results
                    
                    result_count = len(data.get('results', []))
                    print(f"Processing {result_count} trade records for {ticker} (page {page_count})")
                    
                    processed_in_page = 0
                    for trade in data.get('results', []):
                        try:
                            # Convert nanosecond timestamps to UTC datetime objects
                            sip_timestamp_utc = datetime.fromtimestamp(trade.get('sip_timestamp') / 1e9, tz=timezone.utc) if trade.get('sip_timestamp') else None
                            participant_timestamp_utc = datetime.fromtimestamp(trade.get('participant_timestamp') / 1e9, tz=timezone.utc) if trade.get('participant_timestamp') else None
                            trf_timestamp_utc = datetime.fromtimestamp(trade.get('trf_timestamp') / 1e9, tz=timezone.utc) if trade.get('trf_timestamp') else None

                            # Skip records with no sip_timestamp
                            if not sip_timestamp_utc:
                                continue

                            trades_list.append({
                                "ticker": ticker,
                                "sip_timestamp": sip_timestamp_utc,
                                "participant_timestamp": participant_timestamp_utc,
                                "trf_timestamp": trf_timestamp_utc,
                                "sequence_number": trade.get('sequence_number'),
                                "price": float(trade.get('price')) if trade.get('price') is not None else None,
                                "size": int(trade.get('size')) if trade.get('size') is not None else None,
                                "conditions": trade.get('conditions', []),
                                "exchange": int(trade.get('exchange')) if trade.get('exchange') is not None else None,
                                "tape": int(trade.get('tape')) if trade.get('tape') is not None else None
                            })
                            processed_in_page += 1
                        except Exception as e:
                            # Skip individual records that fail processing
                            print(f"Error processing trade record for {ticker}: {type(e).__name__} - {str(e)}")
                            continue
                    
                    print(f"Successfully processed {processed_in_page}/{result_count} trade records for {ticker} (page {page_count})")
                    
                    # Check for next_url for pagination ONLY IN HISTORICAL MODE
                    next_url = data.get('next_url')
                    
                    if is_live_mode or not next_url:
                        # Break after first page in live mode, or if no next_url in historical
                        if is_live_mode:
                            print(f"Live mode: Finished fetching trades for {ticker} (single request).")
                        elif not next_url:
                            print(f"Historical mode: No next_url found for trades of {ticker}. Fetch complete.")
                        break 
                    
                    # Prepare for next iteration (historical mode only)
                    if next_url:
                        # Make sure we're using our proxy URL instead of api.polygon.io
                        if "api.polygon.io" in next_url:
                            next_url = next_url.replace("https://api.polygon.io", config.POLYGON_API_URL)
                            
                        print(f"Historical mode: Found next_url for trades pagination: continuing to page {page_count + 1}")
                        current_url = next_url
                        current_params = None # next_url contains all params
                        
                        # Add a small delay between pagination requests
                        await asyncio.sleep(0.2)
                        
            except aiohttp.ClientError as e:
                print(f"HTTP client error fetching trades for {ticker} (page {page_count}): {type(e).__name__} - {str(e)}")
                break  # Exit loop on client error
            
    except asyncio.TimeoutError:
        print(f"Timeout fetching trades for {ticker}")
        return trades_list  # Return what we have so far
    except Exception as e:
        print(f"Error fetching trades for {ticker}: {type(e).__name__} - {str(e)}")
        return trades_list  # Return what we have so far
        
    return trades_list

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