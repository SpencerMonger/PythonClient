import asyncio
from datetime import datetime, timezone
from typing import Dict, List

from endpoints.polygon_client import get_rest_client, get_aiohttp_session
from endpoints.db import ClickHouseDB
from endpoints import config
import aiohttp

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
    Fetch quotes for a ticker between dates using async HTTP.
    Uses nanosecond timestamps for API query and returns UTC datetime objects.
    Includes retry logic and better error handling.
    """
    quotes_list = []
    
    try:
        # Use a ticker-specific session
        session = await get_aiohttp_session(ticker)
        
        # Determine if we're in live mode (short time range)
        is_live_mode = (to_date - from_date).total_seconds() < 120
        
        # Set timeout based on mode - much longer for historical mode
        timeout = aiohttp.ClientTimeout(total=5 if is_live_mode else 120)
        
        # Polygon v3 uses nanosecond timestamps
        from_ns = int(from_date.timestamp() * 1_000_000_000)
        to_ns = int(to_date.timestamp() * 1_000_000_000)
        
        url = f"{config.POLYGON_API_URL}/v3/quotes/{ticker}"
        params = {
            "timestamp.gte": from_ns,
            "timestamp.lt": to_ns,
            "limit": 50000, # Keep limit high for both modes
            "order": "asc",
            "sort": "timestamp"
        }
        
        # Removed reduced limit for live mode
        if is_live_mode:
            print(f"Fetching quotes for {ticker} in live mode with {timeout.total}s timeout (no pagination)")
        else:
            print(f"Fetching quotes for {ticker} in historical mode with {timeout.total}s timeout (pagination enabled)")
        
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
                print(f"Stopping historical quotes fetch for {ticker} after {max_pages} pages.")
                break
            
            try:
                async with session.get(current_url, params=current_params, timeout=timeout) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        print(f"Error fetching quotes for {ticker} (page {page_count}): HTTP {response.status} URL: {response.url}")
                        print(f"Error response: {error_text[:200]}...")  # Print first 200 chars of error
                        break  # Exit the loop on error

                    data = await response.json()
                    
                    if 'results' not in data or not data['results']:
                        if page_count == 1:
                           print(f"No results found in quotes response for {ticker} (page {page_count})")
                        else:
                           print(f"No more results found in quotes response for {ticker} (page {page_count})")
                        break # Exit loop if no results or empty results
                    
                    result_count = len(data.get('results', []))
                    print(f"Processing {result_count} quote records for {ticker} (page {page_count})")
                    
                    processed_in_page = 0
                    for quote in data.get('results', []):
                        try:
                            # Convert nanosecond timestamps to UTC datetime objects
                            sip_timestamp_utc = datetime.fromtimestamp(quote.get('sip_timestamp') / 1e9, tz=timezone.utc) if quote.get('sip_timestamp') else None
                            participant_timestamp_utc = datetime.fromtimestamp(quote.get('participant_timestamp') / 1e9, tz=timezone.utc) if quote.get('participant_timestamp') else None
                            trf_timestamp_utc = datetime.fromtimestamp(quote.get('trf_timestamp') / 1e9, tz=timezone.utc) if quote.get('trf_timestamp') else None

                            # Skip records with no sip_timestamp
                            if not sip_timestamp_utc:
                                continue
                                
                            quotes_list.append({
                                "ticker": ticker,
                                "sip_timestamp": sip_timestamp_utc, # UTC datetime
                                "ask_exchange": int(quote.get('ask_exchange')) if quote.get('ask_exchange') is not None else None,
                                "ask_price": float(quote.get('ask_price')) if quote.get('ask_price') is not None else None,
                                "ask_size": float(quote.get('ask_size')) if quote.get('ask_size') is not None else None,
                                "bid_exchange": int(quote.get('bid_exchange')) if quote.get('bid_exchange') is not None else None,
                                "bid_price": float(quote.get('bid_price')) if quote.get('bid_price') is not None else None,
                                "bid_size": float(quote.get('bid_size')) if quote.get('bid_size') is not None else None,
                                "conditions": quote.get('conditions', []),
                                "indicators": quote.get('indicators', []),
                                "participant_timestamp": participant_timestamp_utc, # UTC datetime
                                "sequence_number": int(quote.get('sequence_number')) if quote.get('sequence_number') is not None else None,
                                "tape": int(quote.get('tape')) if quote.get('tape') is not None else None,
                                "trf_timestamp": trf_timestamp_utc # UTC datetime
                            })
                            processed_in_page += 1
                        except Exception as e:
                            # Skip individual records that fail processing
                            print(f"Error processing quote record for {ticker}: {type(e).__name__} - {str(e)}")
                            continue
                    
                    print(f"Successfully processed {processed_in_page}/{result_count} quote records for {ticker} (page {page_count})")
                    
                    # Check for next_url for pagination ONLY IN HISTORICAL MODE
                    next_url = data.get('next_url')
                    
                    if is_live_mode or not next_url:
                        # Break after first page in live mode, or if no next_url in historical
                        if is_live_mode:
                            print(f"Live mode: Finished fetching quotes for {ticker} (single request).")
                        elif not next_url:
                             print(f"Historical mode: No next_url found for quotes of {ticker}. Fetch complete.")
                        break
                        
                    # Prepare for next iteration (historical mode only)
                    if next_url:
                        # Make sure we're using our proxy URL instead of api.polygon.io
                        if "api.polygon.io" in next_url:
                            next_url = next_url.replace("https://api.polygon.io", config.POLYGON_API_URL)
                        
                        print(f"Historical mode: Found next_url for quotes pagination: continuing to page {page_count + 1}")
                        current_url = next_url
                        current_params = None # next_url contains all params
                        
                        # Add a small delay between pagination requests
                        await asyncio.sleep(0.2)
                        
            except aiohttp.ClientError as e:
                print(f"HTTP client error fetching quotes for {ticker} (page {page_count}): {type(e).__name__} - {str(e)}")
                break  # Exit loop on client error

    except asyncio.TimeoutError:
        print(f"Timeout fetching quotes for {ticker}")
        return quotes_list  # Return what we have so far
    except Exception as e:
        print(f"Error fetching quotes for {ticker}: {type(e).__name__} - {str(e)}")
        return quotes_list  # Return what we have so far

    return quotes_list

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