import aiohttp
import asyncio
from datetime import datetime, timedelta
import pytz
import json
import time
from endpoints import config

async def test_polygon_latest():
    """Test what is the latest data available from Polygon API for a specific ticker"""
    # Get current time in various timezones for comparison
    et_tz = pytz.timezone('US/Eastern')
    mt_tz = pytz.timezone('US/Mountain')
    utc_tz = pytz.timezone('UTC')
    
    now_et = datetime.now(et_tz)
    now_mt = datetime.now(mt_tz)
    now_utc = datetime.now(utc_tz)
    
    print("Current time in different timezones:")
    print(f"Eastern Time: {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Mountain Time: {now_mt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Create a session with the Polygon API key
    headers = {"Authorization": f"Bearer {config.POLYGON_API_KEY}"}
    async with aiohttp.ClientSession(headers=headers) as session:
        # Test different endpoints
        tickers = ["AAPL", "TSLA", "AMD"]
        
        # Test 1: Get latest minute bars
        print("\n=== Testing minute bars (aggs) ===")
        for ticker in tickers:
            # Format today's date
            today = now_et.strftime("%Y-%m-%d")
            
            # Test both direct Polygon URL and your proxy URL
            urls = [
                f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{today}/{today}?limit=10",
                f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{today}/{today}?limit=10"
            ]
            
            for i, url in enumerate(urls):
                print(f"\nTesting {ticker} with {'Polygon API' if i==0 else 'Proxy URL'}:")
                print(f"URL: {url}")
                
                try:
                    async with session.get(url, timeout=10) as response:
                        if response.status != 200:
                            print(f"Error: HTTP {response.status}")
                            print(await response.text())
                            continue
                            
                        data = await response.json()
                        
                        if data.get('status') != 'OK' or 'results' not in data:
                            print(f"Error in API response: {data.get('error', 'Unknown error')}")
                            print(f"Full response: {json.dumps(data, indent=2)}")
                            continue
                            
                        results = data.get('results', [])
                        if not results:
                            print("No results returned")
                            continue
                            
                        # Print information about results
                        print(f"Number of bars returned: {len(results)}")
                        
                        # Get first and last bar
                        first_bar = results[0]
                        last_bar = results[-1]
                        
                        # Convert timestamps to datetime
                        first_ts = datetime.fromtimestamp(first_bar['t'] / 1000, tz=utc_tz)
                        last_ts = datetime.fromtimestamp(last_bar['t'] / 1000, tz=utc_tz)
                        
                        # Print in different timezones
                        print(f"First bar timestamp:")
                        print(f"  UTC: {first_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        print(f"  Eastern: {first_ts.astimezone(et_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        print(f"  Mountain: {first_ts.astimezone(mt_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        
                        print(f"Last bar timestamp:")
                        print(f"  UTC: {last_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        print(f"  Eastern: {last_ts.astimezone(et_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        print(f"  Mountain: {last_ts.astimezone(mt_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        
                        # Calculate time difference from now
                        time_diff = now_utc - last_ts
                        minutes_diff = time_diff.total_seconds() / 60
                        
                        print(f"Time difference between latest bar and now: {minutes_diff:.1f} minutes")
                        
                except Exception as e:
                    print(f"Error testing {ticker}: {str(e)}")
        
        # Test 2: Get latest trades
        print("\n=== Testing latest trades ===")
        for ticker in tickers:
            # Try both the official Polygon URL and the proxy URL
            urls = [
                f"https://api.polygon.io/v3/trades/{ticker}?limit=1&order=desc&sort=timestamp",
                f"{config.POLYGON_API_URL}/v3/trades/{ticker}?limit=1&order=desc&sort=timestamp"
            ]
            
            for i, url in enumerate(urls):
                print(f"\nTesting {ticker} latest trade with {'Polygon API' if i==0 else 'Proxy URL'}:")
                print(f"URL: {url}")
                
                try:
                    async with session.get(url, timeout=10) as response:
                        if response.status != 200:
                            print(f"Error: HTTP {response.status}")
                            print(await response.text())
                            continue
                            
                        data = await response.json()
                        
                        if 'results' not in data:
                            print(f"Error in API response: {data.get('error', 'Unknown error')}")
                            print(f"Full response: {json.dumps(data, indent=2)}")
                            continue
                            
                        results = data.get('results', [])
                        if not results:
                            print("No results returned")
                            continue
                            
                        # Get the trade
                        trade = results[0]
                        
                        # Print trade details
                        print(f"Latest trade:")
                        print(f"  Price: {trade.get('price')}")
                        print(f"  Size: {trade.get('size')}")
                        
                        # Convert timestamp to datetime (nanoseconds to seconds)
                        trade_ts = datetime.fromtimestamp(trade.get('sip_timestamp', 0) / 1e9, tz=utc_tz)
                        
                        # Print in different timezones
                        print(f"Trade timestamp:")
                        print(f"  UTC: {trade_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        print(f"  Eastern: {trade_ts.astimezone(et_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        print(f"  Mountain: {trade_ts.astimezone(mt_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        
                        # Calculate time difference from now
                        time_diff = now_utc - trade_ts
                        minutes_diff = time_diff.total_seconds() / 60
                        
                        print(f"Time difference between latest trade and now: {minutes_diff:.1f} minutes")
                        
                except Exception as e:
                    print(f"Error testing {ticker} trades: {str(e)}")
        
        # Test 3: Get latest quotes
        print("\n=== Testing latest quotes ===")
        for ticker in tickers:
            # Try both the official Polygon URL and the proxy URL
            urls = [
                f"https://api.polygon.io/v3/quotes/{ticker}?limit=1&order=desc&sort=timestamp",
                f"{config.POLYGON_API_URL}/v3/quotes/{ticker}?limit=1&order=desc&sort=timestamp"
            ]
            
            for i, url in enumerate(urls):
                print(f"\nTesting {ticker} latest quote with {'Polygon API' if i==0 else 'Proxy URL'}:")
                print(f"URL: {url}")
                
                try:
                    async with session.get(url, timeout=10) as response:
                        if response.status != 200:
                            print(f"Error: HTTP {response.status}")
                            print(await response.text())
                            continue
                            
                        data = await response.json()
                        
                        if 'results' not in data:
                            print(f"Error in API response: {data.get('error', 'Unknown error')}")
                            print(f"Full response: {json.dumps(data, indent=2)}")
                            continue
                            
                        results = data.get('results', [])
                        if not results:
                            print("No results returned")
                            continue
                            
                        # Get the quote
                        quote = results[0]
                        
                        # Print quote details
                        print(f"Latest quote:")
                        print(f"  Ask price: {quote.get('ask_price')}")
                        print(f"  Ask size: {quote.get('ask_size')}")
                        print(f"  Bid price: {quote.get('bid_price')}")
                        print(f"  Bid size: {quote.get('bid_size')}")
                        
                        # Convert timestamp to datetime (nanoseconds to seconds)
                        quote_ts = datetime.fromtimestamp(quote.get('sip_timestamp', 0) / 1e9, tz=utc_tz)
                        
                        # Print in different timezones
                        print(f"Quote timestamp:")
                        print(f"  UTC: {quote_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        print(f"  Eastern: {quote_ts.astimezone(et_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        print(f"  Mountain: {quote_ts.astimezone(mt_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        
                        # Calculate time difference from now
                        time_diff = now_utc - quote_ts
                        seconds_diff = time_diff.total_seconds()
                        
                        print(f"Time difference between latest quote and now: {seconds_diff:.1f} seconds")
                        
                except Exception as e:
                    print(f"Error testing {ticker} quotes: {str(e)}")
        
        # Test 4: Get latest daily bars
        print("\n=== Testing daily bars ===")
        for ticker in tickers:
            # Use date range of the past 5 days
            end_date = now_et.strftime("%Y-%m-%d")
            start_date = (now_et - timedelta(days=5)).strftime("%Y-%m-%d")
            
            # Try both the official Polygon URL and the proxy URL
            urls = [
                f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?limit=5",
                f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?limit=5"
            ]
            
            for i, url in enumerate(urls):
                print(f"\nTesting {ticker} daily bars with {'Polygon API' if i==0 else 'Proxy URL'}:")
                print(f"URL: {url}")
                
                try:
                    async with session.get(url, timeout=10) as response:
                        if response.status != 200:
                            print(f"Error: HTTP {response.status}")
                            print(await response.text())
                            continue
                            
                        data = await response.json()
                        
                        if data.get('status') != 'OK' or 'results' not in data:
                            print(f"Error in API response: {data.get('error', 'Unknown error')}")
                            print(f"Full response: {json.dumps(data, indent=2)}")
                            continue
                            
                        results = data.get('results', [])
                        if not results:
                            print("No results returned")
                            continue
                            
                        # Print information about results
                        print(f"Number of daily bars returned: {len(results)}")
                        
                        # Get first and last bar
                        first_bar = results[0]
                        last_bar = results[-1]
                        
                        # Convert timestamps to datetime
                        first_ts = datetime.fromtimestamp(first_bar['t'] / 1000, tz=utc_tz)
                        last_ts = datetime.fromtimestamp(last_bar['t'] / 1000, tz=utc_tz)
                        
                        # Print in different timezones
                        print(f"First day timestamp:")
                        print(f"  UTC: {first_ts.strftime('%Y-%m-%d %Z')}")
                        print(f"  Eastern: {first_ts.astimezone(et_tz).strftime('%Y-%m-%d %Z')}")
                        
                        print(f"Last day timestamp:")
                        print(f"  UTC: {last_ts.strftime('%Y-%m-%d %Z')}")
                        print(f"  Eastern: {last_ts.astimezone(et_tz).strftime('%Y-%m-%d %Z')}")
                        
                        # Calculate time difference from now
                        time_diff = now_utc - last_ts
                        hours_diff = time_diff.total_seconds() / 3600
                        
                        print(f"Time difference between latest daily bar and now: {hours_diff:.1f} hours")
                        
                except Exception as e:
                    print(f"Error testing {ticker} daily bars: {str(e)}")
        
        # Test 5: Get technical indicators (SMA)
        print("\n=== Testing technical indicators (SMA) ===")
        for ticker in tickers:
            # Use date range of the past 50 days for 20-day SMA
            end_date = now_et.strftime("%Y-%m-%d")
            start_date = (now_et - timedelta(days=50)).strftime("%Y-%m-%d")
            
            # Try both the official Polygon URL and the proxy URL
            urls = [
                f"https://api.polygon.io/v1/indicators/sma/{ticker}?timespan=day&timestamp.gte={start_date}&timestamp.lte={end_date}&window=20&order=desc&limit=5",
                f"{config.POLYGON_API_URL}/v1/indicators/sma/{ticker}?timespan=day&timestamp.gte={start_date}&timestamp.lte={end_date}&window=20&order=desc&limit=5"
            ]
            
            for i, url in enumerate(urls):
                print(f"\nTesting {ticker} SMA indicator with {'Polygon API' if i==0 else 'Proxy URL'}:")
                print(f"URL: {url}")
                
                try:
                    async with session.get(url, timeout=10) as response:
                        if response.status != 200:
                            print(f"Error: HTTP {response.status}")
                            print(await response.text())
                            continue
                            
                        data = await response.json()
                        
                        if 'results' not in data or 'values' not in data.get('results', {}):
                            print(f"Error in API response: {data.get('error', 'Unknown error')}")
                            print(f"Full response: {json.dumps(data, indent=2)}")
                            continue
                            
                        values = data.get('results', {}).get('values', [])
                        if not values:
                            print("No results returned")
                            continue
                            
                        # Print information about results
                        print(f"Number of SMA values returned: {len(values)}")
                        
                        # Get most recent SMA value
                        recent_sma = values[0]
                        
                        # Convert timestamp to datetime
                        sma_ts = datetime.fromtimestamp(recent_sma.get('timestamp') / 1000, tz=utc_tz)
                        
                        # Print in different timezones
                        print(f"Most recent SMA timestamp:")
                        print(f"  UTC: {sma_ts.strftime('%Y-%m-%d %Z')}")
                        print(f"  Eastern: {sma_ts.astimezone(et_tz).strftime('%Y-%m-%d %Z')}")
                        
                        print(f"SMA value: {recent_sma.get('value')}")
                        
                        # Calculate time difference from now
                        time_diff = now_utc - sma_ts
                        hours_diff = time_diff.total_seconds() / 3600
                        
                        print(f"Time difference between latest SMA calculation and now: {hours_diff:.1f} hours")
                        
                except Exception as e:
                    print(f"Error testing {ticker} SMA indicator: {str(e)}")
        
        # Test 6: Get snapshot to see last updated timestamp
        print("\n=== Testing snapshot endpoint ===")
        for ticker in tickers:
            # Try both the official Polygon URL and the proxy URL
            urls = [
                f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}",
                f"{config.POLYGON_API_URL}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
            ]
            
            for i, url in enumerate(urls):
                print(f"\nTesting {ticker} snapshot with {'Polygon API' if i==0 else 'Proxy URL'}:")
                print(f"URL: {url}")
                
                try:
                    async with session.get(url, timeout=10) as response:
                        if response.status != 200:
                            print(f"Error: HTTP {response.status}")
                            print(await response.text())
                            continue
                            
                        data = await response.json()
                        
                        if 'ticker' not in data:
                            print(f"Error in API response: {data.get('error', 'Unknown error')}")
                            print(f"Full response: {json.dumps(data, indent=2)}")
                            continue
                            
                        # Get the ticker data
                        ticker_data = data.get('ticker', {})
                        
                        # Get the updated timestamp - just print the raw value
                        updated_ts = ticker_data.get('updated')
                        if updated_ts:
                            print(f"Raw updated timestamp value: {updated_ts}")
                            # Don't try to parse this timestamp, it's in nanoseconds and too large for fromtimestamp
                            print("NOTE: The timestamp format in snapshot is different - appears to be nanoseconds")
                        else:
                            print("No updated timestamp found in response")
                        
                        # Print latest day bar
                        day = ticker_data.get('day', {})
                        if day:
                            print(f"Latest day bar:")
                            print(f"  Open: {day.get('o')}")
                            print(f"  High: {day.get('h')}")
                            print(f"  Low: {day.get('l')}")
                            print(f"  Close: {day.get('c')}")
                            print(f"  Volume: {day.get('v')}")
                            
                            # Try to get the timestamp from the day bar if available
                            day_t = day.get('t')
                            if day_t:
                                try:
                                    day_ts = datetime.fromtimestamp(day_t / 1000, tz=utc_tz)
                                    print(f"  Day timestamp: {day_ts.strftime('%Y-%m-%d %Z')}")
                                    print(f"  Eastern: {day_ts.astimezone(et_tz).strftime('%Y-%m-%d %Z')}")
                                except Exception as e:
                                    print(f"  Error parsing day timestamp: {e}")
                        
                        # Print latest minute bar
                        minute = ticker_data.get('min', {})
                        if minute:
                            print(f"Latest minute bar:")
                            print(f"  Open: {minute.get('o')}")
                            print(f"  High: {minute.get('h')}")
                            print(f"  Low: {minute.get('l')}")
                            print(f"  Close: {minute.get('c')}")
                            print(f"  Volume: {minute.get('v')}")
                            
                            # Try to get the timestamp from the minute bar if available
                            min_t = minute.get('t')
                            if min_t:
                                try:
                                    min_ts = datetime.fromtimestamp(min_t / 1000, tz=utc_tz)
                                    print(f"  Minute timestamp: {min_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                                    print(f"  Eastern: {min_ts.astimezone(et_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                                    print(f"  Mountain: {min_ts.astimezone(mt_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                                    
                                    # Calculate time difference from now for minute bar
                                    time_diff = now_utc - min_ts
                                    minutes_diff = time_diff.total_seconds() / 60
                                    print(f"  Time difference between latest minute bar and now: {minutes_diff:.1f} minutes")
                                except Exception as e:
                                    print(f"  Error parsing minute timestamp: {e}")
                        
                except Exception as e:
                    print(f"Error testing {ticker} snapshot: {str(e)}")

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(test_polygon_latest())
    print(f"\nScript completed in {time.time() - start_time:.2f} seconds") 