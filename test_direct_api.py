import requests
import datetime
import time
from endpoints import config

def test_minute_bars(ticker="AAPL", headers=None):
    """Test the minute bars endpoint"""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{today}/{today}?limit=10"
    
    print(f"\n===== TESTING MINUTE BARS =====")
    print(f"Testing direct Polygon API at {url}")
    print(f"Current time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        response = requests.get(url, headers=headers)
        
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if 'results' in data and data.get('results'):
                results = data['results']
                print(f"Received {len(results)} data points")
                
                # Get first and last bar
                first_bar = results[0]
                last_bar = results[-1]
                
                # Convert timestamps
                first_time = datetime.datetime.fromtimestamp(first_bar['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                last_time = datetime.datetime.fromtimestamp(last_bar['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"First timestamp: {first_time}")
                print(f"Last timestamp: {last_time}")
                
                # Calculate time difference from now
                now = time.time()
                diff_minutes = (now - (last_bar['t'] / 1000)) / 60
                
                print(f"Last data point is from {diff_minutes:.1f} minutes ago")
                return data
            else:
                print("No results in response")
                print(f"Response: {data}")
        else:
            print(f"Error response: {response.text}")
        
        return None
            
    except Exception as e:
        print(f"Error making request: {str(e)}")
        return None

def test_trades(ticker="AAPL", headers=None):
    """Test the trades endpoint"""
    trade_url = f"https://api.polygon.io/v3/trades/{ticker}?limit=1"
    
    print(f"\n===== TESTING TRADES =====")
    print(f"Testing trades endpoint: {trade_url}")
    
    try:
        trade_response = requests.get(trade_url, headers=headers)
        
        print(f"Status code: {trade_response.status_code}")
        
        if trade_response.status_code == 200:
            trade_data = trade_response.json()
            
            if 'results' in trade_data and trade_data['results']:
                trade = trade_data['results'][0]
                # Convert timestamp from nanoseconds to seconds
                trade_time = datetime.datetime.fromtimestamp(trade['sip_timestamp'] / 1e9).strftime('%Y-%m-%d %H:%M:%S')
                print(f"Latest trade timestamp: {trade_time}")
                
                # Calculate time difference from now
                now = time.time()
                diff_seconds = now - (trade['sip_timestamp'] / 1e9)
                
                print(f"Latest trade is from {diff_seconds:.1f} seconds ago")
                return trade_data
            else:
                print("No trade results in response")
        else:
            print(f"Error response: {trade_response.text}")
        
        return None
                
    except Exception as e:
        print(f"Error making request: {str(e)}")
        return None

def test_quotes(ticker="AAPL", headers=None):
    """Test the quotes endpoint"""
    quotes_url = f"https://api.polygon.io/v3/quotes/{ticker}?limit=1"
    
    print(f"\n===== TESTING QUOTES =====")
    print(f"Testing quotes endpoint: {quotes_url}")
    
    try:
        quotes_response = requests.get(quotes_url, headers=headers)
        
        print(f"Status code: {quotes_response.status_code}")
        
        if quotes_response.status_code == 200:
            quotes_data = quotes_response.json()
            
            if 'results' in quotes_data and quotes_data['results']:
                quote = quotes_data['results'][0]
                # Convert timestamp from nanoseconds to seconds
                quote_time = datetime.datetime.fromtimestamp(quote['sip_timestamp'] / 1e9).strftime('%Y-%m-%d %H:%M:%S')
                print(f"Latest quote timestamp: {quote_time}")
                
                # Calculate time difference from now
                now = time.time()
                diff_seconds = now - (quote['sip_timestamp'] / 1e9)
                
                print(f"Latest quote is from {diff_seconds:.1f} seconds ago")
                return quotes_data
            else:
                print("No quote results in response")
        else:
            print(f"Error response: {quotes_response.text}")
        
        return None
                
    except Exception as e:
        print(f"Error making request: {str(e)}")
        return None

def test_daily_bars(ticker="AAPL", headers=None):
    """Test the daily bars endpoint"""
    # Use a date range of the past 5 days
    end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    
    daily_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?limit=5"
    
    print(f"\n===== TESTING DAILY BARS =====")
    print(f"Testing daily bars endpoint: {daily_url}")
    
    try:
        daily_response = requests.get(daily_url, headers=headers)
        
        print(f"Status code: {daily_response.status_code}")
        
        if daily_response.status_code == 200:
            daily_data = daily_response.json()
            
            if 'results' in daily_data and daily_data['results']:
                results = daily_data['results']
                print(f"Received {len(results)} daily bars")
                
                # Get first and last bar
                first_bar = results[0]
                last_bar = results[-1]
                
                # Convert timestamps
                first_time = datetime.datetime.fromtimestamp(first_bar['t'] / 1000).strftime('%Y-%m-%d')
                last_time = datetime.datetime.fromtimestamp(last_bar['t'] / 1000).strftime('%Y-%m-%d')
                
                print(f"First day: {first_time}")
                print(f"Last day: {last_time}")
                
                # Calculate time difference from now
                now = time.time()
                diff_hours = (now - (last_bar['t'] / 1000)) / 3600
                
                print(f"Last daily bar is from {diff_hours:.1f} hours ago")
                return daily_data
            else:
                print("No daily bar results in response")
                print(f"Response: {daily_data}")
        else:
            print(f"Error response: {daily_response.text}")
        
        return None
                
    except Exception as e:
        print(f"Error making request: {str(e)}")
        return None

def test_indicators(ticker="AAPL", headers=None):
    """Test the technical indicators endpoint"""
    # Test SMA indicator with 20-day window
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.datetime.now() - datetime.timedelta(days=50)).strftime("%Y-%m-%d")
    
    indicator_url = f"https://api.polygon.io/v1/indicators/sma/{ticker}?timespan=day&timestamp.gte={start_date}&timestamp.lte={today}&window=20&order=desc&limit=5"
    
    print(f"\n===== TESTING TECHNICAL INDICATORS (SMA) =====")
    print(f"Testing SMA indicator endpoint: {indicator_url}")
    
    try:
        indicator_response = requests.get(indicator_url, headers=headers)
        
        print(f"Status code: {indicator_response.status_code}")
        
        if indicator_response.status_code == 200:
            indicator_data = indicator_response.json()
            
            if 'results' in indicator_data and indicator_data['results']['values']:
                results = indicator_data['results']['values']
                print(f"Received {len(results)} SMA values")
                
                # Get first (most recent) SMA value
                first_sma = results[0]
                
                # Convert timestamp
                timestamp = datetime.datetime.fromtimestamp(first_sma['timestamp'] / 1000).strftime('%Y-%m-%d')
                
                print(f"Most recent SMA date: {timestamp}")
                print(f"SMA value: {first_sma['value']}")
                
                # Calculate time difference from now
                now = time.time()
                diff_hours = (now - (first_sma['timestamp'] / 1000)) / 3600
                
                print(f"Last SMA calculation is from {diff_hours:.1f} hours ago")
                return indicator_data
            else:
                print("No indicator results in response")
                print(f"Response: {indicator_data}")
        else:
            print(f"Error response: {indicator_response.text}")
        
        return None
                
    except Exception as e:
        print(f"Error making request: {str(e)}")
        return None

def test_proxy_bars(ticker="AAPL", headers=None, bars_data=None):
    """Test the proxy URL for bars data"""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    proxy_url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{today}/{today}?limit=10"
    
    print(f"\n===== TESTING PROXY URL =====")
    print(f"Testing proxy URL at {proxy_url}")
    
    try:
        proxy_response = requests.get(proxy_url, headers=headers)
        
        print(f"Status code: {proxy_response.status_code}")
        
        if proxy_response.status_code == 200:
            proxy_data = proxy_response.json()
            
            if 'results' in proxy_data and proxy_data.get('results'):
                results = proxy_data['results']
                print(f"Received {len(results)} data points from proxy")
                
                # Get first and last bar
                first_bar = results[0]
                last_bar = results[-1]
                
                # Convert timestamps
                first_time = datetime.datetime.fromtimestamp(first_bar['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                last_time = datetime.datetime.fromtimestamp(last_bar['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"First timestamp from proxy: {first_time}")
                print(f"Last timestamp from proxy: {last_time}")
                
                # Calculate time difference from now
                now = time.time()
                diff_minutes = (now - (last_bar['t'] / 1000)) / 60
                
                print(f"Last data point from proxy is from {diff_minutes:.1f} minutes ago")
                
                # Check if direct API and proxy results match
                if bars_data and proxy_data.get('status') == 'OK' and bars_data.get('status') == 'OK':
                    direct_last_time = bars_data['results'][-1]['t']
                    proxy_last_time = proxy_data['results'][-1]['t']
                    
                    if direct_last_time == proxy_last_time:
                        print("MATCH: Direct API and proxy return the same latest timestamp")
                    else:
                        print("MISMATCH: Direct API and proxy return different latest timestamps")
                        print(f"Direct API: {datetime.datetime.fromtimestamp(direct_last_time / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"Proxy: {datetime.datetime.fromtimestamp(proxy_last_time / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
                return proxy_data
            else:
                print("No results in proxy response")
                print(f"Response: {proxy_data}")
        else:
            print(f"Error response from proxy: {proxy_response.text}")
        
        return None
            
    except Exception as e:
        print(f"Error making request: {str(e)}")
        return None

def test_direct_polygon():
    """Test the Polygon API directly to check for data availability across endpoints"""
    # Get today's date
    ticker = "AAPL"
    
    # Use your API key
    headers = {"Authorization": f"Bearer {config.POLYGON_API_KEY}"}
    
    print(f"Starting Polygon API tests")
    print(f"Current time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API Key: {config.POLYGON_API_KEY[:5]}...")
    
    # Test each endpoint
    bars_data = test_minute_bars(ticker, headers)
    test_trades(ticker, headers)
    test_quotes(ticker, headers)
    test_daily_bars(ticker, headers)
    test_indicators(ticker, headers)
    test_proxy_bars(ticker, headers, bars_data)
    
    print("\n===== TESTS COMPLETE =====")

if __name__ == "__main__":
    test_direct_polygon() 