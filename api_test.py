import asyncio
from datetime import datetime, timedelta
import pytz
import json
from endpoints.polygon_client import get_aiohttp_session, close_session
from endpoints import config
import aiohttp

async def test_api():
    """Test if the Polygon API is responding correctly"""
    # Format dates - use current day
    et_tz = pytz.timezone('US/Eastern')
    now = datetime.now(et_tz)
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    
    ticker = "AAPL"  # Test with a popular ticker
    
    # Create a custom session instead of using the module's cached session
    session = aiohttp.ClientSession(
        headers={"Authorization": f"Bearer {config.POLYGON_API_KEY}"},
        timeout=aiohttp.ClientTimeout(total=10)
    )
    
    try:
        # Test bars API
        bars_url = f"{config.POLYGON_API_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{yesterday}/{today}?limit=10"
        print(f"\nTesting bars API: {bars_url}")
        
        try:
            async with session.get(bars_url) as response:
                status = response.status
                print(f"Status code: {status}")
                if status == 200:
                    data = await response.json()
                    num_results = len(data.get('results', []))
                    print(f"Received {num_results} bars")
                    if num_results > 0:
                        print(f"Response preview: {json.dumps(data)[:500]}...")
                    else:
                        print("No results returned from API")
                else:
                    body = await response.text()
                    print(f"Error response: {body}")
        except Exception as e:
            print(f"Error in bars API request: {str(e)}")
        
        # Test quotes API with timestamp parameters
        from_ns = int((now - timedelta(minutes=10)).timestamp() * 1_000_000_000)
        to_ns = int(now.timestamp() * 1_000_000_000)
        
        quotes_url = f"{config.POLYGON_API_URL}/v3/quotes/{ticker}"
        quotes_params = {
            "timestamp.gte": from_ns,
            "timestamp.lt": to_ns,
            "limit": 10
        }
        print(f"\nTesting quotes API: {quotes_url}")
        print(f"With params: {quotes_params}")
        
        try:
            async with session.get(quotes_url, params=quotes_params) as response:
                status = response.status
                print(f"Status code: {status}")
                if status == 200:
                    data = await response.json()
                    num_results = len(data.get('results', []))
                    print(f"Received {num_results} quotes")
                    if num_results > 0:
                        print(f"Response preview: {json.dumps(data)[:500]}...")
                    else:
                        print("No results returned from API")
                else:
                    body = await response.text()
                    print(f"Error response: {body}")
        except Exception as e:
            print(f"Error in quotes API request: {str(e)}")

    finally:
        # Close our custom session
        await session.close()

if __name__ == "__main__":
    print(f"Testing Polygon API connection to: {config.POLYGON_API_URL}")
    print(f"Using API key: {config.POLYGON_API_KEY[:5]}..." if config.POLYGON_API_KEY else "ERROR: No API key found")
    if not config.POLYGON_API_URL:
        print("ERROR: No API URL configured")
    else:
        asyncio.run(test_api()) 