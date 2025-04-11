from polygon import RESTClient
from endpoints import config
import aiohttp
import asyncio
from functools import lru_cache

@lru_cache(maxsize=1)
def get_rest_client() -> RESTClient:
    """
    Create a REST client with custom API URL (cached for reuse)
    """
    client = RESTClient(api_key=config.POLYGON_API_KEY)
    # Override the base URL to use our proxy
    client.base_url = config.POLYGON_API_URL
    return client

# Session pool for concurrent requests
_session_pool = {}

async def get_aiohttp_session(ticker: str = None):
    """
    Get or create an aiohttp session for making concurrent requests.
    If ticker is provided, use a dedicated session per ticker for better throughput.
    """
    global _session_pool
    session_key = ticker or 'default'
    
    if session_key not in _session_pool or _session_pool[session_key].closed:
        _session_pool[session_key] = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {config.POLYGON_API_KEY}"},
            timeout=aiohttp.ClientTimeout(total=10)  # 10 seconds timeout for faster failures
        )
    return _session_pool[session_key]

async def close_session():
    """Close all sessions in the session pool"""
    global _session_pool
    close_tasks = []
    
    for key, session in list(_session_pool.items()):
        if not session.closed:
            close_tasks.append(session.close())
            
    if close_tasks:
        await asyncio.gather(*close_tasks, return_exceptions=True)
    
    _session_pool = {} 