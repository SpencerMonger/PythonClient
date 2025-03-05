from polygon import RESTClient
from endpoints import config

def get_rest_client() -> RESTClient:
    """
    Create a REST client with custom API URL
    """
    client = RESTClient(api_key=config.POLYGON_API_KEY)
    # Override the base URL to use our proxy
    client.base_url = config.POLYGON_API_URL
    return client 