import asyncio
from datetime import datetime
from typing import Dict, List

from endpoints.polygon_client import get_rest_client
from endpoints.db import ClickHouseDB
from endpoints import config

# Schema for stock_news table
NEWS_SCHEMA = {
    "id": "String",
    "publisher": "String",
    "title": "String",
    "author": "String",
    "published_utc": "DateTime64(9)",  # Nanosecond precision
    "article_url": "String",
    "tickers": "Array(String)",
    "amp_url": "String",
    "image_url": "String",
    "description": "String",
    "keywords": "Array(String)"
}

def parse_utc_timestamp(timestamp_str: str) -> datetime:
    """
    Parse UTC timestamp from Polygon format to datetime
    """
    try:
        # Remove the 'Z' and parse
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1]
        return datetime.fromisoformat(timestamp_str)
    except (ValueError, AttributeError):
        return datetime.utcnow()

async def fetch_news(ticker: str, from_date: datetime, to_date: datetime) -> List[Dict]:
    """
    Fetch news for a ticker between dates
    """
    client = get_rest_client()
    news_items = []
    
    # Format dates as YYYY-MM-DD
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")
    
    try:
        for news in client.list_ticker_news(
            ticker=ticker,
            published_utc_gte=from_str,
            published_utc_lt=to_str,
            order="desc",
            limit=1000
        ):
            # Handle potential None values and ensure proper types
            published_utc = parse_utc_timestamp(news.published_utc) if news.published_utc else datetime.utcnow()
            
            news_items.append({
                "id": str(news.id) if news.id else "",
                "publisher": news.publisher.name if news.publisher and hasattr(news.publisher, 'name') else "",
                "title": str(news.title) if news.title else "",
                "author": str(news.author) if news.author else "",
                "published_utc": published_utc,
                "article_url": str(news.article_url) if news.article_url else "",
                "tickers": news.tickers if news.tickers else [],
                "amp_url": str(news.amp_url) if news.amp_url else "",
                "image_url": str(news.image_url) if news.image_url else "",
                "description": str(news.description) if news.description else "",
                "keywords": news.keywords if news.keywords else []
            })
    except Exception as e:
        print(f"Error fetching news for {ticker}: {str(e)}")
        return []
        
    return news_items

async def store_news(db: ClickHouseDB, news_items: List[Dict]) -> None:
    """
    Store news data in ClickHouse
    """
    try:
        await db.insert_data(config.TABLE_STOCK_NEWS, news_items)
    except Exception as e:
        print(f"Error storing news: {str(e)}")

async def init_news_table(db: ClickHouseDB) -> None:
    """
    Initialize the stock news table
    """
    db.create_table_if_not_exists(config.TABLE_STOCK_NEWS, NEWS_SCHEMA) 