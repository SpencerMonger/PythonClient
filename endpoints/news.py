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

async def fetch_news(ticker: str) -> List[Dict]:
    """
    Fetch news for a ticker
    """
    client = get_rest_client()
    news_items = []
    
    try:
        for news in client.list_ticker_news(
            ticker=ticker,
            order="desc",
            limit=1000
        ):
            news_items.append({
                "id": news.id,
                "publisher": news.publisher.name,
                "title": news.title,
                "author": news.author,
                "published_utc": news.published_utc,
                "article_url": news.article_url,
                "tickers": news.tickers,
                "amp_url": news.amp_url,
                "image_url": news.image_url,
                "description": news.description,
                "keywords": news.keywords
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