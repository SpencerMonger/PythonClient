import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Polygon.io settings
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_API_URL = os.getenv("POLYGON_API_URL", "http://3.128.134.41")

# ClickHouse settings
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8443"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "true").lower() == "true"

# Table names
TABLE_STOCK_BARS = "stock_bars"
TABLE_STOCK_TRADES = "stock_trades"
TABLE_STOCK_QUOTES = "stock_quotes"
TABLE_STOCK_NEWS = "stock_news"
TABLE_STOCK_INDICATORS = "stock_indicators"
TABLE_STOCK_MASTER = "stock_master"  # New master table that combines all tables

# Technical indicator settings
SMA_WINDOWS = [5, 9, 12, 20, 50, 100, 200]
EMA_WINDOWS = [9, 12, 20]
TIMESPAN = "minute"  # For bar aggregates
SERIES_TYPE = "close"  # For technical indicators 