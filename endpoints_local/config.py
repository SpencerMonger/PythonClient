import os
from dotenv import load_dotenv
from pathlib import Path

# Find the .env file
def find_dotenv():
    # Try current directory first
    if os.path.exists('.env'):
        return '.env'
    
    # Try the directory containing this file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env_in_current = os.path.join(current_dir, '.env')
    if os.path.exists(env_in_current):
        return env_in_current
        
    # Try parent directory
    parent_dir = os.path.dirname(current_dir)
    env_in_parent = os.path.join(parent_dir, '.env')
    if os.path.exists(env_in_parent):
        return env_in_parent
    
    raise FileNotFoundError("Could not find .env file")

# Load environment variables
env_path = find_dotenv()
print(f"Loading environment variables from: {env_path}")
load_dotenv(env_path)

# Polygon.io settings
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_API_URL = os.getenv("POLYGON_API_URL", "http://3.128.134.41")

# ClickHouse settings
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"

# Table names
TABLE_STOCK_BARS = "stock_bars"
TABLE_STOCK_TRADES = "stock_trades"
TABLE_STOCK_QUOTES = "stock_quotes"
TABLE_STOCK_NEWS = "stock_news"
TABLE_STOCK_INDICATORS = "stock_indicators"
TABLE_STOCK_MASTER = "stock_master"  # New master table that combines all tables
TABLE_STOCK_DAILY = "stock_daily"
TABLE_STOCK_NORMALIZED = "stock_normalized"  # Table containing normalized data
TABLE_STOCK_PREDICTIONS = "stock_predictions"  # Table for model predictions

# Technical indicator settings
SMA_WINDOWS = [5, 9, 12, 20, 50, 100, 200]
EMA_WINDOWS = [9, 12, 20]
TIMESPAN = "minute"  # For bar aggregates
SERIES_TYPE = "close"  # For technical indicators 