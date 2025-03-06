import asyncio
from datetime import datetime
from typing import Dict, List

from endpoints.db import ClickHouseDB
from endpoints import config

# Schema for master stock data table
MASTER_SCHEMA = {
    # Common fields
    "ticker": "String",
    "timestamp": "DateTime64(9)",  # Base timestamp at minute intervals
    
    # Fields from stock_bars
    "open": "Nullable(Float64)",
    "high": "Nullable(Float64)",
    "low": "Nullable(Float64)",
    "close": "Nullable(Float64)",
    "volume": "Nullable(Int64)",
    "vwap": "Nullable(Float64)",
    "transactions": "Nullable(Int64)",
    
    # Aggregated fields from stock_quotes (per minute)
    "avg_bid_price": "Nullable(Float64)",
    "avg_ask_price": "Nullable(Float64)",
    "min_bid_price": "Nullable(Float64)",
    "max_ask_price": "Nullable(Float64)",
    "total_bid_size": "Nullable(Float64)",
    "total_ask_size": "Nullable(Float64)",
    "quote_count": "Nullable(Int32)",
    
    # Aggregated fields from stock_trades (per minute)
    "avg_trade_price": "Nullable(Float64)",
    "min_trade_price": "Nullable(Float64)",
    "max_trade_price": "Nullable(Float64)",
    "total_trade_size": "Nullable(Int32)",
    "trade_count": "Nullable(Int32)",
    
    # Fields from stock_indicators
    "sma_5": "Nullable(Float64)",
    "sma_9": "Nullable(Float64)",
    "sma_12": "Nullable(Float64)",
    "sma_20": "Nullable(Float64)",
    "sma_50": "Nullable(Float64)",
    "sma_100": "Nullable(Float64)",
    "sma_200": "Nullable(Float64)",
    "ema_9": "Nullable(Float64)",
    "ema_12": "Nullable(Float64)",
    "ema_20": "Nullable(Float64)",
    "macd_value": "Nullable(Float64)",
    "macd_signal": "Nullable(Float64)",
    "macd_histogram": "Nullable(Float64)",
    "rsi_14": "Nullable(Float64)",
    
    # Fields from stock_news (latest news in the minute)
    "latest_news_id": "Nullable(String)",
    "latest_news_title": "Nullable(String)",
    "latest_news_url": "Nullable(String)"
}

async def populate_master_table(db: ClickHouseDB) -> None:
    """
    Populate the master table with existing data from source tables
    """
    try:
        print("Populating master table with existing data...")
        
        # Insert data using the same query structure as the materialized view
        query = f"""
        INSERT INTO {db.database}.{config.TABLE_STOCK_MASTER}
        WITH 
        -- Convert quotes timestamps to minute intervals
        minute_quotes AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) AS timestamp,
                avg(bid_price) AS avg_bid_price,
                avg(ask_price) AS avg_ask_price,
                min(bid_price) AS min_bid_price,
                max(ask_price) AS max_ask_price,
                sum(bid_size) AS total_bid_size,
                sum(ask_size) AS total_ask_size,
                count(*) AS quote_count
            FROM {db.database}.stock_quotes
            GROUP BY ticker, timestamp
        ),
        
        -- Convert trades timestamps to minute intervals
        minute_trades AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) AS timestamp,
                avg(price) AS avg_trade_price,
                min(price) AS min_trade_price,
                max(price) AS max_trade_price,
                sum(size) AS total_trade_size,
                count(*) AS trade_count
            FROM {db.database}.stock_trades
            GROUP BY ticker, timestamp
        ),
        
        -- Pivot indicators to columns
        pivoted_indicators AS (
            SELECT
                ticker,
                timestamp,
                argMax(IF(indicator_type = 'SMA_5', value, NULL), timestamp) AS sma_5,
                argMax(IF(indicator_type = 'SMA_9', value, NULL), timestamp) AS sma_9,
                argMax(IF(indicator_type = 'SMA_12', value, NULL), timestamp) AS sma_12,
                argMax(IF(indicator_type = 'SMA_20', value, NULL), timestamp) AS sma_20,
                argMax(IF(indicator_type = 'SMA_50', value, NULL), timestamp) AS sma_50,
                argMax(IF(indicator_type = 'SMA_100', value, NULL), timestamp) AS sma_100,
                argMax(IF(indicator_type = 'SMA_200', value, NULL), timestamp) AS sma_200,
                argMax(IF(indicator_type = 'EMA_9', value, NULL), timestamp) AS ema_9,
                argMax(IF(indicator_type = 'EMA_12', value, NULL), timestamp) AS ema_12,
                argMax(IF(indicator_type = 'EMA_20', value, NULL), timestamp) AS ema_20,
                argMax(IF(indicator_type = 'MACD', value, NULL), timestamp) AS macd_value,
                argMax(IF(indicator_type = 'MACD', signal, NULL), timestamp) AS macd_signal,
                argMax(IF(indicator_type = 'MACD', histogram, NULL), timestamp) AS macd_histogram,
                argMax(IF(indicator_type = 'RSI', value, NULL), timestamp) AS rsi_14
            FROM {db.database}.stock_indicators
            GROUP BY ticker, timestamp
        ),
        
        -- Get latest news per minute
        latest_news AS (
            SELECT
                ticker,
                toStartOfMinute(published_utc) AS timestamp,
                argMax(id, published_utc) AS latest_news_id,
                argMax(title, published_utc) AS latest_news_title,
                argMax(article_url, published_utc) AS latest_news_url
            FROM {db.database}.stock_news
            ARRAY JOIN tickers AS ticker
            GROUP BY ticker, timestamp
        )
        
        SELECT
            b.ticker,
            b.timestamp,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume,
            b.vwap,
            b.transactions,
            q.avg_bid_price,
            q.avg_ask_price,
            q.min_bid_price,
            q.max_ask_price,
            q.total_bid_size,
            q.total_ask_size,
            q.quote_count,
            t.avg_trade_price,
            t.min_trade_price,
            t.max_trade_price,
            t.total_trade_size,
            t.trade_count,
            i.sma_5,
            i.sma_9,
            i.sma_12,
            i.sma_20,
            i.sma_50,
            i.sma_100,
            i.sma_200,
            i.ema_9,
            i.ema_12,
            i.ema_20,
            i.macd_value,
            i.macd_signal,
            i.macd_histogram,
            i.rsi_14,
            n.latest_news_id,
            n.latest_news_title,
            n.latest_news_url
        FROM {db.database}.stock_bars b
        LEFT JOIN minute_quotes q ON b.ticker = q.ticker AND b.timestamp = q.timestamp
        LEFT JOIN minute_trades t ON b.ticker = t.ticker AND b.timestamp = t.timestamp
        LEFT JOIN pivoted_indicators i ON b.ticker = i.ticker AND b.timestamp = i.timestamp
        LEFT JOIN latest_news n ON b.ticker = n.ticker AND b.timestamp = n.timestamp
        """
        
        db.client.command(query)
        print("Master table populated successfully")
        
    except Exception as e:
        print(f"Error populating master table: {str(e)}")
        raise e

async def create_master_table(db: ClickHouseDB) -> None:
    """
    Create the master stock data table that combines all other tables
    """
    try:
        # Create the master table
        db.create_table_if_not_exists(config.TABLE_STOCK_MASTER, MASTER_SCHEMA)
        
        # Create the materialized view that will populate the master table
        view_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {db.database}.stock_master_mv 
        TO {db.database}.stock_master
        AS
        WITH 
        -- Convert quotes timestamps to minute intervals
        minute_quotes AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) AS timestamp,
                avg(bid_price) AS avg_bid_price,
                avg(ask_price) AS avg_ask_price,
                min(bid_price) AS min_bid_price,
                max(ask_price) AS max_ask_price,
                sum(bid_size) AS total_bid_size,
                sum(ask_size) AS total_ask_size,
                count(*) AS quote_count
            FROM {db.database}.stock_quotes
            GROUP BY ticker, timestamp
        ),
        
        -- Convert trades timestamps to minute intervals
        minute_trades AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) AS timestamp,
                avg(price) AS avg_trade_price,
                min(price) AS min_trade_price,
                max(price) AS max_trade_price,
                sum(size) AS total_trade_size,
                count(*) AS trade_count
            FROM {db.database}.stock_trades
            GROUP BY ticker, timestamp
        ),
        
        -- Pivot indicators to columns
        pivoted_indicators AS (
            SELECT
                ticker,
                timestamp,
                argMax(IF(indicator_type = 'SMA_5', value, NULL), timestamp) AS sma_5,
                argMax(IF(indicator_type = 'SMA_9', value, NULL), timestamp) AS sma_9,
                argMax(IF(indicator_type = 'SMA_12', value, NULL), timestamp) AS sma_12,
                argMax(IF(indicator_type = 'SMA_20', value, NULL), timestamp) AS sma_20,
                argMax(IF(indicator_type = 'SMA_50', value, NULL), timestamp) AS sma_50,
                argMax(IF(indicator_type = 'SMA_100', value, NULL), timestamp) AS sma_100,
                argMax(IF(indicator_type = 'SMA_200', value, NULL), timestamp) AS sma_200,
                argMax(IF(indicator_type = 'EMA_9', value, NULL), timestamp) AS ema_9,
                argMax(IF(indicator_type = 'EMA_12', value, NULL), timestamp) AS ema_12,
                argMax(IF(indicator_type = 'EMA_20', value, NULL), timestamp) AS ema_20,
                argMax(IF(indicator_type = 'MACD', value, NULL), timestamp) AS macd_value,
                argMax(IF(indicator_type = 'MACD', signal, NULL), timestamp) AS macd_signal,
                argMax(IF(indicator_type = 'MACD', histogram, NULL), timestamp) AS macd_histogram,
                argMax(IF(indicator_type = 'RSI', value, NULL), timestamp) AS rsi_14
            FROM {db.database}.stock_indicators
            GROUP BY ticker, timestamp
        ),
        
        -- Get latest news per minute
        latest_news AS (
            SELECT
                ticker,
                toStartOfMinute(published_utc) AS timestamp,
                argMax(id, published_utc) AS latest_news_id,
                argMax(title, published_utc) AS latest_news_title,
                argMax(article_url, published_utc) AS latest_news_url
            FROM {db.database}.stock_news
            ARRAY JOIN tickers AS ticker
            GROUP BY ticker, timestamp
        )
        
        SELECT
            b.ticker,
            b.timestamp,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume,
            b.vwap,
            b.transactions,
            q.avg_bid_price,
            q.avg_ask_price,
            q.min_bid_price,
            q.max_ask_price,
            q.total_bid_size,
            q.total_ask_size,
            q.quote_count,
            t.avg_trade_price,
            t.min_trade_price,
            t.max_trade_price,
            t.total_trade_size,
            t.trade_count,
            i.sma_5,
            i.sma_9,
            i.sma_12,
            i.sma_20,
            i.sma_50,
            i.sma_100,
            i.sma_200,
            i.ema_9,
            i.ema_12,
            i.ema_20,
            i.macd_value,
            i.macd_signal,
            i.macd_histogram,
            i.rsi_14,
            n.latest_news_id,
            n.latest_news_title,
            n.latest_news_url
        FROM {db.database}.stock_bars b
        LEFT JOIN minute_quotes q ON b.ticker = q.ticker AND b.timestamp = q.timestamp
        LEFT JOIN minute_trades t ON b.ticker = t.ticker AND b.timestamp = t.timestamp
        LEFT JOIN pivoted_indicators i ON b.ticker = i.ticker AND b.timestamp = i.timestamp
        LEFT JOIN latest_news n ON b.ticker = n.ticker AND b.timestamp = n.timestamp
        """
        
        db.client.command(view_query)
        print("Created master table and materialized view successfully")
        
        # Populate with existing data
        await populate_master_table(db)
        
    except Exception as e:
        print(f"Error creating master table: {str(e)}")
        raise e

async def init_master_table(db: ClickHouseDB) -> None:
    """
    Initialize the master stock data table
    """
    await create_master_table(db) 