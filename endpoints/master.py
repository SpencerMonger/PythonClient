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
    
    # Custom metrics
    "price_diff": "Nullable(Float64)",  # Percentage difference with price 15 minutes later
    "target": "Nullable(Int32)",  # Classification of price_diff into categories
    
    # Aggregated fields from stock_quotes (per minute)
    "avg_bid_price": "Nullable(Float64)",
    "avg_ask_price": "Nullable(Float64)",
    "min_bid_price": "Nullable(Float64)",
    "max_ask_price": "Nullable(Float64)",
    "total_bid_size": "Nullable(Float64)",
    "total_ask_size": "Nullable(Float64)",
    "quote_count": "Nullable(Int32)",
    "quote_conditions": "Array(Int32)",
    "ask_exchange": "Nullable(Int32)",
    "bid_exchange": "Nullable(Int32)",
    "quote_indicators": "Array(Int32)",
    
    # Aggregated fields from stock_trades (per minute)
    "avg_trade_price": "Nullable(Float64)",
    "min_trade_price": "Nullable(Float64)",
    "max_trade_price": "Nullable(Float64)",
    "total_trade_size": "Nullable(Int32)",
    "trade_count": "Nullable(Int32)",
    "trade_conditions": "Array(Int32)",
    "trade_exchange": "Nullable(Int32)",
    
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
    
    # Daily high/low metrics
    "daily_high": "Nullable(Float64)",
    "daily_low": "Nullable(Float64)",
    "previous_close": "Nullable(Float64)",  # Previous day's closing price
    
    # True Range and ATR metrics
    "tr_current": "Nullable(Float64)",      # Current day's high-low range
    "tr_high_close": "Nullable(Float64)",   # High minus previous close
    "tr_low_close": "Nullable(Float64)",    # Low minus previous close
    "tr_value": "Nullable(Float64)",        # Maximum of the three TR values
    "atr_value": "Nullable(Float64)"        # 14-day average of TR values
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
        -- Convert quotes timestamps to minute intervals and concatenate arrays
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
                count(*) AS quote_count,
                arrayFlatten([coalesce(groupArray(conditions), [])]) AS quote_conditions,
                argMax(ask_exchange, sip_timestamp) AS ask_exchange,
                argMax(bid_exchange, sip_timestamp) AS bid_exchange,
                arrayFlatten([coalesce(groupArray(indicators), [])]) AS quote_indicators
            FROM {db.database}.stock_quotes
            GROUP BY ticker, timestamp
        ),
        
        -- Convert trades timestamps to minute intervals and concatenate arrays
        minute_trades AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) AS timestamp,
                avg(price) AS avg_trade_price,
                min(price) AS min_trade_price,
                max(price) AS max_trade_price,
                sum(size) AS total_trade_size,
                count(*) AS trade_count,
                arrayFlatten([coalesce(groupArray(conditions), [])]) AS trade_conditions,
                argMax(exchange, sip_timestamp) AS trade_exchange
            FROM {db.database}.stock_trades
            GROUP BY ticker, timestamp
        ),
        
        -- Pivot indicators to columns
        pivoted_indicators AS (
            SELECT
                ticker,
                timestamp,
                any(if(indicator_type = 'SMA_5', value, NULL)) AS sma_5,
                any(if(indicator_type = 'SMA_9', value, NULL)) AS sma_9,
                any(if(indicator_type = 'SMA_12', value, NULL)) AS sma_12,
                any(if(indicator_type = 'SMA_20', value, NULL)) AS sma_20,
                any(if(indicator_type = 'SMA_50', value, NULL)) AS sma_50,
                any(if(indicator_type = 'SMA_100', value, NULL)) AS sma_100,
                any(if(indicator_type = 'SMA_200', value, NULL)) AS sma_200,
                any(if(indicator_type = 'EMA_9', value, NULL)) AS ema_9,
                any(if(indicator_type = 'EMA_12', value, NULL)) AS ema_12,
                any(if(indicator_type = 'EMA_20', value, NULL)) AS ema_20,
                any(if(indicator_type = 'MACD', value, NULL)) AS macd_value,
                any(if(indicator_type = 'MACD', signal, NULL)) AS macd_signal,
                any(if(indicator_type = 'MACD', histogram, NULL)) AS macd_histogram,
                any(if(indicator_type = 'RSI', value, NULL)) AS rsi_14
            FROM {db.database}.stock_indicators
            GROUP BY ticker, timestamp
        ),
        
        -- Calculate price differences and targets
        price_metrics AS (
            SELECT 
                ticker,
                timestamp,
                close,
                ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 AS price_diff,
                multiIf(
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= -1, 0,
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= -0.5, 1,
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= 0, 2,
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= 0.5, 3,
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= 1, 4,
                    5
                ) AS target
            FROM {db.database}.stock_bars
        ),

        -- Calculate daily high/low metrics only
        tr_metrics AS (
            SELECT
                ticker,
                timestamp,
                max(high) OVER (PARTITION BY ticker, toDate(timestamp)) as daily_high,
                min(low) OVER (PARTITION BY ticker, toDate(timestamp)) as daily_low,
                -- Calculate TR components
                max(high) OVER (PARTITION BY ticker, toDate(timestamp)) - min(low) OVER (PARTITION BY ticker, toDate(timestamp)) as tr_current
            FROM {db.database}.stock_bars
        ),

        -- Get previous day's closing price from daily bars
        previous_close_metrics AS (
            WITH RECURSIVE 
            -- First get all possible dates we need to look up
            dates AS (
                SELECT DISTINCT
                    ticker,
                    timestamp,
                    toDate(timestamp) as current_date
                FROM {db.database}.stock_bars
            ),
            -- Recursively look back through dates until we find a close price
            previous_closes AS (
                SELECT
                    d.ticker,
                    d.timestamp,
                    d.current_date,
                    sd.close as found_close,
                    1 as depth
                FROM dates d
                LEFT JOIN {db.database}.stock_daily sd 
                    ON d.ticker = sd.ticker 
                    AND toDate(sd.timestamp) = toDate(subtractDays(d.current_date, 1))
                
                UNION ALL
                
                SELECT
                    pc.ticker,
                    pc.timestamp,
                    pc.current_date,
                    sd.close as found_close,
                    pc.depth + 1 as depth
                FROM previous_closes pc
                LEFT JOIN {db.database}.stock_daily sd 
                    ON pc.ticker = sd.ticker 
                    AND toDate(sd.timestamp) = toDate(subtractDays(pc.current_date, pc.depth + 1))
                WHERE pc.found_close IS NULL 
                    AND pc.depth < 5  -- Look back up to 5 days
            )
            -- Get the first non-null close price found for each timestamp
            SELECT 
                ticker,
                timestamp,
                argMinIf(found_close, depth, found_close IS NOT NULL) as previous_close
            FROM previous_closes
            GROUP BY ticker, timestamp
        ),

        -- Calculate TR and ATR metrics
        tr_atr_metrics AS (
            SELECT
                ticker,
                timestamp,
                tr_current,
                daily_high - previous_close as tr_high_close,
                daily_low - previous_close as tr_low_close,
                greatest(tr_current, abs(daily_high - previous_close), abs(daily_low - previous_close)) as tr_value,
                avg(greatest(tr_current, abs(daily_high - previous_close), abs(daily_low - previous_close))) 
                    OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr_value
            FROM (
                SELECT 
                    t.ticker,
                    t.timestamp,
                    t.daily_high,
                    t.daily_low,
                    t.tr_current,
                    pc.previous_close
                FROM tr_metrics t
                LEFT JOIN previous_close_metrics pc ON t.ticker = pc.ticker AND t.timestamp = pc.timestamp
            )
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
            p.price_diff,
            p.target,
            q.avg_bid_price,
            q.avg_ask_price,
            q.min_bid_price,
            q.max_ask_price,
            q.total_bid_size,
            q.total_ask_size,
            q.quote_count,
            q.quote_conditions,
            q.ask_exchange,
            q.bid_exchange,
            q.quote_indicators,
            t.avg_trade_price,
            t.min_trade_price,
            t.max_trade_price,
            t.total_trade_size,
            t.trade_count,
            t.trade_conditions,
            t.trade_exchange,
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
            tr.daily_high,
            tr.daily_low,
            pc.previous_close,
            tra.tr_current,
            tra.tr_high_close,
            tra.tr_low_close,
            tra.tr_value,
            tra.atr_value
        FROM {db.database}.stock_bars b
        LEFT JOIN price_metrics p ON b.ticker = p.ticker AND b.timestamp = p.timestamp
        LEFT JOIN minute_quotes q ON b.ticker = q.ticker AND b.timestamp = q.timestamp
        LEFT JOIN minute_trades t ON b.ticker = t.ticker AND b.timestamp = t.timestamp
        LEFT JOIN pivoted_indicators i ON b.ticker = i.ticker AND b.timestamp = i.timestamp
        LEFT JOIN tr_metrics tr ON b.ticker = tr.ticker AND b.timestamp = tr.timestamp
        LEFT JOIN previous_close_metrics pc ON b.ticker = pc.ticker AND b.timestamp = pc.timestamp
        LEFT JOIN tr_atr_metrics tra ON b.ticker = tra.ticker AND b.timestamp = tra.timestamp
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
        # Create the master table with timestamp ordering
        columns_def = ", ".join(f"{col} {type_}" for col, type_ in MASTER_SCHEMA.items())
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db.database}.{config.TABLE_STOCK_MASTER} (
            {columns_def}
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, ticker)
        """
        db.client.command(create_table_query)
        print("Created master table successfully")
        
        # Create the materialized view that will populate the master table
        view_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {db.database}.stock_master_mv 
        TO {db.database}.stock_master
        AS
        WITH 
        -- Convert quotes timestamps to minute intervals and concatenate arrays
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
                count(*) AS quote_count,
                arrayFlatten([coalesce(groupArray(conditions), [])]) AS quote_conditions,
                argMax(ask_exchange, sip_timestamp) AS ask_exchange,
                argMax(bid_exchange, sip_timestamp) AS bid_exchange,
                arrayFlatten([coalesce(groupArray(indicators), [])]) AS quote_indicators
            FROM {db.database}.stock_quotes
            GROUP BY ticker, timestamp
        ),
        
        -- Convert trades timestamps to minute intervals and concatenate arrays
        minute_trades AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) AS timestamp,
                avg(price) AS avg_trade_price,
                min(price) AS min_trade_price,
                max(price) AS max_trade_price,
                sum(size) AS total_trade_size,
                count(*) AS trade_count,
                arrayFlatten([coalesce(groupArray(conditions), [])]) AS trade_conditions,
                argMax(exchange, sip_timestamp) AS trade_exchange
            FROM {db.database}.stock_trades
            GROUP BY ticker, timestamp
        ),
        
        -- Pivot indicators to columns
        pivoted_indicators AS (
            SELECT
                ticker,
                timestamp,
                any(if(indicator_type = 'SMA_5', value, NULL)) AS sma_5,
                any(if(indicator_type = 'SMA_9', value, NULL)) AS sma_9,
                any(if(indicator_type = 'SMA_12', value, NULL)) AS sma_12,
                any(if(indicator_type = 'SMA_20', value, NULL)) AS sma_20,
                any(if(indicator_type = 'SMA_50', value, NULL)) AS sma_50,
                any(if(indicator_type = 'SMA_100', value, NULL)) AS sma_100,
                any(if(indicator_type = 'SMA_200', value, NULL)) AS sma_200,
                any(if(indicator_type = 'EMA_9', value, NULL)) AS ema_9,
                any(if(indicator_type = 'EMA_12', value, NULL)) AS ema_12,
                any(if(indicator_type = 'EMA_20', value, NULL)) AS ema_20,
                any(if(indicator_type = 'MACD', value, NULL)) AS macd_value,
                any(if(indicator_type = 'MACD', signal, NULL)) AS macd_signal,
                any(if(indicator_type = 'MACD', histogram, NULL)) AS macd_histogram,
                any(if(indicator_type = 'RSI', value, NULL)) AS rsi_14
            FROM {db.database}.stock_indicators
            GROUP BY ticker, timestamp
        ),
        
        -- Calculate price differences and targets
        price_metrics AS (
            SELECT 
                ticker,
                timestamp,
                close,
                ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 AS price_diff,
                multiIf(
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= -1, 0,
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= -0.5, 1,
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= 0, 2,
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= 0.5, 3,
                    ((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / close) * 100 <= 1, 4,
                    5
                ) AS target
            FROM {db.database}.stock_bars
        ),

        -- Calculate daily high/low metrics only
        tr_metrics AS (
            SELECT
                ticker,
                timestamp,
                max(high) OVER (PARTITION BY ticker, toDate(timestamp)) as daily_high,
                min(low) OVER (PARTITION BY ticker, toDate(timestamp)) as daily_low,
                -- Calculate TR components
                max(high) OVER (PARTITION BY ticker, toDate(timestamp)) - min(low) OVER (PARTITION BY ticker, toDate(timestamp)) as tr_current
            FROM {db.database}.stock_bars
        ),

        -- Get previous day's closing price from daily bars
        previous_close_metrics AS (
            WITH RECURSIVE 
            -- First get all possible dates we need to look up
            dates AS (
                SELECT DISTINCT
                    ticker,
                    timestamp,
                    toDate(timestamp) as current_date
                FROM {db.database}.stock_bars
            ),
            -- Recursively look back through dates until we find a close price
            previous_closes AS (
                SELECT
                    d.ticker,
                    d.timestamp,
                    d.current_date,
                    sd.close as found_close,
                    1 as depth
                FROM dates d
                LEFT JOIN {db.database}.stock_daily sd 
                    ON d.ticker = sd.ticker 
                    AND toDate(sd.timestamp) = toDate(subtractDays(d.current_date, 1))
                
                UNION ALL
                
                SELECT
                    pc.ticker,
                    pc.timestamp,
                    pc.current_date,
                    sd.close as found_close,
                    pc.depth + 1 as depth
                FROM previous_closes pc
                LEFT JOIN {db.database}.stock_daily sd 
                    ON pc.ticker = sd.ticker 
                    AND toDate(sd.timestamp) = toDate(subtractDays(pc.current_date, pc.depth + 1))
                WHERE pc.found_close IS NULL 
                    AND pc.depth < 5  -- Look back up to 5 days
            )
            -- Get the first non-null close price found for each timestamp
            SELECT 
                ticker,
                timestamp,
                argMinIf(found_close, depth, found_close IS NOT NULL) as previous_close
            FROM previous_closes
            GROUP BY ticker, timestamp
        ),

        -- Calculate TR and ATR metrics
        tr_atr_metrics AS (
            SELECT
                ticker,
                timestamp,
                tr_current,
                daily_high - previous_close as tr_high_close,
                daily_low - previous_close as tr_low_close,
                greatest(tr_current, abs(daily_high - previous_close), abs(daily_low - previous_close)) as tr_value,
                avg(greatest(tr_current, abs(daily_high - previous_close), abs(daily_low - previous_close))) 
                    OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr_value
            FROM (
                SELECT 
                    t.ticker,
                    t.timestamp,
                    t.daily_high,
                    t.daily_low,
                    t.tr_current,
                    pc.previous_close
                FROM tr_metrics t
                LEFT JOIN previous_close_metrics pc ON t.ticker = pc.ticker AND t.timestamp = pc.timestamp
            )
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
            p.price_diff,
            p.target,
            q.avg_bid_price,
            q.avg_ask_price,
            q.min_bid_price,
            q.max_ask_price,
            q.total_bid_size,
            q.total_ask_size,
            q.quote_count,
            q.quote_conditions,
            q.ask_exchange,
            q.bid_exchange,
            q.quote_indicators,
            t.avg_trade_price,
            t.min_trade_price,
            t.max_trade_price,
            t.total_trade_size,
            t.trade_count,
            t.trade_conditions,
            t.trade_exchange,
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
            tr.daily_high,
            tr.daily_low,
            pc.previous_close,
            tra.tr_current,
            tra.tr_high_close,
            tra.tr_low_close,
            tra.tr_value,
            tra.atr_value
        FROM {db.database}.stock_bars b
        LEFT JOIN price_metrics p ON b.ticker = p.ticker AND b.timestamp = p.timestamp
        LEFT JOIN minute_quotes q ON b.ticker = q.ticker AND b.timestamp = q.timestamp
        LEFT JOIN minute_trades t ON b.ticker = t.ticker AND b.timestamp = t.timestamp
        LEFT JOIN pivoted_indicators i ON b.ticker = i.ticker AND b.timestamp = i.timestamp
        LEFT JOIN tr_metrics tr ON b.ticker = tr.ticker AND b.timestamp = tr.timestamp
        LEFT JOIN previous_close_metrics pc ON b.ticker = pc.ticker AND b.timestamp = pc.timestamp
        LEFT JOIN tr_atr_metrics tra ON b.ticker = tra.ticker AND b.timestamp = tra.timestamp
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