import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any

from endpoints.db import ClickHouseDB
from endpoints import config

# Schema for base master data table (without calculated fields)
BASE_MASTER_SCHEMA = {
    # Common fields
    "uni_id": "UInt64",
    "ticker": "String",
    "timestamp": "DateTime64(9)",
    
    # Fields from stock_bars
    "open": "Nullable(Float64)",
    "high": "Nullable(Float64)",
    "low": "Nullable(Float64)",
    "close": "Nullable(Float64)",
    "volume": "Nullable(Int64)",
    "vwap": "Nullable(Float64)",
    "transactions": "Nullable(Int64)",
    
    # Aggregated fields from stock_quotes
    "avg_bid_price": "Nullable(Float64)",
    "avg_ask_price": "Nullable(Float64)",
    "min_bid_price": "Nullable(Float64)",
    "max_ask_price": "Nullable(Float64)",
    "total_bid_size": "Nullable(Float64)",
    "total_ask_size": "Nullable(Float64)",
    "quote_count": "Nullable(Int32)",
    "quote_conditions": "String",
    "ask_exchange": "Nullable(Int32)",
    "bid_exchange": "Nullable(Int32)",
    
    # Aggregated fields from stock_trades
    "avg_trade_price": "Nullable(Float64)",
    "min_trade_price": "Nullable(Float64)",
    "max_trade_price": "Nullable(Float64)",
    "total_trade_size": "Nullable(Int32)",
    "trade_count": "Nullable(Int32)",
    "trade_conditions": "String",
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
    "rsi_14": "Nullable(Float64)"
}

# Schema for normalized stock data table
NORMALIZED_SCHEMA = {
    # Non-normalized fields
    "uni_id": "UInt64",  # Unique ID based on ticker and timestamp
    "ticker": "String",
    "timestamp": "DateTime64(9)",
    "target": "Nullable(Int32)",
    "quote_conditions": "String",
    "trade_conditions": "String",
    "ask_exchange": "Nullable(Int32)",
    "bid_exchange": "Nullable(Int32)",
    "trade_exchange": "Nullable(Int32)",
    
    # Normalized fields (all Float64)
    "open": "Float64",
    "high": "Float64",
    "low": "Float64",
    "close": "Float64",
    "volume": "Float64",
    "vwap": "Float64",
    "transactions": "Float64",
    "price_diff": "Float64",
    "max_price_diff": "Float64",
    "avg_bid_price": "Float64",
    "avg_ask_price": "Float64",
    "min_bid_price": "Float64",
    "max_ask_price": "Float64",
    "total_bid_size": "Float64",
    "total_ask_size": "Float64",
    "quote_count": "Float64",
    "avg_trade_price": "Float64",
    "min_trade_price": "Float64",
    "max_trade_price": "Float64",
    "total_trade_size": "Float64",
    "trade_count": "Float64",
    "sma_5": "Float64",
    "sma_9": "Float64",
    "sma_12": "Float64",
    "sma_20": "Float64",
    "sma_50": "Float64",
    "sma_100": "Float64",
    "sma_200": "Float64",
    "ema_9": "Float64",
    "ema_12": "Float64",
    "ema_20": "Float64",
    "macd_value": "Float64",
    "macd_signal": "Float64",
    "macd_histogram": "Float64",
    "rsi_14": "Float64",
    "daily_high": "Float64",
    "daily_low": "Float64",
    "previous_close": "Float64",
    "tr_current": "Float64",
    "tr_high_close": "Float64",
    "tr_low_close": "Float64",
    "tr_value": "Float64",
    "atr_value": "Float64"
}

async def create_base_master_table(db: ClickHouseDB) -> None:
    """Create the base master table that combines raw data from source tables"""
    try:
        # Create the base master table
        columns_def = ", ".join(f"{col} {type_}" for col, type_ in BASE_MASTER_SCHEMA.items())
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db.database}.stock_master_base (
            {columns_def}
        ) ENGINE = ReplacingMergeTree()
        PRIMARY KEY (timestamp, ticker)
        ORDER BY (timestamp, ticker)
        SETTINGS index_granularity = 8192
        """
        db.client.command(create_table_query)
        print("Created base master table successfully")
        
    except Exception as e:
        print(f"Error creating base master table: {str(e)}")
        raise e

async def create_master_view(db: ClickHouseDB) -> None:
    """Create the master table (not a materialized view) with all custom metrics"""
    try:
        # Create the master table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db.database}.{config.TABLE_STOCK_MASTER} (
            uni_id UInt64,
            ticker String,
            timestamp DateTime64(9),
            open Nullable(Float64),
            high Nullable(Float64),
            low Nullable(Float64),
            close Nullable(Float64),
            volume Nullable(Int64),
            vwap Nullable(Float64),
            transactions Nullable(Int64),
            avg_bid_price Nullable(Float64),
            avg_ask_price Nullable(Float64),
            min_bid_price Nullable(Float64),
            max_ask_price Nullable(Float64),
            total_bid_size Nullable(Float64),
            total_ask_size Nullable(Float64),
            quote_count Nullable(Int32),
            quote_conditions String,
            ask_exchange Nullable(Int32),
            bid_exchange Nullable(Int32),
            avg_trade_price Nullable(Float64),
            min_trade_price Nullable(Float64),
            max_trade_price Nullable(Float64),
            total_trade_size Nullable(Int32),
            trade_count Nullable(Int32),
            trade_conditions String,
            trade_exchange Nullable(Int32),
            sma_5 Nullable(Float64),
            sma_9 Nullable(Float64),
            sma_12 Nullable(Float64),
            sma_20 Nullable(Float64),
            sma_50 Nullable(Float64),
            sma_100 Nullable(Float64),
            sma_200 Nullable(Float64),
            ema_9 Nullable(Float64),
            ema_12 Nullable(Float64),
            ema_20 Nullable(Float64),
            macd_value Nullable(Float64),
            macd_signal Nullable(Float64),
            macd_histogram Nullable(Float64),
            rsi_14 Nullable(Float64),
            price_diff Nullable(Float64),
            max_price_diff Nullable(Float64),
            target Nullable(Int32),
            daily_high Nullable(Float64),
            daily_low Nullable(Float64),
            previous_close Nullable(Float64),
            tr_current Nullable(Float64),
            tr_high_close Nullable(Float64),
            tr_low_close Nullable(Float64),
            tr_value Nullable(Float64),
            atr_value Nullable(Float64)
        ) ENGINE = ReplacingMergeTree()
        PRIMARY KEY (timestamp, ticker)
        ORDER BY (timestamp, ticker)
        SETTINGS index_granularity = 8192
        """
        
        db.client.command(create_table_query)
        print("Created master table successfully")
        
        # Now populate the table with existing data
        print("Populating master table with existing data...")
        populate_query = f"""
        INSERT INTO {db.database}.{config.TABLE_STOCK_MASTER}
        WITH base_data AS (
            SELECT 
                b.*,
                -- Calculate price_diff using future close prices
                round(((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100, 2) as price_diff,
                -- Calculate max_price_diff
                round(if(
                    abs(((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100) >
                    abs(((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100),
                    ((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100,
                    ((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100
                ), 2) as max_price_diff,
                -- Calculate daily metrics
                max(high) OVER (PARTITION BY ticker, toDate(timestamp)) as daily_high,
                min(low) OVER (PARTITION BY ticker, toDate(timestamp)) as daily_low,
                any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as previous_close
            FROM {db.database}.stock_master_base b
        )
        SELECT
            b.*,
            -- Calculate target based on price_diff
            multiIf(
                b.price_diff <= -1, 0,
                b.price_diff <= -0.5, 1,
                b.price_diff <= 0, 2,
                b.price_diff <= 0.5, 3,
                b.price_diff <= 1, 4,
                5
            ) as target,
            -- Calculate TR metrics
            b.daily_high - b.daily_low as tr_current,
            b.daily_high - b.previous_close as tr_high_close,
            b.daily_low - b.previous_close as tr_low_close,
            greatest(
                b.daily_high - b.daily_low,
                abs(b.daily_high - b.previous_close),
                abs(b.daily_low - b.previous_close)
            ) as tr_value,
            avg(greatest(
                b.daily_high - b.daily_low,
                abs(b.daily_high - b.previous_close),
                abs(b.daily_low - b.previous_close)
            )) OVER (PARTITION BY b.ticker ORDER BY b.timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr_value
        FROM base_data b
        """
        
        db.client.command(populate_query)
        print("Populated master table successfully")
        
    except Exception as e:
        print(f"Error creating master table: {str(e)}")
        raise e

async def create_normalized_table(db: ClickHouseDB) -> None:
    """Create the normalized stock data table"""
    try:
        # Create the normalized table with explicit column ordering
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db.database}.{config.TABLE_STOCK_NORMALIZED} (
            uni_id UInt64,
            ticker String,
            timestamp DateTime64(9),
            target Nullable(Int32),
            quote_conditions String,
            trade_conditions String,
            ask_exchange Nullable(Int32),
            bid_exchange Nullable(Int32),
            trade_exchange Nullable(Int32),
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            vwap Float64,
            transactions Float64,
            price_diff Float64,
            max_price_diff Float64,
            avg_bid_price Float64,
            avg_ask_price Float64,
            min_bid_price Float64,
            max_ask_price Float64,
            total_bid_size Float64,
            total_ask_size Float64,
            quote_count Float64,
            avg_trade_price Float64,
            min_trade_price Float64,
            max_trade_price Float64,
            total_trade_size Float64,
            trade_count Float64,
            sma_5 Float64,
            sma_9 Float64,
            sma_12 Float64,
            sma_20 Float64,
            sma_50 Float64,
            sma_100 Float64,
            sma_200 Float64,
            ema_9 Float64,
            ema_12 Float64,
            ema_20 Float64,
            macd_value Float64,
            macd_signal Float64,
            macd_histogram Float64,
            rsi_14 Float64,
            daily_high Float64,
            daily_low Float64,
            previous_close Float64,
            tr_current Float64,
            tr_high_close Float64,
            tr_low_close Float64,
            tr_value Float64,
            atr_value Float64
        ) ENGINE = ReplacingMergeTree()
        PRIMARY KEY (timestamp, ticker)
        ORDER BY (timestamp, ticker)
        SETTINGS index_granularity = 8192
        """
        
        db.client.command(create_table_query)
        print("Created normalized table successfully")
        
    except Exception as e:
        print(f"Error creating normalized table: {str(e)}")
        raise e

async def update_normalized_table(db: ClickHouseDB, from_date: datetime, to_date: datetime) -> None:
    """Update the normalized table with data from the master table for the given time range."""
    try:
        print(f"Updating normalized table with data from {from_date} to {to_date}...")
        
        # Format timestamps in ClickHouse format
        from_date_str = from_date.strftime('%Y-%m-%d %H:%M:%S')
        to_date_str = to_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # First, delete any existing data in the time range to avoid duplicates
        delete_query = f"""
        ALTER TABLE {db.database}.{config.TABLE_STOCK_NORMALIZED} 
        DELETE WHERE timestamp BETWEEN '{from_date_str}' AND '{to_date_str}'
        """
        db.client.command(delete_query)
        
        # Insert normalized data for the time range
        insert_query = f"""
        INSERT INTO {db.database}.{config.TABLE_STOCK_NORMALIZED}
        SELECT
            m.uni_id,
            m.ticker,
            m.timestamp,
            m.target,
            m.quote_conditions,
            m.trade_conditions,
            m.ask_exchange,
            m.bid_exchange,
            m.trade_exchange,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.open, 0), 2) / 1000))), 2) as open,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.high, 0), 2) / 1000))), 2) as high,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.low, 0), 2) / 1000))), 2) as low,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.close, 0), 2) / 1000))), 2) as close,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.volume, 0), 2) / 1000000))), 2) as volume,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.vwap, 0), 2) / 1000))), 2) as vwap,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.transactions, 0), 2) / 1000))), 2) as transactions,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.price_diff, 0), 2) / 10))), 2) as price_diff,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.max_price_diff, 0), 2) / 10))), 2) as max_price_diff,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.avg_bid_price, 0), 2) / 1000))), 2) as avg_bid_price,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.avg_ask_price, 0), 2) / 1000))), 2) as avg_ask_price,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.min_bid_price, 0), 2) / 1000))), 2) as min_bid_price,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.max_ask_price, 0), 2) / 1000))), 2) as max_ask_price,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.total_bid_size, 0), 2) / 100000))), 2) as total_bid_size,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.total_ask_size, 0), 2) / 100000))), 2) as total_ask_size,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.quote_count, 0), 2) / 1000))), 2) as quote_count,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.avg_trade_price, 0), 2) / 1000))), 2) as avg_trade_price,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.min_trade_price, 0), 2) / 1000))), 2) as min_trade_price,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.max_trade_price, 0), 2) / 1000))), 2) as max_trade_price,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.total_trade_size, 0), 2) / 100000))), 2) as total_trade_size,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.trade_count, 0), 2) / 1000))), 2) as trade_count,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.sma_5, 0), 2) / 1000))), 2) as sma_5,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.sma_9, 0), 2) / 1000))), 2) as sma_9,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.sma_12, 0), 2) / 1000))), 2) as sma_12,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.sma_20, 0), 2) / 1000))), 2) as sma_20,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.sma_50, 0), 2) / 1000))), 2) as sma_50,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.sma_100, 0), 2) / 1000))), 2) as sma_100,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.sma_200, 0), 2) / 1000))), 2) as sma_200,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.ema_9, 0), 2) / 1000))), 2) as ema_9,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.ema_12, 0), 2) / 1000))), 2) as ema_12,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.ema_20, 0), 2) / 1000))), 2) as ema_20,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.macd_value, 0), 2) / 10))), 2) as macd_value,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.macd_signal, 0), 2) / 10))), 2) as macd_signal,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.macd_histogram, 0), 2) / 10))), 2) as macd_histogram,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.rsi_14, 0), 2) / 100))), 2) as rsi_14,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.daily_high, 0), 2) / 1000))), 2) as daily_high,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.daily_low, 0), 2) / 1000))), 2) as daily_low,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.previous_close, 0), 2) / 1000))), 2) as previous_close,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.tr_current, 0), 2) / 100))), 2) as tr_current,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.tr_high_close, 0), 2) / 100))), 2) as tr_high_close,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.tr_low_close, 0), 2) / 100))), 2) as tr_low_close,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.tr_value, 0), 2) / 100))), 2) as tr_value,
            round(5 / (1 + exp(-5 * (coalesce(nullIf(m.atr_value, 0), 2) / 100))), 2) as atr_value
        FROM {db.database}.{config.TABLE_STOCK_MASTER} m
        WHERE m.timestamp BETWEEN '{from_date_str}' AND '{to_date_str}'
        ORDER BY m.timestamp DESC
        """
        
        db.client.command(insert_query)
        print("Normalized table updated successfully")
        
    except Exception as e:
        print(f"Error updating normalized table: {str(e)}")
        raise e

async def insert_latest_data(db: ClickHouseDB, from_date: datetime, to_date: datetime) -> None:
    """Insert only the latest data into both base and master tables, then update normalized table."""
    try:
        print(f"Inserting latest data from {from_date} to {to_date} into tables...")
        
        # Format timestamps in ClickHouse format
        from_date_str = from_date.strftime('%Y-%m-%d %H:%M:%S')
        to_date_str = to_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # First, delete any existing data in the time range to avoid duplicates
        print("Removing any existing data in the time range...")
        delete_base_query = f"""
        ALTER TABLE {db.database}.stock_master_base 
        DELETE WHERE timestamp BETWEEN '{from_date_str}' AND '{to_date_str}'
        """
        delete_master_query = f"""
        ALTER TABLE {db.database}.{config.TABLE_STOCK_MASTER}
        DELETE WHERE timestamp BETWEEN '{from_date_str}' AND '{to_date_str}'
        """
        db.client.command(delete_base_query)
        db.client.command(delete_master_query)
        
        # First insert into base table
        print("Inserting latest data into base table...")
        base_insert_query = f"""
        INSERT INTO {db.database}.stock_master_base
        SELECT
            b.uni_id,
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
            q.quote_conditions,
            q.ask_exchange,
            q.bid_exchange,
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
            i.rsi_14
        FROM (
            SELECT 
                cityHash64(ticker, toString(timestamp)) as uni_id,
                ticker,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                vwap,
                transactions
            FROM {db.database}.stock_bars
            WHERE timestamp BETWEEN '{from_date_str}' AND '{to_date_str}'
        ) b
        LEFT JOIN (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) as timestamp,
                avg(bid_price) as avg_bid_price,
                avg(ask_price) as avg_ask_price,
                min(bid_price) as min_bid_price,
                max(ask_price) as max_ask_price,
                sum(bid_size) as total_bid_size,
                sum(ask_size) as total_ask_size,
                count(*) as quote_count,
                arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([groupArray(conditions)]))) AS quote_conditions,
                argMax(ask_exchange, sip_timestamp) as ask_exchange,
                argMax(bid_exchange, sip_timestamp) as bid_exchange
            FROM {db.database}.stock_quotes
            WHERE sip_timestamp BETWEEN '{from_date_str}' AND '{to_date_str}'
            GROUP BY ticker, timestamp
        ) q ON b.ticker = q.ticker AND b.timestamp = q.timestamp
        LEFT JOIN (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) as timestamp,
                avg(price) as avg_trade_price,
                min(price) as min_trade_price,
                max(price) as max_trade_price,
                sum(size) as total_trade_size,
                count(*) as trade_count,
                arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([groupArray(conditions)]))) AS trade_conditions,
                argMax(exchange, sip_timestamp) as trade_exchange
            FROM {db.database}.stock_trades
            WHERE sip_timestamp BETWEEN '{from_date_str}' AND '{to_date_str}'
            GROUP BY ticker, timestamp
        ) t ON b.ticker = t.ticker AND b.timestamp = t.timestamp
        LEFT JOIN (
            SELECT
                ticker,
                timestamp,
                any(if(indicator_type = 'SMA_5', value, NULL)) as sma_5,
                any(if(indicator_type = 'SMA_9', value, NULL)) as sma_9,
                any(if(indicator_type = 'SMA_12', value, NULL)) as sma_12,
                any(if(indicator_type = 'SMA_20', value, NULL)) as sma_20,
                any(if(indicator_type = 'SMA_50', value, NULL)) as sma_50,
                any(if(indicator_type = 'SMA_100', value, NULL)) as sma_100,
                any(if(indicator_type = 'SMA_200', value, NULL)) as sma_200,
                any(if(indicator_type = 'EMA_9', value, NULL)) as ema_9,
                any(if(indicator_type = 'EMA_12', value, NULL)) as ema_12,
                any(if(indicator_type = 'EMA_20', value, NULL)) as ema_20,
                any(if(indicator_type = 'MACD', value, NULL)) as macd_value,
                any(if(indicator_type = 'MACD', signal, NULL)) as macd_signal,
                any(if(indicator_type = 'MACD', histogram, NULL)) as macd_histogram,
                any(if(indicator_type = 'RSI', value, NULL)) as rsi_14
            FROM {db.database}.stock_indicators
            WHERE timestamp BETWEEN '{from_date_str}' AND '{to_date_str}'
            GROUP BY ticker, timestamp
        ) i ON b.ticker = i.ticker AND b.timestamp = i.timestamp
        """
        
        db.client.command(base_insert_query)
        print(f"Latest data inserted into base table")

        # Now insert directly into master table with calculated fields
        print("Inserting latest data into master table...")
        master_insert_query = f"""
        INSERT INTO {db.database}.{config.TABLE_STOCK_MASTER}
        WITH base_data AS (
            SELECT 
                b.*,
                -- Calculate price_diff using future close prices
                round(((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100, 2) as price_diff,
                -- Calculate max_price_diff
                round(if(
                    abs(((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100) >
                    abs(((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100),
                    ((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100,
                    ((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100
                ), 2) as max_price_diff,
                -- Calculate daily metrics
                max(high) OVER (PARTITION BY ticker, toDate(timestamp)) as daily_high,
                min(low) OVER (PARTITION BY ticker, toDate(timestamp)) as daily_low,
                any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as previous_close
            FROM {db.database}.stock_master_base b
            WHERE timestamp BETWEEN '{from_date_str}' AND '{to_date_str}'
        )
        SELECT
            b.*,
            -- Calculate target based on price_diff
            multiIf(
                b.price_diff <= -1, 0,
                b.price_diff <= -0.5, 1,
                b.price_diff <= 0, 2,
                b.price_diff <= 0.5, 3,
                b.price_diff <= 1, 4,
                5
            ) as target,
            -- Calculate TR metrics
            b.daily_high - b.daily_low as tr_current,
            b.daily_high - b.previous_close as tr_high_close,
            b.daily_low - b.previous_close as tr_low_close,
            greatest(
                b.daily_high - b.daily_low,
                abs(b.daily_high - b.previous_close),
                abs(b.daily_low - b.previous_close)
            ) as tr_value,
            avg(greatest(
                b.daily_high - b.daily_low,
                abs(b.daily_high - b.previous_close),
                abs(b.daily_low - b.previous_close)
            )) OVER (PARTITION BY b.ticker ORDER BY b.timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr_value
        FROM base_data b
        """

        db.client.command(master_insert_query)
        print(f"Latest data inserted into master table")
        
        # Update the normalized table with the latest data
        await update_normalized_table(db, from_date, to_date)
        
    except Exception as e:
        print(f"Error inserting latest data: {str(e)}")
        raise e

async def populate_base_master_table(db: ClickHouseDB) -> None:
    """Populate the base master table with existing data from source tables"""
    try:
        print("Populating base master table with existing data...")
        
        # Get the date range we need to process
        date_range_query = f"""
        SELECT 
            min(toDate(timestamp)) as min_date,
            max(toDate(timestamp)) as max_date
        FROM {db.database}.stock_bars
        """
        result = db.client.query(date_range_query)
        min_date = result.first_row[0]
        max_date = result.first_row[1]
        
        print(f"Processing data from {min_date} to {max_date}")
        
        # Insert all existing data
        insert_query = f"""
        INSERT INTO {db.database}.stock_master_base
        WITH 
        -- Get base data
        base_data AS (
            SELECT 
                ticker,
                timestamp,
                cityHash64(ticker, toString(timestamp)) as uni_id,
                open,
                high,
                low,
                close,
                volume,
                vwap,
                transactions
            FROM {db.database}.stock_bars
            WHERE toDate(timestamp) BETWEEN '{min_date}' AND '{max_date}'
        ),
        -- Get quote data
        quote_metrics AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) as timestamp,
                avg(bid_price) as avg_bid_price,
                avg(ask_price) as avg_ask_price,
                min(bid_price) as min_bid_price,
                max(ask_price) as max_ask_price,
                sum(bid_size) as total_bid_size,
                sum(ask_size) as total_ask_size,
                count(*) as quote_count,
                arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([groupArray(conditions)]))) AS quote_conditions,
                argMax(ask_exchange, sip_timestamp) as ask_exchange,
                argMax(bid_exchange, sip_timestamp) as bid_exchange
            FROM {db.database}.stock_quotes
            WHERE toDate(sip_timestamp) BETWEEN '{min_date}' AND '{max_date}'
            GROUP BY ticker, timestamp
        ),
        -- Get trade data
        trade_metrics AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) as timestamp,
                avg(price) as avg_trade_price,
                min(price) as min_trade_price,
                max(price) as max_trade_price,
                sum(size) as total_trade_size,
                count(*) as trade_count,
                arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([groupArray(conditions)]))) AS trade_conditions,
                argMax(exchange, sip_timestamp) as trade_exchange
            FROM {db.database}.stock_trades
            WHERE toDate(sip_timestamp) BETWEEN '{min_date}' AND '{max_date}'
            GROUP BY ticker, timestamp
        ),
        -- Get indicator data
        indicator_metrics AS (
            SELECT
                ticker,
                timestamp,
                any(if(indicator_type = 'SMA_5', value, NULL)) as sma_5,
                any(if(indicator_type = 'SMA_9', value, NULL)) as sma_9,
                any(if(indicator_type = 'SMA_12', value, NULL)) as sma_12,
                any(if(indicator_type = 'SMA_20', value, NULL)) as sma_20,
                any(if(indicator_type = 'SMA_50', value, NULL)) as sma_50,
                any(if(indicator_type = 'SMA_100', value, NULL)) as sma_100,
                any(if(indicator_type = 'SMA_200', value, NULL)) as sma_200,
                any(if(indicator_type = 'EMA_9', value, NULL)) as ema_9,
                any(if(indicator_type = 'EMA_12', value, NULL)) as ema_12,
                any(if(indicator_type = 'EMA_20', value, NULL)) as ema_20,
                any(if(indicator_type = 'MACD', value, NULL)) as macd_value,
                any(if(indicator_type = 'MACD', signal, NULL)) as macd_signal,
                any(if(indicator_type = 'MACD', histogram, NULL)) as macd_histogram,
                any(if(indicator_type = 'RSI', value, NULL)) as rsi_14
            FROM {db.database}.stock_indicators
            WHERE toDate(timestamp) BETWEEN '{min_date}' AND '{max_date}'
            GROUP BY ticker, timestamp
        )
        
        SELECT
            b.uni_id,
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
            q.quote_conditions,
            q.ask_exchange,
            q.bid_exchange,
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
            i.rsi_14
        FROM base_data b
        LEFT JOIN quote_metrics q ON b.ticker = q.ticker AND b.timestamp = q.timestamp
        LEFT JOIN trade_metrics t ON b.ticker = t.ticker AND b.timestamp = t.timestamp
        LEFT JOIN indicator_metrics i ON b.ticker = i.ticker AND b.timestamp = i.timestamp
        """
        
        db.client.command(insert_query)
        print("Base master table populated successfully")
        
    except Exception as e:
        print(f"Error populating base master table: {str(e)}")
        raise e

async def init_master_v2(db: ClickHouseDB) -> None:
    """Initialize the new version of master tables"""
    try:
        start_time = datetime.now()
        
        # Drop existing tables
        print("Dropping existing master tables...")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master_base")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.{config.TABLE_STOCK_MASTER}")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.{config.TABLE_STOCK_NORMALIZED}")
        print("Existing tables dropped successfully")
        
        # Create and populate base table
        print("\nCreating base master table...")
        await create_base_master_table(db)
        print("\nPopulating base master table...")
        await populate_base_master_table(db)
        
        # Create master table
        print("\nCreating master table...")
        await create_master_view(db)
        
        # Create normalized table
        print("\nCreating normalized table...")
        await create_normalized_table(db)
        
        # Get date range for populating normalized table
        date_range_query = f"""
        SELECT 
            min(timestamp) as min_date,
            max(timestamp) as max_date
        FROM {db.database}.{config.TABLE_STOCK_MASTER}
        """
        result = db.client.query(date_range_query)
        min_date = result.first_row[0]
        max_date = result.first_row[1]
        
        print(f"\nPopulating normalized table with data from {min_date} to {max_date}...")
        await update_normalized_table(db, min_date, max_date)
        
        total_time = datetime.now() - start_time
        print(f"\nMaster table v2 initialization complete! Total time: {total_time}")
        
    except Exception as e:
        print(f"Error initializing master v2: {str(e)}")
        raise e 