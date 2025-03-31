import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import pytz

from endpoints.db import ClickHouseDB
from endpoints import config

"""
Master V2 Module - Optimized for Live Mode

This module provides an alternative to the master.py module, specifically optimized for live data mode.
While master.py is designed to recreate the entire master table from scratch (historical mode),
this module focuses on efficiently updating only the current day's data.

Key differences from master.py:
1. Uses materialized views for more efficient updates
2. Provides reinit_current_day function to refresh only today's data
3. Simplifies the insert_latest_data function for live mode
4. Uses ReplacingMergeTree engine instead of MergeTree for better performance with updates

Usage:
- For full historical initialization: Use master.py's init_master_table()
- For live mode updates: Use master_v2.py's insert_latest_data()

The module integrates with main.py and live_data.py to provide optimized performance in live mode.
"""

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
    "trade_conditions": "Float64",  # Changed from String to Float64 for model compatibility
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

async def create_master_view(db: ClickHouseDB) -> None:
    """Create the master table as a materialized view"""
    try:
        # Create the master table first
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
        
        # Create the materialized view
        create_view_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {db.database}.stock_master_mv 
        TO {db.database}.{config.TABLE_STOCK_MASTER}
        AS
        WITH 
        -- Get base data with bars
        base_data AS (
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
                transactions,
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
            FROM {db.database}.stock_bars
        ),
        -- Get quote metrics
        quote_metrics AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) as quote_timestamp,
                round(avg(bid_price), 2) as avg_bid_price,
                round(avg(ask_price), 2) as avg_ask_price,
                round(min(bid_price), 2) as min_bid_price,
                round(max(ask_price), 2) as max_ask_price,
                sum(bid_size) as total_bid_size,
                sum(ask_size) as total_ask_size,
                count() as quote_count,
                groupArray(toString(conditions))[1] as quote_conditions,
                any(ask_exchange) as ask_exchange,
                any(bid_exchange) as bid_exchange
            FROM {db.database}.stock_quotes
            WHERE toDate(sip_timestamp) = today()
            GROUP BY ticker, toStartOfMinute(sip_timestamp)
        ),
        -- Get trade metrics
        trade_metrics AS (
            SELECT
                ticker,
                toStartOfMinute(sip_timestamp) as trade_timestamp,
                round(avg(price), 2) as avg_trade_price,
                round(min(price), 2) as min_trade_price,
                round(max(price), 2) as max_trade_price,
                sum(size) as total_trade_size,
                count() as trade_count,
                groupArray(toString(conditions))[1] as trade_conditions,
                any(exchange) as trade_exchange
            FROM {db.database}.stock_trades
            WHERE toDate(sip_timestamp) = today()
            GROUP BY ticker, toStartOfMinute(sip_timestamp)
        ),
        -- Get indicator metrics
        indicator_metrics AS (
            SELECT
                ticker,
                timestamp as indicator_timestamp,
                round(any(if(indicator_type = 'SMA_5', value, NULL)), 2) as sma_5,
                round(any(if(indicator_type = 'SMA_9', value, NULL)), 2) as sma_9,
                round(any(if(indicator_type = 'SMA_12', value, NULL)), 2) as sma_12,
                round(any(if(indicator_type = 'SMA_20', value, NULL)), 2) as sma_20,
                round(any(if(indicator_type = 'SMA_50', value, NULL)), 2) as sma_50,
                round(any(if(indicator_type = 'SMA_100', value, NULL)), 2) as sma_100,
                round(any(if(indicator_type = 'SMA_200', value, NULL)), 2) as sma_200,
                round(any(if(indicator_type = 'EMA_9', value, NULL)), 2) as ema_9,
                round(any(if(indicator_type = 'EMA_12', value, NULL)), 2) as ema_12,
                round(any(if(indicator_type = 'EMA_20', value, NULL)), 2) as ema_20,
                round(any(if(indicator_type = 'MACD', value, NULL)), 2) as macd_value,
                round(any(if(indicator_type = 'MACD', signal, NULL)), 2) as macd_signal,
                round(any(if(indicator_type = 'MACD', histogram, NULL)), 2) as macd_histogram,
                round(any(if(indicator_type = 'RSI', value, NULL)), 2) as rsi_14
            FROM {db.database}.stock_indicators
            WHERE toDate(timestamp) = today()
            GROUP BY ticker, timestamp
        ),
        -- Join all data together
        combined_data AS (
            SELECT
                b.*,
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
                i.rsi_14,
                -- Daily metrics
                b.daily_high,
                b.daily_low,
                b.previous_close,
                -- TR metrics
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
        LEFT JOIN quote_metrics q ON b.ticker = q.ticker AND b.timestamp = q.quote_timestamp
        LEFT JOIN trade_metrics t ON b.ticker = t.ticker AND b.timestamp = t.trade_timestamp
        LEFT JOIN indicator_metrics i ON b.ticker = i.ticker AND b.timestamp = i.indicator_timestamp
        WHERE toDate(b.timestamp) = today()
        )
        SELECT
            uni_id,
            ticker,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            vwap,
            transactions,
            avg_bid_price,
            avg_ask_price,
            min_bid_price,
            max_ask_price,
            total_bid_size,
            total_ask_size,
            quote_count,
            quote_conditions,
            ask_exchange,
            bid_exchange,
            avg_trade_price,
            min_trade_price,
            max_trade_price,
            total_trade_size,
            trade_count,
            trade_conditions,
            trade_exchange,
            sma_5,
            sma_9,
            sma_12,
            sma_20,
            sma_50,
            sma_100,
            sma_200,
            ema_9,
            ema_12,
            ema_20,
            macd_value,
            macd_signal,
            macd_histogram,
            rsi_14,
            price_diff,
            max_price_diff,
            -- Calculate target based on price_diff
            multiIf(
                price_diff <= -1, 0,
                price_diff <= -0.5, 1,
                price_diff <= 0, 2,
                price_diff <= 0.5, 3,
                price_diff <= 1, 4,
                5
            ) as target,
            daily_high,
            daily_low,
            previous_close,
            tr_current,
            tr_high_close,
            tr_low_close,
            tr_value,
            atr_value
        FROM combined_data
        """
        
        db.client.command(create_view_query)
        print("Created master materialized view successfully")
        
    except Exception as e:
        print(f"Error creating master view: {str(e)}")
        raise e

async def create_normalized_table(db: ClickHouseDB) -> None:
    """Create the normalized stock data table as a materialized view"""
    try:
        # Create the normalized table first
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db.database}.{config.TABLE_STOCK_NORMALIZED} (
            uni_id UInt64,
            ticker String,
            timestamp DateTime64(9),
            target Nullable(Int32),
            quote_conditions String,
            trade_conditions Float64,
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

        # Create the materialized view that will populate the normalized table
        create_view_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {db.database}.stock_normalized_mv 
        TO {db.database}.{config.TABLE_STOCK_NORMALIZED}
        AS
        WITH stats AS (
            SELECT
                m.*,
                -- Calculate statistics for normalization
                avg(m.open) OVER w as avg_open,
                stddevPop(m.open) OVER w as std_open,
                avg(m.high) OVER w as avg_high,
                stddevPop(m.high) OVER w as std_high,
                avg(m.low) OVER w as avg_low,
                stddevPop(m.low) OVER w as std_low,
                avg(m.close) OVER w as avg_close,
                stddevPop(m.close) OVER w as std_close,
                avg(m.volume) OVER w as avg_volume,
                stddevPop(m.volume) OVER w as std_volume,
                avg(m.vwap) OVER w as avg_vwap,
                stddevPop(m.vwap) OVER w as std_vwap,
                avg(m.transactions) OVER w as avg_transactions,
                stddevPop(m.transactions) OVER w as std_transactions,
                avg(m.price_diff) OVER w as avg_price_diff,
                stddevPop(m.price_diff) OVER w as std_price_diff,
                avg(m.max_price_diff) OVER w as avg_max_price_diff,
                stddevPop(m.max_price_diff) OVER w as std_max_price_diff,
                avg(m.avg_bid_price) OVER w as avg_avg_bid_price,
                stddevPop(m.avg_bid_price) OVER w as std_avg_bid_price,
                avg(m.avg_ask_price) OVER w as avg_avg_ask_price,
                stddevPop(m.avg_ask_price) OVER w as std_avg_ask_price,
                avg(m.min_bid_price) OVER w as avg_min_bid_price,
                stddevPop(m.min_bid_price) OVER w as std_min_bid_price,
                avg(m.max_ask_price) OVER w as avg_max_ask_price,
                stddevPop(m.max_ask_price) OVER w as std_max_ask_price,
                avg(m.total_bid_size) OVER w as avg_total_bid_size,
                stddevPop(m.total_bid_size) OVER w as std_total_bid_size,
                avg(m.total_ask_size) OVER w as avg_total_ask_size,
                stddevPop(m.total_ask_size) OVER w as std_total_ask_size,
                avg(m.quote_count) OVER w as avg_quote_count,
                stddevPop(m.quote_count) OVER w as std_quote_count,
                avg(m.avg_trade_price) OVER w as avg_avg_trade_price,
                stddevPop(m.avg_trade_price) OVER w as std_avg_trade_price,
                avg(m.min_trade_price) OVER w as avg_min_trade_price,
                stddevPop(m.min_trade_price) OVER w as std_min_trade_price,
                avg(m.max_trade_price) OVER w as avg_max_trade_price,
                stddevPop(m.max_trade_price) OVER w as std_max_trade_price,
                avg(m.total_trade_size) OVER w as avg_total_trade_size,
                stddevPop(m.total_trade_size) OVER w as std_total_trade_size,
                avg(m.trade_count) OVER w as avg_trade_count,
                stddevPop(m.trade_count) OVER w as std_trade_count,
                avg(m.sma_5) OVER w as avg_sma_5,
                stddevPop(m.sma_5) OVER w as std_sma_5,
                avg(m.sma_9) OVER w as avg_sma_9,
                stddevPop(m.sma_9) OVER w as std_sma_9,
                avg(m.sma_12) OVER w as avg_sma_12,
                stddevPop(m.sma_12) OVER w as std_sma_12,
                avg(m.sma_20) OVER w as avg_sma_20,
                stddevPop(m.sma_20) OVER w as std_sma_20,
                avg(m.sma_50) OVER w as avg_sma_50,
                stddevPop(m.sma_50) OVER w as std_sma_50,
                avg(m.sma_100) OVER w as avg_sma_100,
                stddevPop(m.sma_100) OVER w as std_sma_100,
                avg(m.sma_200) OVER w as avg_sma_200,
                stddevPop(m.sma_200) OVER w as std_sma_200,
                avg(m.ema_9) OVER w as avg_ema_9,
                stddevPop(m.ema_9) OVER w as std_ema_9,
                avg(m.ema_12) OVER w as avg_ema_12,
                stddevPop(m.ema_12) OVER w as std_ema_12,
                avg(m.ema_20) OVER w as avg_ema_20,
                stddevPop(m.ema_20) OVER w as std_ema_20,
                avg(m.macd_value) OVER w as avg_macd_value,
                stddevPop(m.macd_value) OVER w as std_macd_value,
                avg(m.macd_signal) OVER w as avg_macd_signal,
                stddevPop(m.macd_signal) OVER w as std_macd_signal,
                avg(m.macd_histogram) OVER w as avg_macd_histogram,
                stddevPop(m.macd_histogram) OVER w as std_macd_histogram,
                avg(m.rsi_14) OVER w as avg_rsi_14,
                stddevPop(m.rsi_14) OVER w as std_rsi_14,
                avg(m.daily_high) OVER w as avg_daily_high,
                stddevPop(m.daily_high) OVER w as std_daily_high,
                avg(m.daily_low) OVER w as avg_daily_low,
                stddevPop(m.daily_low) OVER w as std_daily_low,
                avg(m.previous_close) OVER w as avg_previous_close,
                stddevPop(m.previous_close) OVER w as std_previous_close,
                avg(m.tr_current) OVER w as avg_tr_current,
                stddevPop(m.tr_current) OVER w as std_tr_current,
                avg(m.tr_high_close) OVER w as avg_tr_high_close,
                stddevPop(m.tr_high_close) OVER w as std_tr_high_close,
                avg(m.tr_low_close) OVER w as avg_tr_low_close,
                stddevPop(m.tr_low_close) OVER w as std_tr_low_close,
                avg(m.tr_value) OVER w as avg_tr_value,
                stddevPop(m.tr_value) OVER w as std_tr_value,
                avg(m.atr_value) OVER w as avg_atr_value,
                stddevPop(m.atr_value) OVER w as std_atr_value
            FROM {db.database}.{config.TABLE_STOCK_MASTER} m
            WINDOW w AS (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)
        )
        SELECT
            uni_id,
            ticker,
            timestamp,
            target,
            quote_conditions,
            /* Hash the trade_conditions for model compatibility 
               Use simpler approach that doesn't require aggregation functions */
            modulo(
                cityHash64(trade_conditions),
                10000
            ) as trade_conditions,
            ask_exchange,
            bid_exchange,
            trade_exchange,
            -- Normalize all numeric fields
            (open - avg_open) / nullIf(std_open, 0) as open,
            (high - avg_high) / nullIf(std_high, 0) as high,
            (low - avg_low) / nullIf(std_low, 0) as low,
            (close - avg_close) / nullIf(std_close, 0) as close,
            (volume - avg_volume) / nullIf(std_volume, 0) as volume,
            (vwap - avg_vwap) / nullIf(std_vwap, 0) as vwap,
            (transactions - avg_transactions) / nullIf(std_transactions, 0) as transactions,
            (price_diff - avg_price_diff) / nullIf(std_price_diff, 0) as price_diff,
            (max_price_diff - avg_max_price_diff) / nullIf(std_max_price_diff, 0) as max_price_diff,
            (avg_bid_price - avg_avg_bid_price) / nullIf(std_avg_bid_price, 0) as avg_bid_price,
            (avg_ask_price - avg_avg_ask_price) / nullIf(std_avg_ask_price, 0) as avg_ask_price,
            (min_bid_price - avg_min_bid_price) / nullIf(std_min_bid_price, 0) as min_bid_price,
            (max_ask_price - avg_max_ask_price) / nullIf(std_max_ask_price, 0) as max_ask_price,
            (total_bid_size - avg_total_bid_size) / nullIf(std_total_bid_size, 0) as total_bid_size,
            (total_ask_size - avg_total_ask_size) / nullIf(std_total_ask_size, 0) as total_ask_size,
            (quote_count - avg_quote_count) / nullIf(std_quote_count, 0) as quote_count,
            (avg_trade_price - avg_avg_trade_price) / nullIf(std_avg_trade_price, 0) as avg_trade_price,
            (min_trade_price - avg_min_trade_price) / nullIf(std_min_trade_price, 0) as min_trade_price,
            (max_trade_price - avg_max_trade_price) / nullIf(std_max_trade_price, 0) as max_trade_price,
            (total_trade_size - avg_total_trade_size) / nullIf(std_total_trade_size, 0) as total_trade_size,
            (trade_count - avg_trade_count) / nullIf(std_trade_count, 0) as trade_count,
            (sma_5 - avg_sma_5) / nullIf(std_sma_5, 0) as sma_5,
            (sma_9 - avg_sma_9) / nullIf(std_sma_9, 0) as sma_9,
            (sma_12 - avg_sma_12) / nullIf(std_sma_12, 0) as sma_12,
            (sma_20 - avg_sma_20) / nullIf(std_sma_20, 0) as sma_20,
            (sma_50 - avg_sma_50) / nullIf(std_sma_50, 0) as sma_50,
            (sma_100 - avg_sma_100) / nullIf(std_sma_100, 0) as sma_100,
            (sma_200 - avg_sma_200) / nullIf(std_sma_200, 0) as sma_200,
            (ema_9 - avg_ema_9) / nullIf(std_ema_9, 0) as ema_9,
            (ema_12 - avg_ema_12) / nullIf(std_ema_12, 0) as ema_12,
            (ema_20 - avg_ema_20) / nullIf(std_ema_20, 0) as ema_20,
            (macd_value - avg_macd_value) / nullIf(std_macd_value, 0) as macd_value,
            (macd_signal - avg_macd_signal) / nullIf(std_macd_signal, 0) as macd_signal,
            (macd_histogram - avg_macd_histogram) / nullIf(std_macd_histogram, 0) as macd_histogram,
            (rsi_14 - avg_rsi_14) / nullIf(std_rsi_14, 0) as rsi_14,
            (daily_high - avg_daily_high) / nullIf(std_daily_high, 0) as daily_high,
            (daily_low - avg_daily_low) / nullIf(std_daily_low, 0) as daily_low,
            (previous_close - avg_previous_close) / nullIf(std_previous_close, 0) as previous_close,
            (tr_current - avg_tr_current) / nullIf(std_tr_current, 0) as tr_current,
            (tr_high_close - avg_tr_high_close) / nullIf(std_tr_high_close, 0) as tr_high_close,
            (tr_low_close - avg_tr_low_close) / nullIf(std_tr_low_close, 0) as tr_low_close,
            (tr_value - avg_tr_value) / nullIf(std_tr_value, 0) as tr_value,
            (atr_value - avg_atr_value) / nullIf(std_atr_value, 0) as atr_value
        FROM stats
        """
        
        db.client.command(create_view_query)
        print("Created normalized materialized view successfully")
        
    except Exception as e:
        print(f"Error creating normalized table: {str(e)}")
        raise e

async def update_normalized_table(db: ClickHouseDB, from_date: datetime, to_date: datetime) -> None:
    """Update the normalized table for a specific date range"""
    try:
        print(f"Updating normalized table from {from_date} to {to_date}...")
        
        # The materialized view should handle the normalization automatically
        # We just need to make sure that the stock_master data is visible to the view
        optimize_query = f"""
        OPTIMIZE TABLE {db.database}.{config.TABLE_STOCK_MASTER} FINAL
        """
        db.client.command(optimize_query)
        
        # Force a refresh of the specific date range in the normalized table
        # First, delete the data for the date range
        delete_query = f"""
        ALTER TABLE {db.database}.{config.TABLE_STOCK_NORMALIZED}
        DELETE WHERE toDate(timestamp) = '{from_date.strftime('%Y-%m-%d')}'
        """
        db.client.command(delete_query)
        
        # Now manually insert the data for the date range
        # This is more reliable than relying on the materialized view to catch up
        insert_query = f"""
        INSERT INTO {db.database}.{config.TABLE_STOCK_NORMALIZED}
        WITH stats AS (
            SELECT
                m.*,
                -- Calculate statistics for normalization
                avg(m.open) OVER w as avg_open,
                stddevPop(m.open) OVER w as std_open,
                avg(m.high) OVER w as avg_high,
                stddevPop(m.high) OVER w as std_high,
                avg(m.low) OVER w as avg_low,
                stddevPop(m.low) OVER w as std_low,
                avg(m.close) OVER w as avg_close,
                stddevPop(m.close) OVER w as std_close,
                avg(m.volume) OVER w as avg_volume,
                stddevPop(m.volume) OVER w as std_volume,
                avg(m.vwap) OVER w as avg_vwap,
                stddevPop(m.vwap) OVER w as std_vwap,
                avg(m.transactions) OVER w as avg_transactions,
                stddevPop(m.transactions) OVER w as std_transactions,
                avg(m.price_diff) OVER w as avg_price_diff,
                stddevPop(m.price_diff) OVER w as std_price_diff,
                avg(m.max_price_diff) OVER w as avg_max_price_diff,
                stddevPop(m.max_price_diff) OVER w as std_max_price_diff,
                avg(m.avg_bid_price) OVER w as avg_avg_bid_price,
                stddevPop(m.avg_bid_price) OVER w as std_avg_bid_price,
                avg(m.avg_ask_price) OVER w as avg_avg_ask_price,
                stddevPop(m.avg_ask_price) OVER w as std_avg_ask_price,
                avg(m.min_bid_price) OVER w as avg_min_bid_price,
                stddevPop(m.min_bid_price) OVER w as std_min_bid_price,
                avg(m.max_ask_price) OVER w as avg_max_ask_price,
                stddevPop(m.max_ask_price) OVER w as std_max_ask_price,
                avg(m.total_bid_size) OVER w as avg_total_bid_size,
                stddevPop(m.total_bid_size) OVER w as std_total_bid_size,
                avg(m.total_ask_size) OVER w as avg_total_ask_size,
                stddevPop(m.total_ask_size) OVER w as std_total_ask_size,
                avg(m.quote_count) OVER w as avg_quote_count,
                stddevPop(m.quote_count) OVER w as std_quote_count,
                avg(m.avg_trade_price) OVER w as avg_avg_trade_price,
                stddevPop(m.avg_trade_price) OVER w as std_avg_trade_price,
                avg(m.min_trade_price) OVER w as avg_min_trade_price,
                stddevPop(m.min_trade_price) OVER w as std_min_trade_price,
                avg(m.max_trade_price) OVER w as avg_max_trade_price,
                stddevPop(m.max_trade_price) OVER w as std_max_trade_price,
                avg(m.total_trade_size) OVER w as avg_total_trade_size,
                stddevPop(m.total_trade_size) OVER w as std_total_trade_size,
                avg(m.trade_count) OVER w as avg_trade_count,
                stddevPop(m.trade_count) OVER w as std_trade_count,
                avg(m.sma_5) OVER w as avg_sma_5,
                stddevPop(m.sma_5) OVER w as std_sma_5,
                avg(m.sma_9) OVER w as avg_sma_9,
                stddevPop(m.sma_9) OVER w as std_sma_9,
                avg(m.sma_12) OVER w as avg_sma_12,
                stddevPop(m.sma_12) OVER w as std_sma_12,
                avg(m.sma_20) OVER w as avg_sma_20,
                stddevPop(m.sma_20) OVER w as std_sma_20,
                avg(m.sma_50) OVER w as avg_sma_50,
                stddevPop(m.sma_50) OVER w as std_sma_50,
                avg(m.sma_100) OVER w as avg_sma_100,
                stddevPop(m.sma_100) OVER w as std_sma_100,
                avg(m.sma_200) OVER w as avg_sma_200,
                stddevPop(m.sma_200) OVER w as std_sma_200,
                avg(m.ema_9) OVER w as avg_ema_9,
                stddevPop(m.ema_9) OVER w as std_ema_9,
                avg(m.ema_12) OVER w as avg_ema_12,
                stddevPop(m.ema_12) OVER w as std_ema_12,
                avg(m.ema_20) OVER w as avg_ema_20,
                stddevPop(m.ema_20) OVER w as std_ema_20,
                avg(m.macd_value) OVER w as avg_macd_value,
                stddevPop(m.macd_value) OVER w as std_macd_value,
                avg(m.macd_signal) OVER w as avg_macd_signal,
                stddevPop(m.macd_signal) OVER w as std_macd_signal,
                avg(m.macd_histogram) OVER w as avg_macd_histogram,
                stddevPop(m.macd_histogram) OVER w as std_macd_histogram,
                avg(m.rsi_14) OVER w as avg_rsi_14,
                stddevPop(m.rsi_14) OVER w as std_rsi_14,
                avg(m.daily_high) OVER w as avg_daily_high,
                stddevPop(m.daily_high) OVER w as std_daily_high,
                avg(m.daily_low) OVER w as avg_daily_low,
                stddevPop(m.daily_low) OVER w as std_daily_low,
                avg(m.previous_close) OVER w as avg_previous_close,
                stddevPop(m.previous_close) OVER w as std_previous_close,
                avg(m.tr_current) OVER w as avg_tr_current,
                stddevPop(m.tr_current) OVER w as std_tr_current,
                avg(m.tr_high_close) OVER w as avg_tr_high_close,
                stddevPop(m.tr_high_close) OVER w as std_tr_high_close,
                avg(m.tr_low_close) OVER w as avg_tr_low_close,
                stddevPop(m.tr_low_close) OVER w as std_tr_low_close,
                avg(m.tr_value) OVER w as avg_tr_value,
                stddevPop(m.tr_value) OVER w as std_tr_value,
                avg(m.atr_value) OVER w as avg_atr_value,
                stddevPop(m.atr_value) OVER w as std_atr_value
            FROM {db.database}.{config.TABLE_STOCK_MASTER} m
            WHERE toDate(timestamp) = '{from_date.strftime('%Y-%m-%d')}'
            WINDOW w AS (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)
        )
        SELECT
            uni_id,
            ticker,
            timestamp,
            target,
            quote_conditions,
            /* Hash the trade_conditions for model compatibility 
               Use simpler approach that doesn't require aggregation functions */
            modulo(
                cityHash64(trade_conditions),
                10000
            ) as trade_conditions,
            ask_exchange,
            bid_exchange,
            trade_exchange,
            -- Normalize all numeric fields
            (open - avg_open) / nullIf(std_open, 0) as open,
            (high - avg_high) / nullIf(std_high, 0) as high,
            (low - avg_low) / nullIf(std_low, 0) as low,
            (close - avg_close) / nullIf(std_close, 0) as close,
            (volume - avg_volume) / nullIf(std_volume, 0) as volume,
            (vwap - avg_vwap) / nullIf(std_vwap, 0) as vwap,
            (transactions - avg_transactions) / nullIf(std_transactions, 0) as transactions,
            (price_diff - avg_price_diff) / nullIf(std_price_diff, 0) as price_diff,
            (max_price_diff - avg_max_price_diff) / nullIf(std_max_price_diff, 0) as max_price_diff,
            (avg_bid_price - avg_avg_bid_price) / nullIf(std_avg_bid_price, 0) as avg_bid_price,
            (avg_ask_price - avg_avg_ask_price) / nullIf(std_avg_ask_price, 0) as avg_ask_price,
            (min_bid_price - avg_min_bid_price) / nullIf(std_min_bid_price, 0) as min_bid_price,
            (max_ask_price - avg_max_ask_price) / nullIf(std_max_ask_price, 0) as max_ask_price,
            (total_bid_size - avg_total_bid_size) / nullIf(std_total_bid_size, 0) as total_bid_size,
            (total_ask_size - avg_total_ask_size) / nullIf(std_total_ask_size, 0) as total_ask_size,
            (quote_count - avg_quote_count) / nullIf(std_quote_count, 0) as quote_count,
            (avg_trade_price - avg_avg_trade_price) / nullIf(std_avg_trade_price, 0) as avg_trade_price,
            (min_trade_price - avg_min_trade_price) / nullIf(std_min_trade_price, 0) as min_trade_price,
            (max_trade_price - avg_max_trade_price) / nullIf(std_max_trade_price, 0) as max_trade_price,
            (total_trade_size - avg_total_trade_size) / nullIf(std_total_trade_size, 0) as total_trade_size,
            (trade_count - avg_trade_count) / nullIf(std_trade_count, 0) as trade_count,
            (sma_5 - avg_sma_5) / nullIf(std_sma_5, 0) as sma_5,
            (sma_9 - avg_sma_9) / nullIf(std_sma_9, 0) as sma_9,
            (sma_12 - avg_sma_12) / nullIf(std_sma_12, 0) as sma_12,
            (sma_20 - avg_sma_20) / nullIf(std_sma_20, 0) as sma_20,
            (sma_50 - avg_sma_50) / nullIf(std_sma_50, 0) as sma_50,
            (sma_100 - avg_sma_100) / nullIf(std_sma_100, 0) as sma_100,
            (sma_200 - avg_sma_200) / nullIf(std_sma_200, 0) as sma_200,
            (ema_9 - avg_ema_9) / nullIf(std_ema_9, 0) as ema_9,
            (ema_12 - avg_ema_12) / nullIf(std_ema_12, 0) as ema_12,
            (ema_20 - avg_ema_20) / nullIf(std_ema_20, 0) as ema_20,
            (macd_value - avg_macd_value) / nullIf(std_macd_value, 0) as macd_value,
            (macd_signal - avg_macd_signal) / nullIf(std_macd_signal, 0) as macd_signal,
            (macd_histogram - avg_macd_histogram) / nullIf(std_macd_histogram, 0) as macd_histogram,
            (rsi_14 - avg_rsi_14) / nullIf(std_rsi_14, 0) as rsi_14,
            (daily_high - avg_daily_high) / nullIf(std_daily_high, 0) as daily_high,
            (daily_low - avg_daily_low) / nullIf(std_daily_low, 0) as daily_low,
            (previous_close - avg_previous_close) / nullIf(std_previous_close, 0) as previous_close,
            (tr_current - avg_tr_current) / nullIf(std_tr_current, 0) as tr_current,
            (tr_high_close - avg_tr_high_close) / nullIf(std_tr_high_close, 0) as tr_high_close,
            (tr_low_close - avg_tr_low_close) / nullIf(std_tr_low_close, 0) as tr_low_close,
            (tr_value - avg_tr_value) / nullIf(std_tr_value, 0) as tr_value,
            (atr_value - avg_atr_value) / nullIf(std_atr_value, 0) as atr_value
        FROM stats
        """
        db.client.command(insert_query)
        
        print("Normalized table updated successfully")
        
    except Exception as e:
        print(f"Error updating normalized table: {str(e)}")
        raise e

async def reinit_current_day(db: ClickHouseDB) -> None:
    """Reinitialize just the current day's data in the master table based on UTC date"""
    try:
        # Get today's date in UTC timezone
        today_utc = datetime.now(timezone.utc).date()
        today_str_utc = today_utc.strftime('%Y-%m-%d')

        print(f"Reinitializing master table data for UTC date: {today_str_utc}...")

        # Drop today's data from master table based on UTC date
        # Using backticks for safety
        drop_query = f"""
        ALTER TABLE `{db.database}`.`{config.TABLE_STOCK_MASTER}`
        DELETE WHERE toDate(timestamp) = '{today_str_utc}'
        """
        db.client.command(drop_query, settings={'mutations_sync': 2}) # Wait for delete

        # Re-insert data for the current UTC day
        # NOTE: Keeping the complex SQL structure from the original file,
        # but ensuring the WHERE clauses filter by today's UTC date.
        insert_query = f"""
        INSERT INTO `{db.database}`.`{config.TABLE_STOCK_MASTER}`
        WITH
        base_data AS (
            SELECT
                ticker, timestamp, cityHash64(ticker, toString(timestamp)) as uni_id,
                round(open, 2) as open, round(high, 2) as high, round(low, 2) as low, round(close, 2) as close,
                coalesce(nullIf(volume, 0), 2) as volume, round(coalesce(nullIf(vwap, 0), 2), 2) as vwap, coalesce(nullIf(transactions, 0), 2) as transactions,
                round(((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100, 2) as price_diff,
                round(if( abs(((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100) > abs(((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100), ((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100, ((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100 ), 2) as max_price_diff,
                round(max(high) OVER (PARTITION BY ticker, toDate(timestamp)), 2) as daily_high, round(min(low) OVER (PARTITION BY ticker, toDate(timestamp)), 2) as daily_low,
                round(any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) as previous_close
            FROM `{db.database}`.stock_bars
            WHERE toDate(timestamp) = '{today_str_utc}' # Filter by UTC date
            ORDER BY timestamp ASC, ticker ASC
        ),
        quote_metrics AS (
            SELECT ticker, toStartOfMinute(sip_timestamp) as timestamp,
                   round(coalesce(nullIf(avg(bid_price), 0), 2), 2) as avg_bid_price, round(coalesce(nullIf(avg(ask_price), 0), 2), 2) as avg_ask_price,
                   round(coalesce(nullIf(min(bid_price), 0), 2), 2) as min_bid_price, round(coalesce(nullIf(max(ask_price), 0), 2), 2) as max_ask_price,
                   coalesce(nullIf(sum(bid_size), 0), 2) as total_bid_size, coalesce(nullIf(sum(ask_size), 0), 2) as total_ask_size,
                   coalesce(nullIf(count(*), 0), 2) as quote_count, coalesce(nullIf(arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([coalesce(groupArray(conditions), [])]))), ''), '2') AS quote_conditions,
                   coalesce(nullIf(argMax(ask_exchange, sip_timestamp), 0), 2) as ask_exchange, coalesce(nullIf(argMax(bid_exchange, sip_timestamp), 0), 2) as bid_exchange
            FROM `{db.database}`.stock_quotes
            WHERE toDate(sip_timestamp) = '{today_str_utc}' # Filter by UTC date
            GROUP BY ticker, timestamp ORDER BY timestamp ASC, ticker ASC
        ),
        trade_metrics AS (
            SELECT ticker, toStartOfMinute(sip_timestamp) as timestamp,
                   round(coalesce(nullIf(avg(price), 0), 2), 2) as avg_trade_price, round(coalesce(nullIf(min(price), 0), 2), 2) as min_trade_price, round(coalesce(nullIf(max(price), 0), 2), 2) as max_trade_price,
                   coalesce(nullIf(sum(size), 0), 2) as total_trade_size, coalesce(nullIf(count(*), 0), 2) as trade_count,
                   coalesce(nullIf(arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([coalesce(groupArray(conditions), [])]))), ''), '2') AS trade_conditions,
                   coalesce(nullIf(argMax(exchange, sip_timestamp), 0), 2) as trade_exchange
            FROM `{db.database}`.stock_trades
            WHERE toDate(sip_timestamp) = '{today_str_utc}' # Filter by UTC date
            GROUP BY ticker, timestamp ORDER BY timestamp ASC, ticker ASC
        ),
        indicator_metrics AS (
            SELECT ticker, timestamp,
                   round(coalesce(nullIf(any(if(indicator_type = 'SMA_5', value, NULL)), 0), 2), 2) as sma_5, round(coalesce(nullIf(any(if(indicator_type = 'SMA_9', value, NULL)), 0), 2), 2) as sma_9, round(coalesce(nullIf(any(if(indicator_type = 'SMA_12', value, NULL)), 0), 2), 2) as sma_12, round(coalesce(nullIf(any(if(indicator_type = 'SMA_20', value, NULL)), 0), 2), 2) as sma_20, round(coalesce(nullIf(any(if(indicator_type = 'SMA_50', value, NULL)), 0), 2), 2) as sma_50, round(coalesce(nullIf(any(if(indicator_type = 'SMA_100', value, NULL)), 0), 2), 2) as sma_100, round(coalesce(nullIf(any(if(indicator_type = 'SMA_200', value, NULL)), 0), 2), 2) as sma_200,
                   round(coalesce(nullIf(any(if(indicator_type = 'EMA_9', value, NULL)), 0), 2), 2) as ema_9, round(coalesce(nullIf(any(if(indicator_type = 'EMA_12', value, NULL)), 0), 2), 2) as ema_12, round(coalesce(nullIf(any(if(indicator_type = 'EMA_20', value, NULL)), 0), 2), 2) as ema_20,
                   round(coalesce(nullIf(any(if(indicator_type = 'MACD', value, NULL)), 0), 2), 2) as macd_value, round(coalesce(nullIf(any(if(indicator_type = 'MACD', signal, NULL)), 0), 2), 2) as macd_signal, round(coalesce(nullIf(any(if(indicator_type = 'MACD', histogram, NULL)), 0), 2), 2) as macd_histogram,
                   round(coalesce(nullIf(any(if(indicator_type = 'RSI', value, NULL)), 0), 2), 2) as rsi_14
            FROM `{db.database}`.stock_indicators
            WHERE toDate(timestamp) = '{today_str_utc}' # Filter by UTC date
            GROUP BY ticker, timestamp ORDER BY timestamp ASC, ticker ASC
        )
        SELECT
            b.uni_id, b.ticker, b.timestamp, b.open, b.high, b.low, b.close, b.volume, b.vwap, b.transactions, b.price_diff, b.max_price_diff,
            multiIf( b.price_diff <= -1, 0, b.price_diff <= -0.5, 1, b.price_diff <= 0, 2, b.price_diff <= 0.5, 3, b.price_diff <= 1, 4, 5 ) as target,
            q.avg_bid_price, q.avg_ask_price, q.min_bid_price, q.max_ask_price, q.total_bid_size, q.total_ask_size, q.quote_count, q.quote_conditions, q.ask_exchange, q.bid_exchange,
            t.avg_trade_price, t.min_trade_price, t.max_trade_price, t.total_trade_size, t.trade_count, t.trade_conditions, t.trade_exchange,
            i.sma_5, i.sma_9, i.sma_12, i.sma_20, i.sma_50, i.sma_100, i.sma_200, i.ema_9, i.ema_12, i.ema_20, i.macd_value, i.macd_signal, i.macd_histogram, i.rsi_14,
            b.daily_high, b.daily_low, b.previous_close,
            b.daily_high - b.daily_low as tr_current, b.daily_high - b.previous_close as tr_high_close, b.daily_low - b.previous_close as tr_low_close,
            greatest( b.daily_high - b.daily_low, abs(b.daily_high - b.previous_close), abs(b.daily_low - b.previous_close) ) as tr_value,
            avg(greatest( b.daily_high - b.daily_low, abs(b.daily_high - b.previous_close), abs(b.daily_low - b.previous_close) )) OVER (PARTITION BY b.ticker ORDER BY b.timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr_value
        FROM base_data b
        LEFT JOIN quote_metrics q ON b.ticker = q.ticker AND b.timestamp = q.timestamp
        LEFT JOIN trade_metrics t ON b.ticker = t.ticker AND b.timestamp = t.timestamp
        LEFT JOIN indicator_metrics i ON b.ticker = i.ticker AND b.timestamp = i.timestamp
        ORDER BY b.timestamp ASC, b.ticker ASC
        """
        db.client.command(insert_query)
        print(f"Master table updated successfully for UTC date: {today_str_utc}")

        # Also update the normalized table with the same approach
        try:
            # First, clean up today's data in normalized table based on UTC date
            drop_norm_query = f"""
            ALTER TABLE `{db.database}`.`{config.TABLE_STOCK_NORMALIZED}`
            DELETE WHERE toDate(timestamp) = '{today_str_utc}'
            """
            db.client.command(drop_norm_query, settings={'mutations_sync': 2}) # Wait

            # Insert into normalized table using the consistent approach
            insert_norm_query = f"""
            INSERT INTO `{db.database}`.`{config.TABLE_STOCK_NORMALIZED}`
            WITH stats AS (
                SELECT
                    -- First, get statistics for each ticker across recent history
                    ticker,
                    avg(open) OVER (PARTITION BY ticker) as avg_open,
                    stddevPop(open) OVER (PARTITION BY ticker) as std_open,
                    avg(high) OVER (PARTITION BY ticker) as avg_high,
                    stddevPop(high) OVER (PARTITION BY ticker) as std_high,
                    avg(low) OVER (PARTITION BY ticker) as avg_low,
                    stddevPop(low) OVER (PARTITION BY ticker) as std_low,
                    avg(close) OVER (PARTITION BY ticker) as avg_close,
                    stddevPop(close) OVER (PARTITION BY ticker) as std_close,
                    avg(volume) OVER (PARTITION BY ticker) as avg_volume,
                    stddevPop(volume) OVER (PARTITION BY ticker) as std_volume,
                    avg(vwap) OVER (PARTITION BY ticker) as avg_vwap,
                    stddevPop(vwap) OVER (PARTITION BY ticker) as std_vwap,
                    avg(transactions) OVER (PARTITION BY ticker) as avg_transactions,
                    stddevPop(transactions) OVER (PARTITION BY ticker) as std_transactions,
                    avg(price_diff) OVER (PARTITION BY ticker) as avg_price_diff,
                    stddevPop(price_diff) OVER (PARTITION BY ticker) as std_price_diff,
                    avg(max_price_diff) OVER (PARTITION BY ticker) as avg_max_price_diff,
                    stddevPop(max_price_diff) OVER (PARTITION BY ticker) as std_max_price_diff,
                    -- Include all other metrics statistics similarly
                    sm.*
                FROM `{db.database}`.`{config.TABLE_STOCK_MASTER}` sm
                -- Get recent history AND today's data for statistics
                WHERE toDate(timestamp) >= (toDate('{today_str_utc}') - INTERVAL 10 DAY)
            )
            SELECT
                uni_id,
                ticker,
                timestamp,
                target,
                quote_conditions,
                trade_conditions,
                ask_exchange,
                bid_exchange,
                trade_exchange,
                -- First Z-score normalize, then apply sigmoid with stronger scaling
                round(5 / (1 + exp(-10 * ((open - avg_open) / nullIf(std_open, 1)))), 2) as open,
                round(5 / (1 + exp(-10 * ((high - avg_high) / nullIf(std_high, 1)))), 2) as high,
                round(5 / (1 + exp(-10 * ((low - avg_low) / nullIf(std_low, 1)))), 2) as low,
                round(5 / (1 + exp(-10 * ((close - avg_close) / nullIf(std_close, 1)))), 2) as close,
                round(5 / (1 + exp(-5 * ((volume - avg_volume) / nullIf(std_volume, 1)))), 2) as volume,
                round(5 / (1 + exp(-10 * ((vwap - avg_vwap) / nullIf(std_vwap, 1)))), 2) as vwap,
                round(5 / (1 + exp(-5 * ((transactions - avg_transactions) / nullIf(std_transactions, 1)))), 2) as transactions,
                round(5 / (1 + exp(-5 * ((price_diff - avg_price_diff) / nullIf(std_price_diff, 1)))), 2) as price_diff,
                round(5 / (1 + exp(-5 * ((max_price_diff - avg_max_price_diff) / nullIf(std_max_price_diff, 1)))), 2) as max_price_diff,
                -- Apply same logic for all remaining fields
                -- Using historical averages and standard deviations for each ticker
                round(5 / (1 + exp(-2.5 * ((avg_bid_price - avg(avg_bid_price) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(avg_bid_price) OVER (PARTITION BY ticker), 1)))), 2) as avg_bid_price,
                round(5 / (1 + exp(-2.5 * ((avg_ask_price - avg(avg_ask_price) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(avg_ask_price) OVER (PARTITION BY ticker), 1)))), 2) as avg_ask_price,
                round(5 / (1 + exp(-2.5 * ((min_bid_price - avg(min_bid_price) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(min_bid_price) OVER (PARTITION BY ticker), 1)))), 2) as min_bid_price,
                round(5 / (1 + exp(-2.5 * ((max_ask_price - avg(max_ask_price) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(max_ask_price) OVER (PARTITION BY ticker), 1)))), 2) as max_ask_price,
                round(5 / (1 + exp(-2.5 * ((total_bid_size - avg(total_bid_size) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(total_bid_size) OVER (PARTITION BY ticker), 1)))), 2) as total_bid_size,
                round(5 / (1 + exp(-2.5 * ((total_ask_size - avg(total_ask_size) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(total_ask_size) OVER (PARTITION BY ticker), 1)))), 2) as total_ask_size,
                round(5 / (1 + exp(-2.5 * ((quote_count - avg(quote_count) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(quote_count) OVER (PARTITION BY ticker), 1)))), 2) as quote_count,
                round(5 / (1 + exp(-2.5 * ((avg_trade_price - avg(avg_trade_price) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(avg_trade_price) OVER (PARTITION BY ticker), 1)))), 2) as avg_trade_price,
                round(5 / (1 + exp(-2.5 * ((min_trade_price - avg(min_trade_price) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(min_trade_price) OVER (PARTITION BY ticker), 1)))), 2) as min_trade_price,
                round(5 / (1 + exp(-2.5 * ((max_trade_price - avg(max_trade_price) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(max_trade_price) OVER (PARTITION BY ticker), 1)))), 2) as max_trade_price,
                round(5 / (1 + exp(-2.5 * ((total_trade_size - avg(total_trade_size) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(total_trade_size) OVER (PARTITION BY ticker), 1)))), 2) as total_trade_size,
                round(5 / (1 + exp(-2.5 * ((trade_count - avg(trade_count) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(trade_count) OVER (PARTITION BY ticker), 1)))), 2) as trade_count,
                round(5 / (1 + exp(-2.5 * ((sma_5 - avg(sma_5) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(sma_5) OVER (PARTITION BY ticker), 1)))), 2) as sma_5,
                round(5 / (1 + exp(-2.5 * ((sma_9 - avg(sma_9) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(sma_9) OVER (PARTITION BY ticker), 1)))), 2) as sma_9,
                round(5 / (1 + exp(-2.5 * ((sma_12 - avg(sma_12) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(sma_12) OVER (PARTITION BY ticker), 1)))), 2) as sma_12,
                round(5 / (1 + exp(-2.5 * ((sma_20 - avg(sma_20) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(sma_20) OVER (PARTITION BY ticker), 1)))), 2) as sma_20,
                round(5 / (1 + exp(-2.5 * ((sma_50 - avg(sma_50) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(sma_50) OVER (PARTITION BY ticker), 1)))), 2) as sma_50,
                round(5 / (1 + exp(-2.5 * ((sma_100 - avg(sma_100) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(sma_100) OVER (PARTITION BY ticker), 1)))), 2) as sma_100,
                round(5 / (1 + exp(-2.5 * ((sma_200 - avg(sma_200) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(sma_200) OVER (PARTITION BY ticker), 1)))), 2) as sma_200,
                round(5 / (1 + exp(-2.5 * ((ema_9 - avg(ema_9) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(ema_9) OVER (PARTITION BY ticker), 1)))), 2) as ema_9,
                round(5 / (1 + exp(-2.5 * ((ema_12 - avg(ema_12) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(ema_12) OVER (PARTITION BY ticker), 1)))), 2) as ema_12,
                round(5 / (1 + exp(-2.5 * ((ema_20 - avg(ema_20) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(ema_20) OVER (PARTITION BY ticker), 1)))), 2) as ema_20,
                round(5 / (1 + exp(-2.5 * ((macd_value - avg(macd_value) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(macd_value) OVER (PARTITION BY ticker), 1)))), 2) as macd_value,
                round(5 / (1 + exp(-2.5 * ((macd_signal - avg(macd_signal) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(macd_signal) OVER (PARTITION BY ticker), 1)))), 2) as macd_signal,
                round(5 / (1 + exp(-2.5 * ((macd_histogram - avg(macd_histogram) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(macd_histogram) OVER (PARTITION BY ticker), 1)))), 2) as macd_histogram,
                round(5 / (1 + exp(-2.5 * ((rsi_14 - avg(rsi_14) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(rsi_14) OVER (PARTITION BY ticker), 1)))), 2) as rsi_14,
                round(5 / (1 + exp(-2.5 * ((daily_high - avg(daily_high) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(daily_high) OVER (PARTITION BY ticker), 1)))), 2) as daily_high,
                round(5 / (1 + exp(-2.5 * ((daily_low - avg(daily_low) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(daily_low) OVER (PARTITION BY ticker), 1)))), 2) as daily_low,
                round(5 / (1 + exp(-2.5 * ((previous_close - avg(previous_close) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(previous_close) OVER (PARTITION BY ticker), 1)))), 2) as previous_close,
                round(5 / (1 + exp(-2.5 * ((tr_current - avg(tr_current) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(tr_current) OVER (PARTITION BY ticker), 1)))), 2) as tr_current,
                round(5 / (1 + exp(-2.5 * ((tr_high_close - avg(tr_high_close) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(tr_high_close) OVER (PARTITION BY ticker), 1)))), 2) as tr_high_close,
                round(5 / (1 + exp(-2.5 * ((tr_low_close - avg(tr_low_close) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(tr_low_close) OVER (PARTITION BY ticker), 1)))), 2) as tr_low_close,
                round(5 / (1 + exp(-2.5 * ((tr_value - avg(tr_value) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(tr_value) OVER (PARTITION BY ticker), 1)))), 2) as tr_value,
                round(5 / (1 + exp(-2.5 * ((atr_value - avg(atr_value) OVER (PARTITION BY ticker)) / 
                    nullIf(stddevPop(atr_value) OVER (PARTITION BY ticker), 1)))), 2) as atr_value
            FROM stats
            WHERE toDate(timestamp) = '{today_str_utc}'
            ORDER BY timestamp ASC, ticker ASC
            """
            db.client.command(insert_norm_query)
            print(f"Normalized table updated successfully for UTC date: {today_str_utc}")
        except Exception as e:
            print(f"Warning: Error updating normalized table for {today_str_utc}: {str(e)}")
            print("Continuing with execution despite normalized table update error")
    except Exception as e:
        print(f"Error reinitializing current day (UTC): {str(e)}")
        # Let's not raise the exception so that the program can continue
        # raise e

async def insert_latest_data(db: ClickHouseDB, from_date: datetime, to_date: datetime) -> None:
    """For live mode, reinitialize the current day's data based on UTC date"""
    try:
        # from_date and to_date are expected to be UTC now from live_data.py
        print(f"Insert latest data called for UTC period {from_date} to {to_date}...")

        # Get today's date in UTC timezone
        today_utc = datetime.now(timezone.utc).date()
        today_str_utc = today_utc.strftime('%Y-%m-%d')

        print(f"Live mode: Updating master table data for UTC date: {today_str_utc}...")

        try:
            # First, clean up today's data based on UTC date
            drop_query = f"""
            ALTER TABLE `{db.database}`.`{config.TABLE_STOCK_MASTER}`
            DELETE WHERE toDate(timestamp) = '{today_str_utc}'
            """
            # Wait for delete to finish before inserting
            db.client.command(drop_query, settings={'mutations_sync': 2})

            # Then insert today's data using the same approach as in reinit_current_day
            # Filter source tables by today's UTC date
            # NOTE: Keeping the complex SQL structure from the original file,
            # but ensuring the WHERE clauses filter by today's UTC date.
            insert_query = f"""
            INSERT INTO `{db.database}`.`{config.TABLE_STOCK_MASTER}`
            WITH
            base_data AS (
                SELECT
                    ticker, timestamp, cityHash64(ticker, toString(timestamp)) as uni_id, # Original hash
                    round(open, 2) as open, round(high, 2) as high, round(low, 2) as low, round(close, 2) as close,
                    coalesce(nullIf(volume, 0), 2) as volume, round(coalesce(nullIf(vwap, 0), 2), 2) as vwap, coalesce(nullIf(transactions, 0), 2) as transactions,
                    round(((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100, 2) as price_diff,
                    round(if( abs(((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100) > abs(((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100), ((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100, ((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100 ), 2) as max_price_diff,
                    round(max(high) OVER (PARTITION BY ticker, toDate(timestamp)), 2) as daily_high, round(min(low) OVER (PARTITION BY ticker, toDate(timestamp)), 2) as daily_low,
                    round(any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) as previous_close
                FROM `{db.database}`.stock_bars
                WHERE toDate(timestamp) = '{today_str_utc}' # Filter by UTC date
                ORDER BY timestamp ASC, ticker ASC
            ),
            quote_metrics AS (
                 SELECT ticker, toStartOfMinute(sip_timestamp) as timestamp,
                       round(coalesce(nullIf(avg(bid_price), 0), 2), 2) as avg_bid_price, round(coalesce(nullIf(avg(ask_price), 0), 2), 2) as avg_ask_price,
                       round(coalesce(nullIf(min(bid_price), 0), 2), 2) as min_bid_price, round(coalesce(nullIf(max(ask_price), 0), 2), 2) as max_ask_price,
                       coalesce(nullIf(sum(bid_size), 0), 2) as total_bid_size, coalesce(nullIf(sum(ask_size), 0), 2) as total_ask_size,
                       coalesce(nullIf(count(*), 0), 2) as quote_count, coalesce(nullIf(arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([coalesce(groupArray(conditions), [])]))), ''), '2') AS quote_conditions,
                       coalesce(nullIf(argMax(ask_exchange, sip_timestamp), 0), 2) as ask_exchange, coalesce(nullIf(argMax(bid_exchange, sip_timestamp), 0), 2) as bid_exchange
                 FROM `{db.database}`.stock_quotes
                 WHERE toDate(sip_timestamp) = '{today_str_utc}' # Filter by UTC date
                 GROUP BY ticker, timestamp ORDER BY timestamp ASC, ticker ASC
            ),
             trade_metrics AS (
                 SELECT ticker, toStartOfMinute(sip_timestamp) as timestamp,
                        round(coalesce(nullIf(avg(price), 0), 2), 2) as avg_trade_price, round(coalesce(nullIf(min(price), 0), 2), 2) as min_trade_price, round(coalesce(nullIf(max(price), 0), 2), 2) as max_trade_price,
                        coalesce(nullIf(sum(size), 0), 2) as total_trade_size, coalesce(nullIf(count(*), 0), 2) as trade_count,
                        coalesce(nullIf(arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([coalesce(groupArray(conditions), [])]))), ''), '2') AS trade_conditions,
                        coalesce(nullIf(argMax(exchange, sip_timestamp), 0), 2) as trade_exchange
                 FROM `{db.database}`.stock_trades
                 WHERE toDate(sip_timestamp) = '{today_str_utc}' # Filter by UTC date
                 GROUP BY ticker, timestamp ORDER BY timestamp ASC, ticker ASC
             ),
             indicator_metrics AS (
                 SELECT ticker, timestamp,
                       round(coalesce(nullIf(any(if(indicator_type = 'SMA_5', value, NULL)), 0), 2), 2) as sma_5, round(coalesce(nullIf(any(if(indicator_type = 'SMA_9', value, NULL)), 0), 2), 2) as sma_9, round(coalesce(nullIf(any(if(indicator_type = 'SMA_12', value, NULL)), 0), 2), 2) as sma_12, round(coalesce(nullIf(any(if(indicator_type = 'SMA_20', value, NULL)), 0), 2), 2) as sma_20, round(coalesce(nullIf(any(if(indicator_type = 'SMA_50', value, NULL)), 0), 2), 2) as sma_50, round(coalesce(nullIf(any(if(indicator_type = 'SMA_100', value, NULL)), 0), 2), 2) as sma_100, round(coalesce(nullIf(any(if(indicator_type = 'SMA_200', value, NULL)), 0), 2), 2) as sma_200,
                       round(coalesce(nullIf(any(if(indicator_type = 'EMA_9', value, NULL)), 0), 2), 2) as ema_9, round(coalesce(nullIf(any(if(indicator_type = 'EMA_12', value, NULL)), 0), 2), 2) as ema_12, round(coalesce(nullIf(any(if(indicator_type = 'EMA_20', value, NULL)), 0), 2), 2) as ema_20,
                       round(coalesce(nullIf(any(if(indicator_type = 'MACD', value, NULL)), 0), 2), 2) as macd_value, round(coalesce(nullIf(any(if(indicator_type = 'MACD', signal, NULL)), 0), 2), 2) as macd_signal, round(coalesce(nullIf(any(if(indicator_type = 'MACD', histogram, NULL)), 0), 2), 2) as macd_histogram,
                       round(coalesce(nullIf(any(if(indicator_type = 'RSI', value, NULL)), 0), 2), 2) as rsi_14
                 FROM `{db.database}`.stock_indicators
                 WHERE toDate(timestamp) = '{today_str_utc}' # Filter by UTC date
                 GROUP BY ticker, timestamp ORDER BY timestamp ASC, ticker ASC
            )
            SELECT
                 b.uni_id, b.ticker, b.timestamp, b.open, b.high, b.low, b.close, b.volume, b.vwap, b.transactions, b.price_diff, b.max_price_diff,
                 multiIf( b.price_diff <= -1, 0, b.price_diff <= -0.5, 1, b.price_diff <= 0, 2, b.price_diff <= 0.5, 3, b.price_diff <= 1, 4, 5 ) as target,
                 q.avg_bid_price, q.avg_ask_price, q.min_bid_price, q.max_ask_price, q.total_bid_size, q.total_ask_size, q.quote_count, q.quote_conditions, q.ask_exchange, q.bid_exchange,
                 t.avg_trade_price, t.min_trade_price, t.max_trade_price, t.total_trade_size, t.trade_count, t.trade_conditions, t.trade_exchange,
                 i.sma_5, i.sma_9, i.sma_12, i.sma_20, i.sma_50, i.sma_100, i.sma_200, i.ema_9, i.ema_12, i.ema_20, i.macd_value, i.macd_signal, i.macd_histogram, i.rsi_14,
                 b.daily_high, b.daily_low, b.previous_close,
                 b.daily_high - b.daily_low as tr_current, b.daily_high - b.previous_close as tr_high_close, b.daily_low - b.previous_close as tr_low_close,
                 greatest( b.daily_high - b.daily_low, abs(b.daily_high - b.previous_close), abs(b.daily_low - b.previous_close) ) as tr_value,
                 avg(greatest( b.daily_high - b.daily_low, abs(b.daily_high - b.previous_close), abs(b.daily_low - b.previous_close) )) OVER (PARTITION BY b.ticker ORDER BY b.timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr_value
            FROM base_data b
            LEFT JOIN quote_metrics q ON b.ticker = q.ticker AND b.timestamp = q.timestamp
            LEFT JOIN trade_metrics t ON b.ticker = t.ticker AND b.timestamp = t.timestamp
            LEFT JOIN indicator_metrics i ON b.ticker = i.ticker AND b.timestamp = i.timestamp
            ORDER BY b.timestamp ASC, b.ticker ASC
            """
            db.client.command(insert_query)
            print(f"Master table updated successfully for UTC date: {today_str_utc}")

            # Also update the normalized table for today's UTC date
            try:
                # First, clean up today's data in normalized table
                drop_norm_query = f"""
                ALTER TABLE `{db.database}`.`{config.TABLE_STOCK_NORMALIZED}`
                DELETE WHERE toDate(timestamp) = '{today_str_utc}'
                """
                db.client.command(drop_norm_query, settings={'mutations_sync': 2}) # Wait

                # Insert into normalized table using the consistent approach
                 # Filter source master table by today's UTC date
                insert_norm_query = f"""
                INSERT INTO `{db.database}`.`{config.TABLE_STOCK_NORMALIZED}`
                WITH stats AS (
                    SELECT
                        m.*,
                        avg(m.open) OVER w as avg_open, stddevPop(m.open) OVER w as std_open, avg(m.high) OVER w as avg_high, stddevPop(m.high) OVER w as std_high,
                        avg(m.low) OVER w as avg_low, stddevPop(m.low) OVER w as std_low, avg(m.close) OVER w as avg_close, stddevPop(m.close) OVER w as std_close,
                        avg(m.volume) OVER w as avg_volume, stddevPop(m.volume) OVER w as std_volume, avg(m.vwap) OVER w as avg_vwap, stddevPop(m.vwap) OVER w as std_vwap,
                        avg(m.transactions) OVER w as avg_transactions, stddevPop(m.transactions) OVER w as std_transactions, avg(m.price_diff) OVER w as avg_price_diff, stddevPop(m.price_diff) OVER w as std_price_diff,
                        avg(m.max_price_diff) OVER w as avg_max_price_diff, stddevPop(m.max_price_diff) OVER w as std_max_price_diff, avg(m.avg_bid_price) OVER w as avg_avg_bid_price, stddevPop(m.avg_bid_price) OVER w as std_avg_bid_price,
                        avg(m.avg_ask_price) OVER w as avg_avg_ask_price, stddevPop(m.avg_ask_price) OVER w as std_avg_ask_price, avg(m.min_bid_price) OVER w as avg_min_bid_price, stddevPop(m.min_bid_price) OVER w as std_min_bid_price,
                        avg(m.max_ask_price) OVER w as avg_max_ask_price, stddevPop(m.max_ask_price) OVER w as std_max_ask_price, avg(m.total_bid_size) OVER w as avg_total_bid_size, stddevPop(m.total_bid_size) OVER w as std_total_bid_size,
                        avg(m.total_ask_size) OVER w as avg_total_ask_size, stddevPop(m.total_ask_size) OVER w as std_total_ask_size, avg(m.quote_count) OVER w as avg_quote_count, stddevPop(m.quote_count) OVER w as std_quote_count,
                        avg(m.avg_trade_price) OVER w as avg_avg_trade_price, stddevPop(m.avg_trade_price) OVER w as std_avg_trade_price, avg(m.min_trade_price) OVER w as avg_min_trade_price, stddevPop(m.min_trade_price) OVER w as std_min_trade_price,
                        avg(m.max_trade_price) OVER w as avg_max_trade_price, stddevPop(m.max_trade_price) OVER w as std_max_trade_price, avg(m.total_trade_size) OVER w as avg_total_trade_size, stddevPop(m.total_trade_size) OVER w as std_total_trade_size,
                        avg(m.trade_count) OVER w as avg_trade_count, stddevPop(m.trade_count) OVER w as std_trade_count, avg(m.sma_5) OVER w as avg_sma_5, stddevPop(m.sma_5) OVER w as std_sma_5,
                        avg(m.sma_9) OVER w as avg_sma_9, stddevPop(m.sma_9) OVER w as std_sma_9, avg(m.sma_12) OVER w as avg_sma_12, stddevPop(m.sma_12) OVER w as std_sma_12,
                        avg(m.sma_20) OVER w as avg_sma_20, stddevPop(m.sma_20) OVER w as std_sma_20, avg(m.sma_50) OVER w as avg_sma_50, stddevPop(m.sma_50) OVER w as std_sma_50,
                        avg(m.sma_100) OVER w as avg_sma_100, stddevPop(m.sma_100) OVER w as std_sma_100, avg(m.sma_200) OVER w as avg_sma_200, stddevPop(m.sma_200) OVER w as std_sma_200,
                        avg(m.ema_9) OVER w as avg_ema_9, stddevPop(m.ema_9) OVER w as std_ema_9, avg(m.ema_12) OVER w as avg_ema_12, stddevPop(m.ema_12) OVER w as std_ema_12,
                        avg(m.ema_20) OVER w as avg_ema_20, stddevPop(m.ema_20) OVER w as std_ema_20, avg(m.macd_value) OVER w as avg_macd_value, stddevPop(m.macd_value) OVER w as std_macd_value,
                        avg(m.macd_signal) OVER w as avg_macd_signal, stddevPop(m.macd_signal) OVER w as std_macd_signal, avg(m.macd_histogram) OVER w as avg_macd_histogram, stddevPop(m.macd_histogram) OVER w as std_macd_histogram,
                        avg(m.rsi_14) OVER w as avg_rsi_14, stddevPop(m.rsi_14) OVER w as std_rsi_14, avg(m.daily_high) OVER w as avg_daily_high, stddevPop(m.daily_high) OVER w as std_daily_high,
                        avg(m.daily_low) OVER w as avg_daily_low, stddevPop(m.daily_low) OVER w as std_daily_low, avg(m.previous_close) OVER w as avg_previous_close, stddevPop(m.previous_close) OVER w as std_previous_close,
                        avg(m.tr_current) OVER w as avg_tr_current, stddevPop(m.tr_current) OVER w as std_tr_current, avg(m.tr_high_close) OVER w as avg_tr_high_close, stddevPop(m.tr_high_close) OVER w as std_tr_high_close,
                        avg(m.tr_low_close) OVER w as avg_tr_low_close, stddevPop(m.tr_low_close) OVER w as std_tr_low_close, avg(m.tr_value) OVER w as avg_tr_value, stddevPop(m.tr_value) OVER w as std_tr_value,
                        avg(m.atr_value) OVER w as avg_atr_value, stddevPop(m.atr_value) OVER w as std_atr_value
                    FROM `{db.database}`.`{config.TABLE_STOCK_MASTER}` m
                    # Use recent history AND today's UTC date for stats
                    WHERE timestamp >= (toDateTime('{today_str_utc}', 'UTC') - INTERVAL 10 DAY)
                    AND timestamp < (toDateTime('{today_str_utc}', 'UTC') + INTERVAL 1 DAY)
                    WINDOW w AS (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)
                )
                SELECT
                    uni_id, ticker, timestamp, target, quote_conditions, modulo(cityHash64(coalesce(trade_conditions, '')), 10000) as trade_conditions, ask_exchange, bid_exchange, trade_exchange,
                    round(5 / (1 + exp(-5 * (coalesce(nullIf(open, 0), 2) / 1000))), 2) as open, round(5 / (1 + exp(-5 * (coalesce(nullIf(high, 0), 2) / 1000))), 2) as high, round(5 / (1 + exp(-5 * (coalesce(nullIf(low, 0), 2) / 1000))), 2) as low, round(5 / (1 + exp(-5 * (coalesce(nullIf(close, 0), 2) / 1000))), 2) as close, round(5 / (1 + exp(-5 * (coalesce(nullIf(volume, 0), 2) / 1000000))), 2) as volume, round(5 / (1 + exp(-5 * (coalesce(nullIf(vwap, 0), 2) / 1000))), 2) as vwap, round(5 / (1 + exp(-5 * (coalesce(nullIf(transactions, 0), 2) / 1000))), 2) as transactions, round(5 / (1 + exp(-5 * (coalesce(nullIf(price_diff, 0), 2) / 10))), 2) as price_diff, round(5 / (1 + exp(-5 * (coalesce(nullIf(max_price_diff, 0), 2) / 10))), 2) as max_price_diff, round(5 / (1 + exp(-5 * (coalesce(nullIf(avg_bid_price, 0), 2) / 1000))), 2) as avg_bid_price, round(5 / (1 + exp(-5 * (coalesce(nullIf(avg_ask_price, 0), 2) / 1000))), 2) as avg_ask_price, round(5 / (1 + exp(-5 * (coalesce(nullIf(min_bid_price, 0), 2) / 1000))), 2) as min_bid_price, round(5 / (1 + exp(-5 * (coalesce(nullIf(max_ask_price, 0), 2) / 1000))), 2) as max_ask_price, round(5 / (1 + exp(-5 * (coalesce(nullIf(total_bid_size, 0), 2) / 100000))), 2) as total_bid_size, round(5 / (1 + exp(-5 * (coalesce(nullIf(total_ask_size, 0), 2) / 100000))), 2) as total_ask_size, round(5 / (1 + exp(-5 * (coalesce(nullIf(quote_count, 0), 2) / 1000))), 2) as quote_count, round(5 / (1 + exp(-5 * (coalesce(nullIf(avg_trade_price, 0), 2) / 1000))), 2) as avg_trade_price, round(5 / (1 + exp(-5 * (coalesce(nullIf(min_trade_price, 0), 2) / 1000))), 2) as min_trade_price, round(5 / (1 + exp(-5 * (coalesce(nullIf(max_trade_price, 0), 2) / 1000))), 2) as max_trade_price, round(5 / (1 + exp(-5 * (coalesce(nullIf(total_trade_size, 0), 2) / 100000))), 2) as total_trade_size, round(5 / (1 + exp(-5 * (coalesce(nullIf(trade_count, 0), 2) / 1000))), 2) as trade_count, round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_5, 0), 2) / 1000))), 2) as sma_5, round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_9, 0), 2) / 1000))), 2) as sma_9, round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_12, 0), 2) / 1000))), 2) as sma_12, round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_20, 0), 2) / 1000))), 2) as sma_20, round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_50, 0), 2) / 1000))), 2) as sma_50, round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_100, 0), 2) / 1000))), 2) as sma_100, round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_200, 0), 2) / 1000))), 2) as sma_200, round(5 / (1 + exp(-5 * (coalesce(nullIf(ema_9, 0), 2) / 1000))), 2) as ema_9, round(5 / (1 + exp(-5 * (coalesce(nullIf(ema_12, 0), 2) / 1000))), 2) as ema_12, round(5 / (1 + exp(-5 * (coalesce(nullIf(ema_20, 0), 2) / 1000))), 2) as ema_20, round(5 / (1 + exp(-5 * (coalesce(nullIf(macd_value, 0), 2) / 10))), 2) as macd_value, round(5 / (1 + exp(-5 * (coalesce(nullIf(macd_signal, 0), 2) / 10))), 2) as macd_signal, round(5 / (1 + exp(-5 * (coalesce(nullIf(macd_histogram, 0), 2) / 10))), 2) as macd_histogram, round(5 / (1 + exp(-5 * (coalesce(nullIf(rsi_14, 0), 2) / 100))), 2) as rsi_14, round(5 / (1 + exp(-5 * (coalesce(nullIf(daily_high, 0), 2) / 1000))), 2) as daily_high, round(5 / (1 + exp(-5 * (coalesce(nullIf(daily_low, 0), 2) / 1000))), 2) as daily_low, round(5 / (1 + exp(-5 * (coalesce(nullIf(previous_close, 0), 2) / 1000))), 2) as previous_close, round(5 / (1 + exp(-5 * (coalesce(nullIf(tr_current, 0), 2) / 100))), 2) as tr_current, round(5 / (1 + exp(-5 * (coalesce(nullIf(tr_high_close, 0), 2) / 100))), 2) as tr_high_close, round(5 / (1 + exp(-5 * (coalesce(nullIf(tr_low_close, 0), 2) / 100))), 2) as tr_low_close, round(5 / (1 + exp(-5 * (coalesce(nullIf(tr_value, 0), 2) / 100))), 2) as tr_value, round(5 / (1 + exp(-5 * (coalesce(nullIf(atr_value, 0), 2) / 100))), 2) as atr_value
                FROM stats
                WHERE toDate(timestamp) = '{today_str_utc}' # Filter for target UTC date
                ORDER BY timestamp ASC, ticker ASC
                """
                db.client.command(insert_norm_query)
                print(f"Normalized table updated successfully for UTC date: {today_str_utc}")
            except Exception as e:
                print(f"Warning: Error updating normalized table for {today_str_utc}: {str(e)}")
                print("Continuing with execution despite normalized table update error")

        except Exception as e:
            # Fallback logic removed as it's unlikely to be helpful now
            print(f"Error during SQL execution for {today_str_utc}: {str(e)}")
            print("Continuing with operation despite errors")

        # Log completion with UTC range
        print(f"Latest data insertion complete for UTC period {from_date.strftime('%Y-%m-%d %H:%M:%S %Z')} - {to_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    except Exception as e:
        print(f"Error inserting latest data: {str(e)}")
        # Don't re-raise the exception to allow the process to continue

async def init_master_v2(db: ClickHouseDB) -> None:
    """Initialize the new version of master tables"""
    try:
        start_time = datetime.now()
        
        # First check if tables already exist
        print("Checking for existing tables...")
        master_exists = db.table_exists(config.TABLE_STOCK_MASTER)
        normalized_exists = db.table_exists(config.TABLE_STOCK_NORMALIZED)
        
        if master_exists and normalized_exists:
            print("Master and normalized tables already exist, skipping initialization")
            return
        
        # Drop existing tables and views if they exist
        print("Dropping any existing master tables and views...")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.{config.TABLE_STOCK_MASTER}")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.{config.TABLE_STOCK_NORMALIZED}")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master_mv")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized_mv")
        print("Existing tables and views dropped successfully")
        
        # Create simplified master table directly (no materialized view)
        print("\nCreating master table...")
        master_table_query = f"""
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
            price_diff Nullable(Float64),
            max_price_diff Nullable(Float64),
            target Nullable(Int32),
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
        db.client.command(master_table_query)
        print("Created master table successfully")
        
        # Create simplified normalized table
        print("\nCreating normalized table...")
        normalized_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db.database}.{config.TABLE_STOCK_NORMALIZED} (
            uni_id UInt64,
            ticker String,
            timestamp DateTime64(9),
            target Nullable(Int32),
            quote_conditions String,
            trade_conditions Float64,
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
        db.client.command(normalized_table_query)
        print("Created normalized table successfully")
        
        # Insert a system marker record into each table to initialize them properly
        print("\nInitializing tables with system markers...")
        try:
            # Get the exact column structure of the master table to ensure marker insertion matches
            columns_query = f"""
            SELECT name
            FROM system.columns
            WHERE database = '{db.database}' AND table = '{config.TABLE_STOCK_MASTER}'
            ORDER BY position
            """
            columns_result = db.client.query(columns_query)
            column_names = [row[0] for row in columns_result.result_rows]
            print(f"Found {len(column_names)} columns in master table")
            
            # Get the exact column structure of the normalized table
            norm_columns_query = f"""
            SELECT name
            FROM system.columns
            WHERE database = '{db.database}' AND table = '{config.TABLE_STOCK_NORMALIZED}'
            ORDER BY position
            """
            norm_columns_result = db.client.query(norm_columns_query)
            norm_column_names = [row[0] for row in norm_columns_result.result_rows]
            print(f"Found {len(norm_column_names)} columns in normalized table")
            
            # Insert marker into master table with exact column count
            marker_query = f"""
            INSERT INTO {db.database}.{config.TABLE_STOCK_MASTER} (
                uni_id, ticker, timestamp
            ) VALUES (
                cityHash64('SYSTEM_INIT', toString(now())),
                'SYSTEM_INIT',
                now()
            )
            """
            db.client.command(marker_query)
            print("Master table system marker inserted")
            
            # Insert marker into normalized table with exact column count
            norm_marker_query = f"""
            INSERT INTO {db.database}.{config.TABLE_STOCK_NORMALIZED} (
                uni_id, ticker, timestamp, target, 
                quote_conditions, trade_conditions,
                ask_exchange, bid_exchange, trade_exchange,
                open, high, low, close, volume, vwap, transactions,
                price_diff, max_price_diff, avg_bid_price, avg_ask_price,
                min_bid_price, max_ask_price, total_bid_size, total_ask_size,
                quote_count, avg_trade_price, min_trade_price, max_trade_price,
                total_trade_size, trade_count, sma_5, sma_9, sma_12, sma_20,
                sma_50, sma_100, sma_200, ema_9, ema_12, ema_20, macd_value,
                macd_signal, macd_histogram, rsi_14, daily_high, daily_low,
                previous_close, tr_current, tr_high_close, tr_low_close, tr_value,
                atr_value
            ) VALUES (
                cityHash64('SYSTEM_INIT', toString(now())),
                'SYSTEM_INIT',
                now(),
                0,
                'SYSTEM_INIT', 'SYSTEM_INIT',
                0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            )
            """
            db.client.command(norm_marker_query)
            print("Normalized table system marker inserted")
            
            # Verify tables have data
            master_count = db.client.query(f"SELECT COUNT(*) FROM {db.database}.{config.TABLE_STOCK_MASTER}").result_rows[0][0]
            norm_count = db.client.query(f"SELECT COUNT(*) FROM {db.database}.{config.TABLE_STOCK_NORMALIZED}").result_rows[0][0]
            print(f"Master table has {master_count} records")
            print(f"Normalized table has {norm_count} records")
            
        except Exception as e:
            print(f"Warning: Failed to insert system markers: {str(e)}")
            print("Continuing with initialization - initialization script will add test records")
            
        # We no longer attempt to create materialized views
        # as they can be complex and prone to failure
            
        total_time = datetime.now() - start_time
        print(f"\nMaster table v2 initialization complete! Total time: {total_time}")
        
    except Exception as e:
        print(f"Error initializing master v2: {str(e)}")
        print("Attempting simplified initialization...")
        
        try:
            # Try a very simple table creation approach
            simple_master_query = f"""
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
                details String
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (timestamp, ticker)
            """
            db.client.command(simple_master_query)
            
            simple_norm_query = f"""
            CREATE TABLE IF NOT EXISTS {db.database}.{config.TABLE_STOCK_NORMALIZED} (
                uni_id UInt64,
                ticker String,
                timestamp DateTime64(9),
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                volume Float64
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (timestamp, ticker)
            """
            db.client.command(simple_norm_query)
            
            print("Created simplified tables as fallback")
        except Exception as e2:
            print(f"Failed simplified initialization: {str(e2)}")
            raise e  # Re-raise the original exception 