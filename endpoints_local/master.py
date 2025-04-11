import asyncio
from datetime import datetime, timedelta
from typing import Dict, List

from endpoints.db import ClickHouseDB
from endpoints import config

# Schema for master stock data table
MASTER_SCHEMA = {
    # Common fields
    "uni_id": "UInt64",  # Unique ID based on ticker and timestamp
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
    "max_price_diff": "Nullable(Float64)",  # Maximum absolute percentage difference within 15 minutes
    "target": "Nullable(Int32)",  # Classification of price_diff into categories
    
    # Aggregated fields from stock_quotes (per minute)
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
    
    # Aggregated fields from stock_trades (per minute)
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

# Schema for normalized stock data table
NORMALIZED_SCHEMA = {
    # Non-normalized fields
    "uni_id": "UInt64",  # Unique ID based on ticker and timestamp
    "ticker": "String",
    "timestamp": "DateTime64(9)",
    "target": "Nullable(Int32)",
    "quote_conditions": "Float64",  # Changed from String
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

async def create_normalized_table(db: ClickHouseDB) -> None:
    """
    Create the normalized stock data table
    """
    try:
        # Create the normalized table
        columns_def = ", ".join(f"{col} {type_}" for col, type_ in NORMALIZED_SCHEMA.items())
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db.database}.stock_normalized (
            {columns_def}
        ) ENGINE = MergeTree()
        PRIMARY KEY (timestamp, ticker)
        ORDER BY (timestamp, ticker)
        SETTINGS index_granularity = 8192
        """
        db.client.command(create_table_query)
        print("Created normalized table successfully")
        
        # Populate the normalized table with existing data
        populate_query = f"""
        INSERT INTO {db.database}.stock_normalized
        SELECT *
        FROM (
            SELECT
                uni_id,
                ticker,
                timestamp,
                target,
                /* Hash the quote_conditions similarly to trade_conditions */
                modulo(
                    cityHash64(quote_conditions),
                    1000000
                ) as quote_conditions,
                /* Hash the trade_conditions for model compatibility
                   Use simpler approach that doesn't require aggregation functions */
                modulo(
                    cityHash64(trade_conditions),
                    1000000
                ) as trade_conditions,
                ask_exchange,
                bid_exchange,
                trade_exchange,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(open, 0), 2) / 1000))), 2) as open,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(high, 0), 2) / 1000))), 2) as high,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(low, 0), 2) / 1000))), 2) as low,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(close, 0), 2) / 1000))), 2) as close,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(volume, 0), 2) / 1000000))), 2) as volume,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(vwap, 0), 2) / 1000))), 2) as vwap,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(transactions, 0), 2) / 1000))), 2) as transactions,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(price_diff, 0), 2) / 10))), 2) as price_diff,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(max_price_diff, 0), 2) / 10))), 2) as max_price_diff,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(avg_bid_price, 0), 2) / 1000))), 2) as avg_bid_price,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(avg_ask_price, 0), 2) / 1000))), 2) as avg_ask_price,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(min_bid_price, 0), 2) / 1000))), 2) as min_bid_price,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(max_ask_price, 0), 2) / 1000))), 2) as max_ask_price,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(total_bid_size, 0), 2) / 100000))), 2) as total_bid_size,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(total_ask_size, 0), 2) / 100000))), 2) as total_ask_size,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(quote_count, 0), 2) / 1000))), 2) as quote_count,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(avg_trade_price, 0), 2) / 1000))), 2) as avg_trade_price,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(min_trade_price, 0), 2) / 1000))), 2) as min_trade_price,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(max_trade_price, 0), 2) / 1000))), 2) as max_trade_price,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(total_trade_size, 0), 2) / 100000))), 2) as total_trade_size,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(trade_count, 0), 2) / 1000))), 2) as trade_count,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_5, 0), 2) / 1000))), 2) as sma_5,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_9, 0), 2) / 1000))), 2) as sma_9,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_12, 0), 2) / 1000))), 2) as sma_12,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_20, 0), 2) / 1000))), 2) as sma_20,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_50, 0), 2) / 1000))), 2) as sma_50,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_100, 0), 2) / 1000))), 2) as sma_100,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(sma_200, 0), 2) / 1000))), 2) as sma_200,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(ema_9, 0), 2) / 1000))), 2) as ema_9,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(ema_12, 0), 2) / 1000))), 2) as ema_12,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(ema_20, 0), 2) / 1000))), 2) as ema_20,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(macd_value, 0), 2) / 10))), 2) as macd_value,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(macd_signal, 0), 2) / 10))), 2) as macd_signal,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(macd_histogram, 0), 2) / 10))), 2) as macd_histogram,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(rsi_14, 0), 2) / 100))), 2) as rsi_14,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(daily_high, 0), 2) / 1000))), 2) as daily_high,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(daily_low, 0), 2) / 1000))), 2) as daily_low,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(previous_close, 0), 2) / 1000))), 2) as previous_close,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(tr_current, 0), 2) / 100))), 2) as tr_current,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(tr_high_close, 0), 2) / 100))), 2) as tr_high_close,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(tr_low_close, 0), 2) / 100))), 2) as tr_low_close,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(tr_value, 0), 2) / 100))), 2) as tr_value,
                round(5 / (1 + exp(-5 * (coalesce(nullIf(atr_value, 0), 2) / 100))), 2) as atr_value
            FROM {db.database}.{config.TABLE_STOCK_MASTER}
            ORDER BY timestamp ASC, ticker ASC
        )
        """
        
        db.client.command(populate_query)
        print("Populated normalized table with existing data")
        
    except Exception as e:
        print(f"Error creating normalized table: {str(e)}")
        raise e

async def populate_master_table(db: ClickHouseDB, from_date: datetime = None, to_date: datetime = None) -> None:
    """
    Populate the master table with existing data from source tables
    
    Args:
        db: Database connection
        from_date: Optional start date (not used for auto-detection)
        to_date: Optional end date (not used for auto-detection)
    """
    try:
        print("Populating master table with existing data...")
        
        # Always use auto-detection to find all available data
        date_range_query = f"""
        SELECT 
            min(toDate(timestamp)) as min_date,
            max(toDate(timestamp)) as max_date
        FROM {db.database}.stock_bars
        """
        result = db.client.query(date_range_query)
        min_date = result.first_row[0]
        max_date = result.first_row[1]
        print(f"Auto-detected date range from stock_bars: {min_date} to {max_date}")
        
        current_date = min_date
        
        print(f"Processing data from {min_date} to {max_date}")
        
        # First, collect all dates to process
        dates_to_process = []
        while current_date <= max_date:
            dates_to_process.append(current_date)
            current_date = current_date + timedelta(days=1)
        
        # Process dates in ascending order
        for current_date in sorted(dates_to_process):
            print(f"Processing {current_date}...")
            
            # Insert data for current day
            insert_query = f"""
            INSERT INTO {db.database}.{config.TABLE_STOCK_MASTER}
            WITH 
            -- Get base data for the current day
            base_data AS (
                SELECT 
                    ticker,
                    timestamp,
                    cityHash64(ticker, toString(timestamp)) as uni_id,
                    round(open, 2) as open,
                    round(high, 2) as high,
                    round(low, 2) as low,
                    round(close, 2) as close,
                    coalesce(nullIf(volume, 0), 2) as volume,
                    round(coalesce(nullIf(vwap, 0), 2), 2) as vwap,
                    coalesce(nullIf(transactions, 0), 2) as transactions,
                    -- Calculate future close prices for price_diff
                    any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) as future_close,
                    -- Calculate price_diff using future_close
                    round(((any(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 15 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100, 2) as price_diff,
                    -- Calculate max_price_diff
                    round(if(
                        abs(((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100) >
                        abs(((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100),
                        ((max(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100,
                        ((min(close) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) - close) / nullIf(close, 0)) * 100
                    ), 2) as max_price_diff,
                    -- Calculate daily metrics
                    round(max(high) OVER (PARTITION BY ticker, toDate(timestamp)), 2) as daily_high,
                    round(min(low) OVER (PARTITION BY ticker, toDate(timestamp)), 2) as daily_low,
                    -- Get previous day's close using lagInFrame
                    round(lagInFrame(close) OVER (PARTITION BY ticker ORDER BY toDate(timestamp)), 2) as previous_close
                FROM {db.database}.stock_bars
                WHERE toDate(timestamp) = '{current_date}'
                ORDER BY timestamp ASC, ticker ASC
            ),
            -- Get quote data
            quote_metrics AS (
                SELECT
                    ticker,
                    toStartOfMinute(sip_timestamp) as timestamp,
                    round(coalesce(nullIf(avg(bid_price), 0), 2), 2) as avg_bid_price,
                    round(coalesce(nullIf(avg(ask_price), 0), 2), 2) as avg_ask_price,
                    round(coalesce(nullIf(min(bid_price), 0), 2), 2) as min_bid_price,
                    round(coalesce(nullIf(max(ask_price), 0), 2), 2) as max_ask_price,
                    coalesce(nullIf(sum(bid_size), 0), 2) as total_bid_size,
                    coalesce(nullIf(sum(ask_size), 0), 2) as total_ask_size,
                    coalesce(nullIf(count(*), 0), 2) as quote_count,
                    coalesce(nullIf(arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([coalesce(groupArray(conditions), [])]))), ''), '2') AS quote_conditions,
                    coalesce(nullIf(argMax(ask_exchange, sip_timestamp), 0), 2) as ask_exchange,
                    coalesce(nullIf(argMax(bid_exchange, sip_timestamp), 0), 2) as bid_exchange
                FROM {db.database}.stock_quotes
                WHERE toDate(sip_timestamp) = '{current_date}'
                GROUP BY ticker, timestamp
                ORDER BY timestamp ASC, ticker ASC
            ),
            -- Get trade data
            trade_metrics AS (
                SELECT
                    ticker,
                    toStartOfMinute(sip_timestamp) as timestamp,
                    round(coalesce(nullIf(avg(price), 0), 2), 2) as avg_trade_price,
                    round(coalesce(nullIf(min(price), 0), 2), 2) as min_trade_price,
                    round(coalesce(nullIf(max(price), 0), 2), 2) as max_trade_price,
                    coalesce(nullIf(sum(size), 0), 2) as total_trade_size,
                    coalesce(nullIf(count(*), 0), 2) as trade_count,
                    coalesce(nullIf(arrayStringConcat(arrayMap(x -> toString(x), arrayFlatten([coalesce(groupArray(conditions), [])]))), ''), '2') AS trade_conditions,
                    coalesce(nullIf(argMax(exchange, sip_timestamp), 0), 2) as trade_exchange
                FROM {db.database}.stock_trades
                WHERE toDate(sip_timestamp) = '{current_date}'
                GROUP BY ticker, timestamp
                ORDER BY timestamp ASC, ticker ASC
            ),
            -- Get indicator data
            indicator_metrics AS (
                SELECT
                    ticker,
                    timestamp,
                    cityHash64(ticker, toString(timestamp)) as uni_id,
                    round(coalesce(nullIf(any(if(indicator_type = 'SMA_5', value, NULL)), 0), 2), 2) as sma_5,
                    round(coalesce(nullIf(any(if(indicator_type = 'SMA_9', value, NULL)), 0), 2), 2) as sma_9,
                    round(coalesce(nullIf(any(if(indicator_type = 'SMA_12', value, NULL)), 0), 2), 2) as sma_12,
                    round(coalesce(nullIf(any(if(indicator_type = 'SMA_20', value, NULL)), 0), 2), 2) as sma_20,
                    round(coalesce(nullIf(any(if(indicator_type = 'SMA_50', value, NULL)), 0), 2), 2) as sma_50,
                    round(coalesce(nullIf(any(if(indicator_type = 'SMA_100', value, NULL)), 0), 2), 2) as sma_100,
                    round(coalesce(nullIf(any(if(indicator_type = 'SMA_200', value, NULL)), 0), 2), 2) as sma_200,
                    round(coalesce(nullIf(any(if(indicator_type = 'EMA_9', value, NULL)), 0), 2), 2) as ema_9,
                    round(coalesce(nullIf(any(if(indicator_type = 'EMA_12', value, NULL)), 0), 2), 2) as ema_12,
                    round(coalesce(nullIf(any(if(indicator_type = 'EMA_20', value, NULL)), 0), 2), 2) as ema_20,
                    round(coalesce(nullIf(any(if(indicator_type = 'MACD', value, NULL)), 0), 2), 2) as macd_value,
                    round(coalesce(nullIf(any(if(indicator_type = 'MACD', signal, NULL)), 0), 2), 2) as macd_signal,
                    round(coalesce(nullIf(any(if(indicator_type = 'MACD', histogram, NULL)), 0), 2), 2) as macd_histogram,
                    round(coalesce(nullIf(any(if(indicator_type = 'RSI', value, NULL)), 0), 2), 2) as rsi_14
                FROM {db.database}.stock_indicators
                WHERE toDate(timestamp) = '{current_date}'
                GROUP BY ticker, timestamp
                ORDER BY timestamp ASC, ticker ASC
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
                -- Price metrics
                b.price_diff,
                b.max_price_diff,
                multiIf(
                    b.price_diff <= -1, 0,
                    b.price_diff <= -0.5, 1,
                    b.price_diff <= 0, 2,
                    b.price_diff <= 0.5, 3,
                    b.price_diff <= 1, 4,
                    5
                ) as target,
                -- Quote metrics
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
                -- Trade metrics
                t.avg_trade_price,
                t.min_trade_price,
                t.max_trade_price,
                t.total_trade_size,
                t.trade_count,
                t.trade_conditions,
                t.trade_exchange,
                -- Technical indicators
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
            LEFT JOIN quote_metrics q ON b.ticker = q.ticker AND b.timestamp = q.timestamp
            LEFT JOIN trade_metrics t ON b.ticker = t.ticker AND b.timestamp = t.timestamp
            LEFT JOIN indicator_metrics i ON b.ticker = i.ticker AND b.timestamp = i.timestamp
            ORDER BY b.timestamp ASC, b.ticker ASC
            """
            
            db.client.command(insert_query)
            
        print("Master table populated successfully")
        
    except Exception as e:
        print(f"Error populating master table: {str(e)}")
        raise e

async def create_master_table(db: ClickHouseDB, from_date: datetime = None, to_date: datetime = None) -> None:
    """
    Create the master stock data table that combines all other tables
    
    Args:
        db: Database connection
        from_date: Optional start date for populating data
        to_date: Optional end date for populating data
    """
    try:
        # Create the master table with timestamp ordering
        columns_def = ", ".join(f"{col} {type_}" for col, type_ in MASTER_SCHEMA.items())
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db.database}.{config.TABLE_STOCK_MASTER} (
            {columns_def}
        ) ENGINE = MergeTree()
        PRIMARY KEY (timestamp, ticker)
        ORDER BY (timestamp, ticker)
        SETTINGS index_granularity = 8192
        """
        db.client.command(create_table_query)
        print("Created master table successfully")
        
        # Ensure source tables exist before populating
        tables_to_check = ['stock_bars', 'stock_quotes', 'stock_trades', 'stock_indicators']
        for table in tables_to_check:
            if not db.table_exists(table):
                raise Exception(f"Source table {table} does not exist. Please create source tables first.")
        
        # After creating the table structure, populate it with data
        await populate_master_table(db, from_date, to_date)
        
    except Exception as e:
        print(f"Error creating master table: {str(e)}")
        raise e

async def init_master_table(db: ClickHouseDB, from_date: datetime = None, to_date: datetime = None) -> None:
    """
    Initialize the master stock data table
    
    Args:
        db: Database connection
        from_date: Optional start date for populating data
        to_date: Optional end date for populating data
    """
    try:
        start_time = datetime.now()
        
        # Drop existing tables to ensure clean initialization
        print("Dropping existing master tables...")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.{config.TABLE_STOCK_MASTER}")
        print("Existing tables dropped successfully")
        
        # Create master table
        print("\nCreating master table...")
        master_start = datetime.now()
        await create_master_table(db, from_date, to_date)
        master_time = datetime.now() - master_start
        print(f"Master table created successfully (took {master_time})")
        
        # Create normalized table
        print("\nCreating normalized table...")
        norm_start = datetime.now()
        await create_normalized_table(db)
        norm_time = datetime.now() - norm_start
        print(f"Normalized table created successfully (took {norm_time})")
        
        total_time = datetime.now() - start_time
        print(f"\nMaster table initialization complete! Total time: {total_time}")
        
    except Exception as e:
        print(f"Error initializing master table: {str(e)}")
        raise e 