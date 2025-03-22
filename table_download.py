import os
import asyncio
import pandas as pd
from datetime import datetime
from clickhouse_driver import Client
from endpoints.db import ClickHouseDB
from endpoints import config

async def download_normalized_table(db: ClickHouseDB, output_dir: str) -> None:
    """
    Download the stock_normalized table as a parquet file
    
    Args:
        db: ClickHouseDB instance
        output_dir: Directory to save the parquet file
    """
    try:
        print("Starting download of normalized table...")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Query to get all data from stock_normalized table with explicit column names
        query = f"""
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
            open,
            high,
            low,
            close,
            volume,
            vwap,
            transactions,
            price_diff,
            max_price_diff,
            avg_bid_price,
            avg_ask_price,
            min_bid_price,
            max_ask_price,
            total_bid_size,
            total_ask_size,
            quote_count,
            avg_trade_price,
            min_trade_price,
            max_trade_price,
            total_trade_size,
            trade_count,
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
            daily_high,
            daily_low,
            previous_close,
            tr_current,
            tr_high_close,
            tr_low_close,
            tr_value,
            atr_value
        FROM {db.database}.stock_normalized
        ORDER BY timestamp, ticker
        """
        
        # Execute query and fetch results
        print("Executing query...")
        result = db.client.query(query)
        
        # Convert to pandas DataFrame
        print("Converting to DataFrame...")
        df = pd.DataFrame(
            result.result_rows,
            columns=[
                'uni_id', 'ticker', 'timestamp', 'target', 'quote_conditions', 'trade_conditions',
                'ask_exchange', 'bid_exchange', 'trade_exchange', 'open', 'high', 'low',
                'close', 'volume', 'vwap', 'transactions', 'price_diff', 'max_price_diff',
                'avg_bid_price', 'avg_ask_price', 'min_bid_price', 'max_ask_price',
                'total_bid_size', 'total_ask_size', 'quote_count', 'avg_trade_price',
                'min_trade_price', 'max_trade_price', 'total_trade_size', 'trade_count',
                'sma_5', 'sma_9', 'sma_12', 'sma_20', 'sma_50', 'sma_100', 'sma_200',
                'ema_9', 'ema_12', 'ema_20', 'macd_value', 'macd_signal', 'macd_histogram',
                'rsi_14', 'daily_high', 'daily_low', 'previous_close', 'tr_current',
                'tr_high_close', 'tr_low_close', 'tr_value', 'atr_value'
            ]
        )
        
        # Save as parquet file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"stock_normalized_{timestamp}.parquet")
        print(f"Saving to {output_file}...")
        df.to_parquet(output_file, index=False)
        
        print(f"Successfully downloaded normalized table to {output_file}")
        print(f"DataFrame shape: {df.shape}")
        
    except Exception as e:
        print(f"Error downloading normalized table: {str(e)}")
        raise e

async def main():
    """Main function to run the download process"""
    # Initialize database connection
    db = ClickHouseDB()
    
    # Set output directory
    output_dir = r"C:\Users\spenc\Downloads\Dev Files\client-python-master\downloads"
    
    # Download the table
    await download_normalized_table(db, output_dir)

if __name__ == "__main__":
    asyncio.run(main()) 