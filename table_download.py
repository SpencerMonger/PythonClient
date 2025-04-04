import os
import asyncio
import pandas as pd
from datetime import datetime
import argparse  # Added argparse
from clickhouse_driver import Client
from endpoints.db import ClickHouseDB
from endpoints import config
import numpy as np

# Helper function to validate date format
def validate_date(date_str):
    """Validates date format (YYYY-MM-DD) and converts to datetime object."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")

async def download_normalized_table(db: ClickHouseDB, output_dir: str, start_date_str: str | None = None, end_date_str: str | None = None) -> None:
    """
    Download the stock_normalized table as a parquet file, optionally filtering by a date range.

    Args:
        db: ClickHouseDB instance
        output_dir: Directory to save the parquet file
        start_date_str: Start date in 'YYYY-MM-DD' format (inclusive).
        end_date_str: End date in 'YYYY-MM-DD' format (exclusive).
    """
    try:
        print("Starting download of normalized table...")
        if start_date_str and end_date_str:
            print(f"Filtering data from {start_date_str} (inclusive) to {end_date_str} (exclusive).")
        else:
            print("Downloading all data (no date range specified).")

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Base query
        base_query = f"""
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
        """

        # Add WHERE clause if dates are provided
        where_clause = ""
        if start_date_str and end_date_str:
            # Format dates for ClickHouse (assuming timestamp is DateTime or similar)
            # Use YYYY-MM-DD HH:MM:SS format
            start_dt = validate_date(start_date_str)
            end_dt = validate_date(end_date_str)
            start_timestamp_ch = start_dt.strftime('%Y-%m-%d 00:00:00')
            end_timestamp_ch = end_dt.strftime('%Y-%m-%d 00:00:00') # Exclusive end date
            where_clause = f"WHERE timestamp >= '{start_timestamp_ch}' AND timestamp < '{end_timestamp_ch}'"

        # Final query
        query = f"{base_query} {where_clause} ORDER BY timestamp, ticker"

        # Execute query and fetch results
        print("Executing query...")
        # Assuming db.client is the raw clickhouse_driver Client instance
        # If ClickHouseDB has a wrapper method, use that instead.
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

        # <<< IMPROVED QUALITY CHECK >>>
        print("\nPerforming thorough data quality check before export...")
        total_issues_fixed = 0
        
        # Define columns expected to be numeric
        expected_numeric_cols = [
            'open', 'high', 'low', 'close', 'volume', 'vwap', 'transactions',
            'price_diff', 'max_price_diff', 'avg_bid_price', 'avg_ask_price',
            'min_bid_price', 'max_ask_price', 'total_bid_size', 'total_ask_size',
            'quote_count', 'avg_trade_price', 'min_trade_price', 'max_trade_price',
            'total_trade_size', 'trade_count', 'sma_5', 'sma_9', 'sma_12', 'sma_20',
            'sma_50', 'sma_100', 'sma_200', 'ema_9', 'ema_12', 'ema_20',
            'macd_value', 'macd_signal', 'macd_histogram', 'rsi_14', 'daily_high',
            'daily_low', 'previous_close', 'tr_current', 'tr_high_close',
            'tr_low_close', 'tr_value', 'atr_value', 'trade_conditions', 'quote_conditions'
        ]
        
        for col in expected_numeric_cols:
            if col not in df.columns:
                print(f"  - WARNING: Expected numeric column '{col}' not found in DataFrame.")
                continue
                
            # Check column dtype
            col_dtype = df[col].dtype
            # print(f"  Checking column '{col}' (dtype: {col_dtype})...")
            
            # First, identify non-numeric values by type and specific values
            issues = {}
            
            # 1. Check for empty strings
            empty_string_mask = df[col].astype(str).eq('')
            empty_string_count = empty_string_mask.sum()
            if empty_string_count > 0:
                issues['empty_strings'] = empty_string_count
                print(f"    - Found {empty_string_count} empty string(s) ('')")
                # Show some affected row indices
                sample_indices = df.index[empty_string_mask].tolist()[:5]
                print(f"      Sample affected rows (indices): {sample_indices}")
            
            # 2. Check for NaN, None values
            nan_mask = df[col].isna()
            nan_count = nan_mask.sum()
            if nan_count > 0:
                issues['nans'] = nan_count
                print(f"    - Found {nan_count} NaN/None values")
            
            # 3. Check for infinite values (if numeric)
            inf_count = 0
            if pd.api.types.is_numeric_dtype(df[col]):
                inf_mask = ~np.isfinite(df[col])
                inf_count = inf_mask.sum() - nan_count  # Subtract NaNs already counted
                if inf_count > 0:
                    issues['infs'] = inf_count
                    print(f"    - Found {inf_count} infinite values")
            
            # 4. Check for other non-numeric strings
            other_non_numeric = 0
            if col_dtype == 'object':
                # This will attempt to convert all values to numeric
                numeric_conversion = pd.to_numeric(df[col], errors='coerce')
                # Count newly created NaNs that weren't NaN before
                new_nans_mask = numeric_conversion.isna() & ~df[col].isna()
                other_non_numeric = new_nans_mask.sum() - empty_string_count  # Don't double-count empty strings
                if other_non_numeric > 0:
                    issues['other_non_numeric'] = other_non_numeric
                    print(f"    - Found {other_non_numeric} other non-numeric values")
                    # Show some examples of these values
                    non_numeric_values = df.loc[new_nans_mask & ~empty_string_mask, col].unique()
                    print(f"      Examples: {non_numeric_values[:10]}")
            
            # Fix issues if any were found
            total_issues = sum(issues.values())
            if total_issues > 0:
                print(f"    Fixing {total_issues} problematic values in column '{col}'")
                # Create a temp copy to work with
                col_fixed = df[col].copy()
                
                # Fix empty strings
                if empty_string_count > 0:
                    col_fixed = col_fixed.replace('', 2.5)  # Replace with 2.5 per your comment
                    
                # Fix other non-numeric values
                if other_non_numeric > 0 or nan_count > 0 or inf_count > 0:
                    # Convert to numeric, replacing all problematic values with NaN
                    col_fixed = pd.to_numeric(col_fixed, errors='coerce')
                    # Replace NaNs with 2.5
                    col_fixed = col_fixed.fillna(2.5)
                    # Replace infinities with 2.5
                    col_fixed = col_fixed.replace([np.inf, -np.inf], 2.5)
                
                # Update the DataFrame with the fixed column
                df[col] = col_fixed
                total_issues_fixed += total_issues
            else:
                # Only print if no fixing was needed, otherwise the fixing block prints
                pass # print("    No issues found.")
            
            # Print the final dtype *after* potential conversion/fixing
            print(f"  Processed column '{col}' (final dtype: {df[col].dtype}).")
        
        if total_issues_fixed > 0:
            print(f"\nFixed a total of {total_issues_fixed} problematic values across all columns.")
        else:
            print("\nNo problematic values found in any column.")
        # <<< END IMPROVED QUALITY CHECK >>>

        if df.empty:
            print("No data found for the specified criteria.")
            return

        # Identify float columns to round (excluding IDs, timestamps, conditions, etc.)
        float_cols_to_round = [
            'open', 'high', 'low', 'close', 'volume', 'vwap', 'transactions',
            'price_diff', 'max_price_diff', 'avg_bid_price', 'avg_ask_price',
            'min_bid_price', 'max_ask_price', 'total_bid_size', 'total_ask_size',
            'quote_count', 'avg_trade_price', 'min_trade_price', 'max_trade_price',
            'total_trade_size', 'trade_count', 'sma_5', 'sma_9', 'sma_12', 'sma_20',
            'sma_50', 'sma_100', 'sma_200', 'ema_9', 'ema_12', 'ema_20',
            'macd_value', 'macd_signal', 'macd_histogram', 'rsi_14', 'daily_high',
            'daily_low', 'previous_close', 'tr_current', 'tr_high_close',
            'tr_low_close', 'tr_value', 'atr_value'
            # Note: trade_conditions is already Float64 in the normalized schema,
            # but it comes from a hash, so rounding might not be appropriate.
            # Keep it as is unless you specifically want to round the hash modulo result.
        ]

        # Round the identified float columns to 2 decimal places
        print("Rounding float columns to 2 decimal places...")
        for col in float_cols_to_round:
            if col in df.columns:
                # Ensure the column is numeric before rounding
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].round(2)

        # Save as parquet file with timestamp and optional date range in name
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_range_str = f"_{start_date_str}_to_{end_date_str}" if start_date_str and end_date_str else ""
        output_file = os.path.join(output_dir, f"stock_normalized{date_range_str}_{current_time}.parquet")
        print(f"Saving to {output_file}...")
        df.to_parquet(output_file, index=False)

        print(f"Successfully downloaded normalized table to {output_file}")
        print(f"DataFrame shape: {df.shape}")

    except ValueError as ve:
        print(f"Date validation error: {ve}")
    except Exception as e:
        print(f"Error downloading normalized table: {str(e)}")
        # raise e # Optionally re-raise the exception

async def main():
    """Main function to parse arguments and run the download process"""
    parser = argparse.ArgumentParser(description='Download stock_normalized table from ClickHouse, optionally filtering by date.')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format (inclusive). Required if --end-date is set.')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format (exclusive). Required if --start-date is set.')
    parser.add_argument('--output-dir', type=str, default=r"C:\Users\spenc\Downloads\Dev Files\client-python-master\downloads", help='Directory to save the output parquet file.')

    args = parser.parse_args()

    # Basic validation for date arguments
    if (args.start_date and not args.end_date) or (not args.start_date and args.end_date):
        parser.error("--start-date and --end-date must be used together.")
        return
    if args.start_date and args.end_date:
        try:
            # Further validation happens in download_normalized_table
             validate_date(args.start_date)
             validate_date(args.end_date)
             if datetime.strptime(args.start_date, '%Y-%m-%d') >= datetime.strptime(args.end_date, '%Y-%m-%d'):
                 parser.error("Start date must be before end date.")
                 return
        except ValueError as e:
             parser.error(f"Invalid date format: {e}")
             return


    # Initialize database connection
    db = None
    try:
        db = ClickHouseDB()

        # Set output directory
        output_dir = args.output_dir # Use output dir from args

        # Download the table
        await download_normalized_table(db, output_dir, args.start_date, args.end_date)

    except Exception as e:
        print(f"An error occurred in main: {e}")
    finally:
        if db:
            # Assuming ClickHouseDB has a close method or handles closing internally
            pass # db.close() if necessary

if __name__ == "__main__":
    # Consider adding error handling for asyncio.run itself
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Script failed to run: {e}") 