import os
from datetime import timedelta, datetime
import pandas as pd

# Assuming db_utils is in the same directory
from db_utils import ClickHouseClient

# --- Configuration ---
# Database table names
PREDICTIONS_TABLE = "stock_historical_predictions"
QUOTES_TABLE = "stock_quotes"
PNL_TABLE = "stock_pnl"

# Share size for P&L calculation
SHARE_SIZE = 100

# Time offsets for entry and exit relative to prediction timestamp
# Entry: 1 minute + 10 seconds after prediction timestamp
# Exit: 15 minutes after entry time
ENTRY_OFFSET_SECONDS = 70  # 1 * 60 + 10
EXIT_OFFSET_SECONDS = ENTRY_OFFSET_SECONDS + (15 * 60)
# ---------------------

# Define the schema for the PnL table
# Using DateTime64(9, 'UTC') for timestamps to match predictions and quotes
PNL_SCHEMA = {
    'prediction_timestamp': "DateTime64(9, 'UTC')", # Original prediction timestamp
    'ticker': 'String',
    'prediction_raw': 'Float64',
    'prediction_cat': 'UInt8',
    'pos_long': 'UInt8',       # 1 if long, 0 otherwise
    'pos_short': 'UInt8',      # 1 if short, 0 otherwise
    'entry_timestamp': "DateTime64(9, 'UTC')",  # Timestamp for entry quote
    'exit_timestamp': "DateTime64(9, 'UTC')",   # Timestamp for exit quote
    'entry_bid_price': 'Float64', # Bid price at entry time (used for short buy-to-cover)
    'entry_ask_price': 'Float64', # Ask price at entry time (used for long buy)
    'exit_bid_price': 'Float64',  # Bid price at exit time (used for short sell)
    'exit_ask_price': 'Float64',  # Ask price at exit time (used for long sell)
    'price_diff_per_share': 'Float64', # P&L per share based on long/short logic
    'share_size': 'UInt32',
    'pnl_total': 'Float64'      # Total P&L for the trade (price_diff_per_share * share_size)
}

# Define table engine and sorting key for PnL table
PNL_ENGINE = "ENGINE = MergeTree()" # Standard MergeTree is fine here
PNL_ORDER_BY = "ORDER BY (ticker, prediction_timestamp)"

# Batching configuration for inner loop (predictions per ticker)
PREDICTION_CHUNK_SIZE = 10000

# Buffer added to the min/max quote time range for ASOF JOIN
QUOTE_TIME_BUFFER = timedelta(hours=1) # Adjust buffer as needed (e.g., timedelta(days=1))

def run_pnl_calculation():
    """Connects to DB, calculates P&L, and stores it in the PNL_TABLE in batches by ticker and prediction chunk."""
    db_client = None
    try:
        # 1. Initialize Database Connection
        db_client = ClickHouseClient()
        db_name = db_client.database

        # Check if source tables exist
        if not db_client.table_exists(PREDICTIONS_TABLE):
            print(f"Error: Predictions table '{PREDICTIONS_TABLE}' not found. Please run predict_historical.py first.")
            return
        if not db_client.table_exists(QUOTES_TABLE):
            print(f"Error: Quotes table '{QUOTES_TABLE}' not found. Ensure quote data is available.")
            return

        # 2. Get list of distinct tickers to process
        print(f"Fetching distinct tickers from {PREDICTIONS_TABLE}...")
        ticker_query = f"SELECT DISTINCT ticker FROM `{db_name}`.`{PREDICTIONS_TABLE}` ORDER BY ticker"
        ticker_df = db_client.query_dataframe(ticker_query)
        if ticker_df is None or ticker_df.empty:
            print(f"No tickers found in {PREDICTIONS_TABLE}. Nothing to process.")
            return
        tickers_to_process = ticker_df['ticker'].tolist()
        print(f"Found {len(tickers_to_process)} tickers to process.")

        # 3. Drop and Recreate the PnL Table (start fresh before batching)
        print(f"Dropping existing PnL table '{PNL_TABLE}' (if it exists)...")
        db_client.drop_table_if_exists(PNL_TABLE)
        print(f"Creating empty PnL table '{PNL_TABLE}'...")
        create_table_query = f"""
        CREATE TABLE `{db_name}`.`{PNL_TABLE}` (
            {', '.join([f'`{col}` {dtype}' for col, dtype in PNL_SCHEMA.items()])}
        )
        {PNL_ENGINE}
        {PNL_ORDER_BY}
        """
        create_result = db_client.execute(create_table_query)
        # Basic check if creation likely succeeded (handle potential race condition/silent fail)
        if not create_result and not db_client.table_exists(PNL_TABLE):
            print(f"Failed to create table '{PNL_TABLE}'. Aborting.")
            return
        print(f"Successfully created or confirmed table '{PNL_TABLE}'.")

        # 4. Process P&L Calculation and Insertion in Batches
        total_rows_inserted_overall = 0
        start_time_all = datetime.now()

        for i, ticker in enumerate(tickers_to_process):
            print(f"\n--- Processing ticker {i+1}/{len(tickers_to_process)}: {ticker} ---")
            start_time_ticker = datetime.now()

            # Get total predictions count for this ticker for chunking
            count_query = f"SELECT count() FROM `{db_name}`.`{PREDICTIONS_TABLE}` WHERE ticker = {{ticker:String}}"
            count_result = db_client.execute(count_query, params={'ticker': ticker})
            if not count_result or not count_result.result_rows:
                print(f"Could not get prediction count for ticker {ticker}. Skipping...")
                continue
            total_predictions_for_ticker = count_result.result_rows[0][0]
            if total_predictions_for_ticker == 0:
                print(f"No predictions found for ticker {ticker}. Skipping...")
                continue
            print(f"Found {total_predictions_for_ticker} predictions for {ticker}. Processing in chunks of {PREDICTION_CHUNK_SIZE}...")

            # Inner loop for prediction chunks within the current ticker
            offset = 0
            while offset < total_predictions_for_ticker:
                print(f"  Processing chunk: offset {offset}, limit {PREDICTION_CHUNK_SIZE}")
                start_time_chunk = datetime.now()

                # --- Step 1: Fetch target times for the current chunk --- 
                target_times_query = f"""
                SELECT
                    timestamp + INTERVAL {ENTRY_OFFSET_SECONDS} SECOND AS target_entry_time,
                    timestamp + INTERVAL {EXIT_OFFSET_SECONDS} SECOND AS target_exit_time
                FROM `{db_name}`.`{PREDICTIONS_TABLE}`
                WHERE ticker = {{ticker:String}}
                ORDER BY timestamp -- Must match the main query order
                LIMIT {PREDICTION_CHUNK_SIZE} OFFSET {{offset:UInt64}}
                """
                params_chunk = {'ticker': ticker, 'offset': offset}
                target_times_df = db_client.query_dataframe(target_times_query, params=params_chunk)

                if target_times_df is None or target_times_df.empty:
                    print(f"  Warning: Could not fetch target times for chunk (offset {offset}). Skipping chunk.")
                    offset += PREDICTION_CHUNK_SIZE
                    continue
                
                # Ensure columns are datetime objects
                target_times_df['target_entry_time'] = pd.to_datetime(target_times_df['target_entry_time'], utc=True)
                target_times_df['target_exit_time'] = pd.to_datetime(target_times_df['target_exit_time'], utc=True)

                # --- Step 2: Determine min/max quote time range for the chunk --- 
                min_target_time = target_times_df['target_entry_time'].min()
                max_target_time = target_times_df['target_exit_time'].max()

                # Add buffer
                quote_range_start = min_target_time - QUOTE_TIME_BUFFER
                quote_range_end = max_target_time + QUOTE_TIME_BUFFER
                
                print(f"    Quote Time Range for ASOF JOIN: {quote_range_start} to {quote_range_end}")

                # --- Step 3: Construct and Execute the INSERT query with time filters --- 
                # Format timestamps for direct inclusion in the SQL query string
                quote_start_str = quote_range_start.strftime('%Y-%m-%d %H:%M:%S.%f')
                quote_end_str = quote_range_end.strftime('%Y-%m-%d %H:%M:%S.%f')

                insert_pnl_query = f"""
                INSERT INTO `{db_name}`.`{PNL_TABLE}`
                WITH prediction_times AS (
                    SELECT
                        timestamp AS prediction_timestamp,
                        ticker,
                        prediction_raw,
                        prediction_cat,
                        prediction_timestamp + INTERVAL {ENTRY_OFFSET_SECONDS} SECOND AS target_entry_time,
                        prediction_timestamp + INTERVAL {EXIT_OFFSET_SECONDS} SECOND AS target_exit_time
                    FROM `{db_name}`.`{PREDICTIONS_TABLE}`
                    WHERE ticker = {{ticker:String}}
                    ORDER BY prediction_timestamp
                    LIMIT {PREDICTION_CHUNK_SIZE} OFFSET {{offset:UInt64}}
                )
                SELECT
                    pt.prediction_timestamp AS prediction_timestamp,
                    pt.ticker AS ticker,
                    pt.prediction_raw AS prediction_raw,
                    pt.prediction_cat AS prediction_cat,
                    if(pt.prediction_cat IN (3, 4, 5), 1, 0) AS pos_long,
                    if(pt.prediction_cat IN (0, 1, 2), 1, 0) AS pos_short,
                    entry_q.timestamp AS entry_timestamp,
                    exit_q.timestamp AS exit_timestamp,
                    entry_q.bid_price AS entry_bid_price,
                    entry_q.ask_price AS entry_ask_price,
                    exit_q.bid_price AS exit_bid_price,
                    exit_q.ask_price AS exit_ask_price,
                    multiIf(
                        pos_long = 1, exit_q.bid_price - entry_q.ask_price,
                        pos_short = 1, entry_q.bid_price - exit_q.ask_price,
                        0
                    ) AS price_diff_per_share,
                    {SHARE_SIZE} AS share_size,
                    price_diff_per_share * share_size AS pnl_total
                FROM prediction_times pt
                ASOF LEFT JOIN (
                    SELECT sip_timestamp as timestamp, ticker, bid_price, ask_price
                    FROM `{db_name}`.`{QUOTES_TABLE}`
                    WHERE ticker = {{ticker:String}}
                      AND sip_timestamp >= '{quote_start_str}'  -- Manually formatted string
                      AND sip_timestamp <= '{quote_end_str}'    -- Manually formatted string
                    ORDER BY ticker, timestamp
                 ) AS entry_q
                ON pt.ticker = entry_q.ticker AND pt.target_entry_time >= entry_q.timestamp
                ASOF LEFT JOIN (
                    SELECT sip_timestamp as timestamp, ticker, bid_price, ask_price
                    FROM `{db_name}`.`{QUOTES_TABLE}`
                    WHERE ticker = {{ticker:String}}
                      AND sip_timestamp >= '{quote_start_str}'  -- Manually formatted string
                      AND sip_timestamp <= '{quote_end_str}'    -- Manually formatted string
                    ORDER BY ticker, timestamp
                ) AS exit_q
                ON pt.ticker = exit_q.ticker AND pt.target_exit_time >= exit_q.timestamp
                WHERE
                    entry_q.timestamp IS NOT NULL AND
                    exit_q.timestamp IS NOT NULL AND
                    entry_q.ask_price > 0 AND entry_q.bid_price > 0 AND
                    exit_q.ask_price > 0 AND exit_q.bid_price > 0
                """

                # Execute the query - NOTE: quote_start/end are now in the string, not params
                params = {
                    'ticker': ticker,
                    'offset': offset
                    # 'quote_start': quote_range_start, # Removed from params
                    # 'quote_end': quote_range_end     # Removed from params
                }
                print(f"  Executing calculation and insertion for {ticker} chunk (offset {offset})...")
                insert_result = db_client.execute(insert_pnl_query, params=params)
                end_time_chunk = datetime.now()

                if insert_result:
                    print(f"  Successfully processed chunk for {ticker} (offset {offset}). Time: {end_time_chunk - start_time_chunk}")
                else:
                    print(f"  Failed to process chunk for {ticker} (offset {offset}). Check query and logs. Skipping chunk...")

                offset += PREDICTION_CHUNK_SIZE

            end_time_ticker = datetime.now()
            print(f"--- Finished processing ticker {ticker}. Total Time: {end_time_ticker - start_time_ticker} --- ")

        end_time_all = datetime.now()
        print(f"\n--- Finished processing all tickers --- ")
        print(f"Total batch processing time: {end_time_all - start_time_all}")

        # Final count
        count_result = db_client.execute(f"SELECT count() FROM `{db_name}`.`{PNL_TABLE}`")
        if count_result and count_result.result_rows:
            total_rows_inserted_overall = count_result.result_rows[0][0]
            print(f"Total rows inserted into '{PNL_TABLE}': {total_rows_inserted_overall}")
        else:
            print(f"Could not retrieve final row count for '{PNL_TABLE}'.")

    except FileNotFoundError as e:
        print(f"Error: {e}.")
    except ValueError as e:
        print(f"Configuration or data error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during P&L calculation: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if db_client:
            db_client.close()

if __name__ == "__main__":
    print("=== Starting P&L Calculation ===")
    run_pnl_calculation()
    print("===============================") 