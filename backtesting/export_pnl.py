import os
import pandas as pd
import csv
from datetime import datetime
import argparse

# Assuming db_utils is in the same directory
from db_utils import ClickHouseClient

# --- Configuration ---
# Database table name to read from
PNL_TABLE = "stock_pnl"

# Output CSV file path
# Use an absolute path as requested
OUTPUT_DIR = r"C:\Users\spenc\Downloads\Dev Files\client-python-master\backtesting\files"
# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# CSV header
CSV_HEADER = ["Date", "Time", "Symbol", "Quantity", "Price", "Side"]

# Chunk size for fetching from DB (adjust if needed based on memory/file size)
DB_CHUNK_SIZE = 50000
# ---------------------

def format_timestamp(ts: pd.Timestamp) -> tuple:
    """Formats pandas Timestamp into Date (MM/DD/YYYY) and Time (HH:MM:SS)."""
    if pd.isna(ts):
        return None, None
    # Ensure the timestamp is timezone-aware (UTC)
    if ts.tzinfo is None:
        ts_utc = ts.tz_localize('UTC')
    else:
        ts_utc = ts.tz_convert('UTC')
    # Format as required
    date_str = ts_utc.strftime('%m/%d/%Y')
    time_str = ts_utc.strftime('%H:%M:%S') # Using %S for seconds
    return date_str, time_str

def export_pnl_to_csv(size_multiplier: float = 1.0, use_time_filter: bool = False, start_date_str: str | None = None, end_date_str: str | None = None, use_random_sample: bool = False, sample_fraction: float = 0.1, max_concurrent_positions: int | None = None):
    """Fetches P&L data from ClickHouse, applies filters, optionally samples, limits concurrent positions per symbol, sorts rows within each chunk, and exports each chunk to a separate CSV file."""
    db_client = None
    base_timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    files_written = []
    try:
        # 1. Initialize Database Connection
        db_client = ClickHouseClient()
        db_name = db_client.database

        # 2. Check if PNL table exists
        if not db_client.table_exists(PNL_TABLE):
            print(f"Error: PnL table '{PNL_TABLE}' not found. Please run calculate_pnl.py first.")
            return

        # 3. Define filters and get total row count for chunking
        time_filter = "(formatDateTime(prediction_timestamp, '%H:%M:%S') >= '14:30:00' AND formatDateTime(prediction_timestamp, '%H:%M:%S') <= '20:00:00')" # Corresponds to 9am-4pm EST (UTC-4)
        category_filter = "prediction_cat IN (0, 1, 2, 3, 4, 5)"
        raw_filter = "(prediction_raw >= 3.2 OR prediction_raw <= 1.8)"

        active_filters = [category_filter, raw_filter]

        # Add time filter if requested
        if use_time_filter:
            active_filters.append(time_filter)
            print("Time filter is ENABLED.")
        else:
            print("Time filter is DISABLED.")

        # Add date filters if provided
        date_filters = []
        if start_date_str:
            date_filters.append(f"toDate(prediction_timestamp) >= toDate('{start_date_str}')")
            print(f"Date filter: Starting from {start_date_str}")
        if end_date_str:
            date_filters.append(f"toDate(prediction_timestamp) <= toDate('{end_date_str}')")
            print(f"Date filter: Ending at {end_date_str}")
        if date_filters:
            active_filters.extend(date_filters)

        combined_filter_clause = " AND ".join(f"({f})" for f in active_filters)
        where_clause = f"WHERE {combined_filter_clause}" if combined_filter_clause else ""

        # --- Construct Base Query Parts ---
        base_table_expression = f"`{db_name}`.`{PNL_TABLE}`"
        # Remove subquery logic, sampling will be done in pandas
        # subquery_for_sampling = f"(SELECT * FROM {base_table_expression} {where_clause})"

        # --- Handle Sampling (Now done post-fetch) ---
        # count_query_source = base_table_expression
        # fetch_query_source = base_table_expression
        # final_where_clause = where_clause
        # sample_clause = ""

        if use_random_sample:
            print(f"Random sampling ENABLED: {sample_fraction * 100:.1f}% (Applied after fetch)")
            if not (0 < sample_fraction <= 1):
                print("Error: Sample fraction must be between 0 (exclusive) and 1 (inclusive). Aborting.")
                return
            # sample_clause = f"SAMPLE {sample_fraction}"
            # count_query_source = subquery_for_sampling
            # fetch_query_source = subquery_for_sampling
            # final_where_clause = ""
        else:
            print("Random sampling DISABLED.")

        # --- Final Query Construction (No Sampling Clause) ---
        # Count query uses only the where_clause
        count_query = f"SELECT count() FROM {base_table_expression} {where_clause}"
        print(f"Counting rows with filter: {combined_filter_clause if combined_filter_clause else 'None'}...") # Removed sampling info from log
        count_result = db_client.execute(count_query)
        if not count_result or not count_result.result_rows:
            print(f"Error: Could not get row count from {PNL_TABLE} with applied filters.") # Adjusted error message
            return
        total_rows = count_result.result_rows[0][0]
        if total_rows == 0:
            print(f"No data found in {PNL_TABLE} matching filters to export.")
            return
        print(f"Total P&L records to export: {total_rows}. Processing in chunks of {DB_CHUNK_SIZE}...")

        # 4. Fetch, process, sort within chunk, and write data in chunks
        offset = 0
        part_number = 1
        while offset < total_rows: # total_rows now reflects the filtered count
            print(f"\nProcessing chunk {part_number} (offset {offset})...")

            # Fetch chunk with combined filters, potential sampling, ordered ONLY by prediction_timestamp
            print(f"  Fetching rows {offset} to {offset + DB_CHUNK_SIZE - 1} from {PNL_TABLE} (with filters)...", end='', flush=True) # Removed sampling info from log
            # Construct the fetch query for the current chunk (No Sampling Clause)
            query = f"""
            SELECT * FROM {base_table_expression} {where_clause}
            ORDER BY prediction_timestamp -- Order by time only for fetching
            LIMIT {DB_CHUNK_SIZE} OFFSET {offset}
            """
            pnl_df = db_client.query_dataframe(query)
            print(" Done.")

            if pnl_df is None:
                print(f"  Error fetching chunk starting at offset {offset}. Skipping...")
                offset += DB_CHUNK_SIZE
                part_number += 1
                continue # Try next chunk
            if pnl_df.empty:
                print("  No more data found (or chunk was empty). Finishing.")
                break # Exit loop if no data is returned

            # --- Apply Sampling in Pandas (if enabled) --- START
            if use_random_sample and not pnl_df.empty:
                original_chunk_size = len(pnl_df)
                # Use random_state for reproducibility if needed, removing for pure random sampling now
                # pnl_df = pnl_df.sample(frac=sample_fraction, random_state=1)
                pnl_df = pnl_df.sample(frac=sample_fraction)
                print(f"  Applied pandas sampling ({sample_fraction * 100:.1f}%): {original_chunk_size} -> {len(pnl_df)} rows.")
                if pnl_df.empty:
                    print("  Chunk became empty after sampling. Skipping processing for this chunk.")
                    offset += DB_CHUNK_SIZE # Still advance offset based on original fetch size
                    part_number += 1
                    continue # Skip to next fetch iteration
            # --- Apply Sampling in Pandas (if enabled) --- END

            print(f"  Fetched {len(pnl_df)} P&L records for this chunk. Transforming data...")

            # Ensure timestamp columns are pandas Timestamps (UTC)
            for col in ['prediction_timestamp', 'entry_timestamp', 'exit_timestamp']:
                if col in pnl_df.columns:
                    pnl_df[col] = pd.to_datetime(pnl_df[col], utc=True)

            # --- Sort chunk by entry time for concurrency check --- START
            print(f"  Sorting chunk by entry_timestamp for concurrency check...")
            pnl_df.sort_values(by='entry_timestamp', inplace=True)
            # --- Sort chunk by entry time for concurrency check --- END

            # --- Concurrency Limiting Logic --- START
            active_positions = {} # {symbol: [exit_ts1, exit_ts2, ...]}
            skipped_concurrency_count = 0
            # --- Concurrency Limiting Logic --- END

            # Transform data for this chunk
            output_rows = []
            output_rows.append(CSV_HEADER) # Add header

            rows_added_count = 0
            for _, row in pnl_df.iterrows():
                symbol = row['ticker']
                original_quantity = row['share_size']
                quantity = int(round(original_quantity * size_multiplier))

                entry_timestamp = row['entry_timestamp'] # Keep as Timestamp for comparison
                exit_timestamp = row['exit_timestamp']   # Keep as Timestamp for comparison
                entry_date, entry_time = format_timestamp(entry_timestamp)
                exit_date, exit_time = format_timestamp(exit_timestamp)

                if pd.isna(entry_timestamp) or pd.isna(exit_timestamp) or not entry_date or not exit_date:
                    # print(f"  Warning: Skipping row for ticker {symbol} in chunk {part_number} due to invalid timestamp.")
                    continue

                # --- Concurrency Limiting Logic --- START
                if max_concurrent_positions is not None:
                    # Clean up expired positions for this symbol
                    current_symbol_positions = active_positions.get(symbol, [])
                    # Keep only positions whose exit time is AFTER the current entry time
                    active_positions[symbol] = [exit_ts for exit_ts in current_symbol_positions if exit_ts > entry_timestamp]

                    # Check the limit
                    if len(active_positions[symbol]) >= max_concurrent_positions:
                        # print(f"  Skipping {symbol} @ {entry_timestamp} due to concurrency limit ({len(active_positions[symbol])} >= {max_concurrent_positions})")
                        skipped_concurrency_count += 1
                        continue # Skip this row

                    # If allowed, add this position's exit time to the tracker
                    active_positions.setdefault(symbol, []).append(exit_timestamp)
                # --- Concurrency Limiting Logic --- END

                try:
                    entry_ask = float(row['entry_ask_price'])
                    exit_bid = float(row['exit_bid_price'])
                    entry_bid = float(row['entry_bid_price'])
                    exit_ask = float(row['exit_ask_price'])
                except (ValueError, TypeError):
                    # print(f"  Warning: Skipping row for ticker {symbol} in chunk {part_number} due to invalid price.")
                    continue

                if row['pos_long'] == 1:
                    output_rows.append([entry_date, entry_time, symbol, quantity, f"{entry_ask:.2f}", "Buy"])
                    output_rows.append([exit_date, exit_time, symbol, quantity, f"{exit_bid:.2f}", "Sell"])
                    rows_added_count += 2
                elif row['pos_short'] == 1:
                    output_rows.append([entry_date, entry_time, symbol, quantity, f"{entry_bid:.2f}", "Sell"])
                    output_rows.append([exit_date, exit_time, symbol, quantity, f"{exit_ask:.2f}", "Buy"])
                    rows_added_count += 2

            print(f"  Generated {rows_added_count} trade legs for this chunk.")

            # Sort rows within this chunk (excluding header) by Date then Time
            if rows_added_count > 0:
                print(f"  Sorting {rows_added_count} trade legs for this chunk...")
                data_rows = output_rows[1:]
                try:
                    # Sort using datetime objects for robustness
                    data_rows.sort(key=lambda r: datetime.strptime(f"{r[0]} {r[1]}", '%m/%d/%Y %H:%M:%S'))
                    sorted_chunk_rows = [output_rows[0]] + data_rows # Re-add header
                except ValueError as e:
                    print(f"  Error during chunk sorting - invalid date/time format found: {e}. Using unsorted chunk.")
                    sorted_chunk_rows = output_rows # Proceed with unsorted if error occurs

                # Generate filename for this chunk
                chunk_csv_filename = f"backtest_results_{base_timestamp_str}_part_{part_number}.csv"
                chunk_output_csv_path = os.path.join(OUTPUT_DIR, chunk_csv_filename)

                # Write this sorted chunk to its CSV file
                print(f"  Writing {rows_added_count} sorted trade legs to {chunk_output_csv_path}...")
                try:
                    # Limit rows written per file (excluding header)
                    rows_to_write = sorted_chunk_rows[:DB_CHUNK_SIZE + 1]
                    with open(chunk_output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerows(rows_to_write)
                    files_written.append(chunk_output_csv_path)
                    print(f"  Successfully wrote chunk {part_number} ({len(rows_to_write) - 1} rows).")
                except IOError as e:
                    print(f"  Error writing chunk {part_number} to CSV file {chunk_output_csv_path}: {e}")
            else:
                 print(f"  No valid trade legs generated for chunk {part_number}. No file written.")

            # Move to the next chunk
            offset += DB_CHUNK_SIZE # Use DB_CHUNK_SIZE for offset, as it relates to fetched rows
            part_number += 1

        print(f"\nExport finished. {len(files_written)} file(s) written to: {OUTPUT_DIR}")
        for file_path in files_written:
            print(f"  - {os.path.basename(file_path)}")

    except Exception as e:
        print(f"An unexpected error occurred during CSV export: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if db_client:
            db_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export P&L data from ClickHouse to multiple CSV files, sorted chronologically within each file.")
    parser.add_argument(
        "--size-multiplier",
        type=float,
        default=1.0,
        help="Multiplier to apply to the share_size from the PNL table. Default is 1.0."
    )
    parser.add_argument(
        "--use-time-filter",
        action='store_true', # Default is False, flag presence sets it to True
        help="Enable the time filter (currently 13:00-20:00 UTC / 9am-4pm EST-4). If not specified, the time filter is disabled."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for filtering (inclusive), format YYYY-MM-DD."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for filtering (inclusive), format YYYY-MM-DD."
    )
    parser.add_argument(
        "--use-random-sample",
        action='store_true',
        help="Enable random sampling of the filtered data."
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=0.1,
        help="Fraction of data to randomly sample (e.g., 0.5 for 50%). Used only if --use-random-sample is enabled. Must be > 0 and <= 1."
    )
    parser.add_argument(
        "--max-concurrent-positions",
        type=int,
        default=None,
        help="Maximum number of concurrent positions allowed per symbol at any time. If not specified, no limit is applied."
    )

    args = parser.parse_args()

    if args.size_multiplier <= 0:
        parser.error("Size multiplier must be a positive number.")
    if args.use_random_sample and not (0 < args.sample_fraction <= 1):
         parser.error(f"Sample fraction must be > 0 and <= 1, but got: {args.sample_fraction}")
    if args.max_concurrent_positions is not None and args.max_concurrent_positions <= 0:
        parser.error("Maximum concurrent positions must be a positive integer if specified.")

    print("=== Starting Backtest Results CSV Export (Multiple Sorted Files) ===")
    if args.max_concurrent_positions is not None:
        print(f"Concurrency Limit ENABLED: Max {args.max_concurrent_positions} positions per symbol.")
    else:
        print("Concurrency Limit DISABLED.")
    export_pnl_to_csv(
        size_multiplier=args.size_multiplier,
        use_time_filter=args.use_time_filter,
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        use_random_sample=args.use_random_sample,
        sample_fraction=args.sample_fraction,
        max_concurrent_positions=args.max_concurrent_positions
    )
    print("===============================================================") 