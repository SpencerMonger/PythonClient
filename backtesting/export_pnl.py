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

def export_pnl_to_csv(size_multiplier: float = 1.0):
    """Fetches P&L data from ClickHouse in chunks (ordered by time), sorts rows within each chunk, and exports each chunk to a separate CSV file."""
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

        # 3. Get total row count for chunking (with time and category filters applied)
        time_filter = "(formatDateTime(prediction_timestamp, '%H:%M:%S') >= '13:30:00' AND formatDateTime(prediction_timestamp, '%H:%M:%S') <= '14:30:00') OR (formatDateTime(prediction_timestamp, '%H:%M:%S') >= '19:00:00' AND formatDateTime(prediction_timestamp, '%H:%M:%S') <= '20:00:00')"
        category_filter = "prediction_cat IN (0, 1, 2, 3, 4, 5)"
        raw_filter = "(prediction_raw >= 3.0 OR prediction_raw <= 2.0)"
        combined_filter_clause = f"({time_filter}) AND ({category_filter}) AND ({raw_filter})"
        count_query = f"SELECT count() FROM `{db_name}`.`{PNL_TABLE}` WHERE {combined_filter_clause}"
        print(f"Counting rows with filter: {combined_filter_clause}...")
        count_result = db_client.execute(count_query)
        if not count_result or not count_result.result_rows:
            print(f"Error: Could not get row count from {PNL_TABLE}.")
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

            # Fetch chunk with combined filters, ordered ONLY by prediction_timestamp
            print(f"  Fetching rows {offset} to {offset + DB_CHUNK_SIZE - 1} from {PNL_TABLE} (with filters)...", end='', flush=True)
            query = f"""
            SELECT * FROM `{db_name}`.`{PNL_TABLE}`
            WHERE {combined_filter_clause} -- Apply the combined filter here
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

            print(f"  Fetched {len(pnl_df)} P&L records for this chunk. Transforming data...")

            # Ensure timestamp columns are pandas Timestamps (UTC)
            for col in ['prediction_timestamp', 'entry_timestamp', 'exit_timestamp']:
                if col in pnl_df.columns:
                    pnl_df[col] = pd.to_datetime(pnl_df[col], utc=True)

            # Transform data for this chunk
            output_rows = []
            output_rows.append(CSV_HEADER) # Add header

            rows_added_count = 0
            for _, row in pnl_df.iterrows():
                symbol = row['ticker']
                original_quantity = row['share_size']
                quantity = int(round(original_quantity * size_multiplier))

                entry_date, entry_time = format_timestamp(row['entry_timestamp'])
                exit_date, exit_time = format_timestamp(row['exit_timestamp'])

                if not entry_date or not exit_date:
                    # print(f"  Warning: Skipping row for ticker {symbol} in chunk {part_number} due to invalid timestamp.")
                    continue

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
    args = parser.parse_args()

    if args.size_multiplier <= 0:
        parser.error("Size multiplier must be a positive number.")

    print("=== Starting Backtest Results CSV Export (Multiple Sorted Files) ===")
    export_pnl_to_csv(size_multiplier=args.size_multiplier)
    print("===============================================================") 