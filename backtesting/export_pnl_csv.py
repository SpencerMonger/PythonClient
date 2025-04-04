import os
import pandas as pd
import csv
from datetime import datetime

# Assuming db_utils is in the same directory
from db_utils import ClickHouseClient

# --- Configuration ---
# Database table name to read from
PNL_TABLE = "stock_pnl"

# Output CSV file path (relative to this script's location)
OUTPUT_DIR = os.path.dirname(__file__)
CSV_FILENAME = "backtest_results.csv"
OUTPUT_CSV_PATH = os.path.join(OUTPUT_DIR, CSV_FILENAME)

# CSV header
CSV_HEADER = ["Date", "Time", "Symbol", "Quantity", "Price", "Side"]
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

def export_pnl_to_csv():
    """Fetches P&L data from ClickHouse and exports it to the specified CSV format."""
    db_client = None
    try:
        # 1. Initialize Database Connection
        db_client = ClickHouseClient()
        db_name = db_client.database

        # 2. Check if PNL table exists
        if not db_client.table_exists(PNL_TABLE):
            print(f"Error: PnL table '{PNL_TABLE}' not found. Please run calculate_pnl.py first.")
            return

        # 3. Fetch all data from the PnL table
        print(f"Fetching data from {PNL_TABLE}...")
        query = f"SELECT * FROM `{db_name}`.`{PNL_TABLE}` ORDER BY ticker, prediction_timestamp"
        pnl_df = db_client.query_dataframe(query)

        if pnl_df is None:
            print(f"Error fetching data from {PNL_TABLE}.")
            return
        if pnl_df.empty:
            print(f"No data found in {PNL_TABLE} to export.")
            return

        print(f"Fetched {len(pnl_df)} P&L records.")

        # Ensure timestamp columns are pandas Timestamps (UTC)
        for col in ['prediction_timestamp', 'entry_timestamp', 'exit_timestamp']:
            if col in pnl_df.columns:
                # Convert assuming the data from ClickHouse might be naive or already UTC
                pnl_df[col] = pd.to_datetime(pnl_df[col], utc=True)

        # 4. Transform data into the target CSV format
        print("Transforming data for CSV export...")
        output_rows = []
        output_rows.append(CSV_HEADER)

        for _, row in pnl_df.iterrows():
            symbol = row['ticker']
            quantity = row['share_size']

            entry_date, entry_time = format_timestamp(row['entry_timestamp'])
            exit_date, exit_time = format_timestamp(row['exit_timestamp'])

            if not entry_date or not exit_date: # Skip if timestamps are invalid
                print(f"Warning: Skipping row for ticker {symbol} due to invalid timestamp.")
                continue

            if row['pos_long'] == 1:
                # Entry leg (Buy)
                output_rows.append([
                    entry_date,
                    entry_time,
                    symbol,
                    quantity,
                    f"{row['entry_ask_price']:.2f}", # Buy at Ask
                    "Buy"
                ])
                # Exit leg (Sell)
                output_rows.append([
                    exit_date,
                    exit_time,
                    symbol,
                    quantity,
                    f"{row['exit_bid_price']:.2f}", # Sell at Bid
                    "Sell"
                ])
            elif row['pos_short'] == 1:
                # Entry leg (Sell Short)
                output_rows.append([
                    entry_date,
                    entry_time,
                    symbol,
                    quantity,
                    f"{row['entry_bid_price']:.2f}", # Sell at Bid
                    "Sell"
                ])
                # Exit leg (Buy to Cover)
                output_rows.append([
                    exit_date,
                    exit_time,
                    symbol,
                    quantity,
                    f"{row['exit_ask_price']:.2f}", # Buy at Ask
                    "Buy"
                ])

        # 5. Write to CSV file
        print(f"Writing {len(output_rows) - 1} trade legs to {OUTPUT_CSV_PATH}...")
        try:
            with open(OUTPUT_CSV_PATH, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(output_rows)
            print("Successfully exported backtest results.")
        except IOError as e:
            print(f"Error writing to CSV file {OUTPUT_CSV_PATH}: {e}")

    except Exception as e:
        print(f"An unexpected error occurred during CSV export: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if db_client:
            db_client.close()

if __name__ == "__main__":
    print("=== Starting Backtest Results CSV Export ===")
    export_pnl_to_csv()
    print("============================================") 