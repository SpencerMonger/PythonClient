import argparse
from datetime import datetime
from endpoints.db import ClickHouseDB
from endpoints import config

# --- Configuration ---
# Define tables and their timestamp columns
# IMPORTANT: Update the timestamp_column value if it's different for any table.
TABLE_CONFIG = {
    'stock_normalized': 'timestamp',
    config.TABLE_STOCK_MASTER: 'timestamp', # Assuming timestamp column for master table
    'stock_trades': 'timestamp',
    'stock_quotes': 'timestamp',
    'stock_bars': 'timestamp',
    'stock_daily': 'timestamp',
    'stock_indicators': 'timestamp',
    'stock_news': 'timestamp',
}
# --- End Configuration ---

def parse_arguments():
    """Parses command-line arguments for start and end dates."""
    parser = argparse.ArgumentParser(description='Delete data from ClickHouse tables within a specified timestamp range.')
    parser.add_argument('start_date', type=str, help='Start date in YYYY-MM-DD format.')
    parser.add_argument('end_date', type=str, help='End date in YYYY-MM-DD format.')
    return parser.parse_args()

def validate_date(date_str):
    """Validates date format (YYYY-MM-DD) and converts to datetime object."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")

def main():
    args = parse_arguments()

    try:
        # Validate dates
        start_dt = validate_date(args.start_date)
        end_dt = validate_date(args.end_date)

        # Ensure start date is not after end date
        if start_dt > end_dt:
            print("Error: Start date cannot be after end date.")
            return

        # Format dates for ClickHouse query (assuming timestamp column is DateTime or similar)
        # ClickHouse typically uses 'YYYY-MM-DD HH:MM:SS' format
        start_timestamp_str = start_dt.strftime('%Y-%m-%d 00:00:00')
        end_timestamp_str = end_dt.strftime('%Y-%m-%d 23:59:59')

        print(f"Attempting to delete data from {start_timestamp_str} to {end_timestamp_str}...")

        db = ClickHouseDB()
        try:
            for table_name, ts_column in TABLE_CONFIG.items():
                if not ts_column:
                    print(f"Skipping table '{table_name}': Timestamp column not defined.")
                    continue

                # Check if table exists before attempting delete (optional but good practice)
                # This might require a specific method in your ClickHouseDB class,
                # or executing a "EXISTS table_name" query.
                # For simplicity, we'll proceed directly with DELETE.

                print(f"Deleting data from {table_name}...")
                # Construct the DELETE query
                # NOTE: DELETE operations in ClickHouse are asynchronous mutations by default.
                # They might take time to fully complete.
                delete_query = f"DELETE FROM {table_name} WHERE {ts_column} >= '{start_timestamp_str}' AND {ts_column} <= '{end_timestamp_str}'"

                try:
                    # Assuming your ClickHouseDB class has an execute_query method
                    db.execute_query(delete_query)
                    print(f"Delete statement issued for {table_name}.")
                except Exception as e:
                    print(f"Error executing delete for table {table_name}: {e}")
                    # Decide if you want to stop or continue with other tables
                    # continue

            print("Delete operations issued for all configured tables.")
            print("Note: ClickHouse DELETE mutations run asynchronously in the background.")
            print("Monitor the system.mutations table for completion status.")

        finally:
            db.close()
            print("Database connection closed.")

    except ValueError as ve:
        print(f"Date validation error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main() 