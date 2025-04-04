import asyncio
import argparse
from endpoints.db import ClickHouseDB
# Make sure config defines TABLE_STOCK_NORMALIZED or adjust the default below
from endpoints import config

async def check_table_schema(db: ClickHouseDB, table_name: str):
    """Connects to ClickHouse and prints the schema of the specified table."""
    try:
        print(f"Checking schema for table: {db.database}.{table_name}")

        # Use system.columns to get column names and types
        query = f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{db.database}' AND table = '{table_name}'
        ORDER BY position
        """

        # Run the query (using asyncio.to_thread for synchronous driver calls)
        result = await asyncio.to_thread(db.client.query, query)

        if not result or not result.result_rows:
            print(f"Table '{table_name}' not found or has no columns in database '{db.database}'.")
            return

        print("\nColumn Schema:")
        print("-" * 30)
        # Basic formatting to align columns
        max_name_len = 0
        if result.result_rows:
             max_name_len = max(len(row[0]) for row in result.result_rows)

        for column_name, data_type in result.result_rows:
            print(f"{column_name:<{max_name_len}} : {data_type}")
        print("-" * 30)

    except Exception as e:
        print(f"Error checking table schema for {table_name}: {str(e)}")

async def main():
    """Main function to parse arguments and run the schema check."""
    parser = argparse.ArgumentParser(description='Check the column schema of a table in ClickHouse.')

    # Try to get the default table name from config, otherwise use 'stock_normalized'
    default_table = 'stock_normalized' # Default fallback
    if hasattr(config, 'TABLE_STOCK_NORMALIZED') and config.TABLE_STOCK_NORMALIZED:
         default_table = config.TABLE_STOCK_NORMALIZED

    parser.add_argument(
        '--table-name',
        type=str,
        default=default_table,
        help=f'Name of the table to check (default: {default_table})'
    )

    args = parser.parse_args()

    db = None
    try:
        print("Initializing database connection...")
        # Ensure ClickHouseDB handles connection details (host, port, user, password)
        db = ClickHouseDB()
        print("Connection attempt finished.") # Changed message slightly

        if db.client: # Check if client connection was successful within ClickHouseDB init
            print("Database connection appears successful.")
            await check_table_schema(db, args.table_name)
        else:
            print("Failed to establish database connection. Please check DB credentials and availability.")

    except Exception as e:
        print(f"An error occurred during database initialization or schema check: {e}")
    finally:
        # Attempt to close the connection if the client object exists and has disconnect
        if db and hasattr(db, 'client') and db.client and hasattr(db.client, 'disconnect'):
             try:
                 print("Closing database connection...")
                 db.client.disconnect()
                 print("Database connection closed.")
             except Exception as close_e:
                 print(f"Error closing database connection: {close_e}")
        else:
            print("Could not close database connection (client might not exist or lack disconnect method).")


if __name__ == "__main__":
    print("Running schema check script...")
    # Using asyncio.run for simplicity
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Script execution failed: {e}")
    print("Schema check script finished.")
