import asyncio
from endpoints.db import ClickHouseDB
from endpoints import config

def get_unique_columns_for_table(table: str) -> list:
    """
    Return the list of unique columns to filter duplicates on
    based on the table name.
    """
    if table == 'stock_indicators':
        return ['ticker', 'timestamp', 'indicator_type']
    else:
        return ['ticker', 'timestamp']

def delete_duplicate_rows(table: str, dry_run: bool = False) -> None:
    """
    Identifies and deletes duplicate rows in the specified ClickHouse table.
    For stock_indicators: deduplicate by (ticker, timestamp, indicator_type).
    For other tables: deduplicate by (ticker, timestamp).
    """

    db = ClickHouseDB()
    # Use the default database from config / .env
    db.database = config.CLICKHOUSE_DATABASE

    try:
        # Check if table exists
        check_query = f"""
            SELECT name
            FROM system.columns
            WHERE database = '{db.database}'
              AND table = '{table}'
            LIMIT 1
        """
        check_result = db.client.query(check_query)
        if not check_result.result_rows:
            print(f"Error: Table {table} not found in database {db.database}.")
            return

        # Count total rows
        total_rows_query = f"SELECT count() FROM {db.database}.{table}"
        total_rows_result = db.client.query(total_rows_query)
        total_rows = total_rows_result.result_rows[0][0]

        # Get the unique columns
        unique_cols = get_unique_columns_for_table(table)
        joined_cols = ', '.join(unique_cols)

        # Count unique rows
        unique_rows_query = f"""
            SELECT count()
            FROM (
                SELECT {joined_cols}
                FROM {db.database}.{table}
                GROUP BY {joined_cols}
            )
        """
        unique_rows_result = db.client.query(unique_rows_query)
        unique_count = unique_rows_result.result_rows[0][0]
        duplicate_count = total_rows - unique_count

        if duplicate_count <= 0:
            print(f"No duplicates found in table {table}.")
            return

        if dry_run:
            print(f"[DRY RUN] Table: {table}, Total Rows: {total_rows}, Unique Rows: {unique_count}, Duplicates: {duplicate_count}")
            print(f"[DRY RUN] Would delete {duplicate_count} rows.")
            print("[DRY RUN] No rows deleted.")
            return

        # Delete - keep only the first row per (unique_cols)
        partition_cols = ', '.join(unique_cols)
        delete_query = f"""
            ALTER TABLE {db.database}.{table}
            DELETE WHERE ({joined_cols}, rn) IN
            (
                SELECT {joined_cols}, rn
                FROM
                (
                    SELECT
                        {joined_cols},
                        row_number() OVER (PARTITION BY {partition_cols} ORDER BY timestamp ASC) AS rn
                    FROM {db.database}.{table}
                )
                WHERE rn > 1
            )
            SETTINGS mutations_sync = 2
        """
        db.client.command(delete_query)
        print(f"Deletion mutation issued. Table: {table}.")

        # Check row count after deletion
        post_rows_query = f"SELECT count() FROM {db.database}.{table}"
        post_rows_result = db.client.query(post_rows_query)
        rows_after = post_rows_result.result_rows[0][0]
        print(f"Rows before: {total_rows}, Rows after: {rows_after}, Deleted: {total_rows - rows_after}")

    except Exception as err:
        print(f"Error deleting duplicates from {table}: {str(err)}")
    finally:
        db.close()

def main():
    """
    Main function with built-in defaults. Adjust as needed.
    """
    # Toggle dry_run here if desired
    dry_run = False

    # Tables to deduplicate
    tables_to_deduplicate = [
        'stock_bars',
        'stock_daily',
        'stock_trades',
        'stock_quotes',
        'stock_indicators'
    ]

    # Run deduplication for each table
    for table_name in tables_to_deduplicate:
        print(f"\n--- Deduplicating table: {table_name} ---")
        delete_duplicate_rows(table=table_name, dry_run=dry_run)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.sleep(0))
    main()