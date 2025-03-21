from endpoints.db import ClickHouseDB
import argparse
from datetime import datetime, timedelta
from endpoints import config
from endpoints.master import init_master_table
import asyncio

def delete_duplicates(db: ClickHouseDB, table_name: str, dry_run: bool = True) -> dict:
    """
    Delete duplicate rows from a table based on uni_id
    """
    try:
        # First check if the table exists and get its columns
        check_query = f"""
            SELECT name 
            FROM system.columns 
            WHERE database = '{db.database}' 
            AND table = '{table_name}'
        """
        result = db.client.query(check_query)
        columns = [row[0] for row in result.result_rows]
        
        if not columns:
            return {'table': table_name, 'error': f'Table {table_name} not found'}
            
        # Determine timestamp column based on table name
        timestamp_col = 'sip_timestamp' if table_name in ['stock_trades', 'stock_quotes'] else 'timestamp'

        # First get total count
        count_query = f"SELECT count() FROM {db.database}.{table_name}"
        result = db.client.query(count_query)
        total_rows = result.result_rows[0][0]

        # Count duplicates using the same logic as check_duplicates.py
        duplicate_count_query = f"""
            WITH counts AS (
                SELECT uni_id, count(*) as cnt
                FROM {db.database}.{table_name}
                GROUP BY uni_id
            )
            SELECT 
                sum(CASE WHEN cnt > 1 THEN cnt - 1 ELSE 0 END) as duplicate_count
            FROM counts
            SETTINGS 
                max_memory_usage = 80000000000,
                max_bytes_before_external_group_by = 20000000000,
                group_by_overflow_mode = 'any'
        """
        result = db.client.query(duplicate_count_query)
        duplicate_count = result.result_rows[0][0]
        
        if duplicate_count == 0:
            return {
                'table': table_name,
                'total_rows': total_rows,
                'duplicate_rows': 0,
                'message': 'No duplicates found'
            }

        if dry_run:
            return {
                'table': table_name,
                'total_rows': total_rows,
                'duplicate_rows': duplicate_count,
                'message': f'Would delete {duplicate_count:,} duplicate rows'
            }
        
        # Store initial counts before deletion
        initial_total = total_rows
        
        # Delete duplicates keeping only the first occurrence of each uni_id
        delete_query = f"""
            ALTER TABLE {db.database}.{table_name}
            DELETE WHERE _row_number > 1
            FROM (
                SELECT *,
                    row_number() OVER (PARTITION BY uni_id ORDER BY {timestamp_col}) as _row_number
                FROM {db.database}.{table_name}
            )
            SETTINGS mutations_sync = 2
        """
        db.client.command(delete_query)
        
        # Get count after deletion
        result = db.client.query(f"SELECT count() FROM {db.database}.{table_name}")
        rows_after = result.result_rows[0][0]
        rows_deleted = initial_total - rows_after
        
        # Verify we didn't delete more rows than predicted
        if rows_deleted != duplicate_count:
            return {
                'table': table_name,
                'total_rows': initial_total,
                'rows_after': rows_after,
                'duplicate_rows': rows_deleted,
                'error': f'CRITICAL ERROR: Deleted {rows_deleted:,} rows but expected to delete {duplicate_count:,} rows. Please restore from backup.'
            }
        
        # Drop and reinitialize master tables if any rows were deleted
        if rows_deleted > 0:
            print("\nReinitializing master tables...")
            
            # Drop master tables
            print("Dropping stock_normalized_mv materialized view...")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized_mv")
            print("Normalized materialized view dropped successfully")
            
            print("\nDropping stock_normalized table...")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized")
            print("Normalized table dropped successfully")
            
            print("\nDropping stock_master_mv materialized view...")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master_mv")
            print("Master materialized view dropped successfully")
            
            print("\nDropping stock_master table...")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.{config.TABLE_STOCK_MASTER}")
            print("Master table dropped successfully")
            
            # Initialize master tables
            print("\nInitializing master tables...")
            asyncio.run(init_master_table(db))
            print("Master tables initialized successfully")
        
        return {
            'table': table_name,
            'total_rows': initial_total,
            'rows_after': rows_after,
            'duplicate_rows': rows_deleted,
            'message': f'Successfully deleted {rows_deleted:,} duplicate rows'
        }
        
    except Exception as e:
        return {'table': table_name, 'error': str(e)}

def main():
    parser = argparse.ArgumentParser(description='Delete duplicate rows from ClickHouse tables')
    parser.add_argument('--table', type=str, help='Specific table to deduplicate (optional)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without actually deleting')
    args = parser.parse_args()
    
    db = ClickHouseDB()
    
    # List of tables to process
    tables = [args.table] if args.table else [
        'stock_bars',
        'stock_daily',
        'stock_trades',
        'stock_quotes',
        'stock_indicators'
        # Excluding:
        # - stock_news (no timestamp column)
        # - stock_master (materialized view, updates automatically)
        # - stock_normalized (materialized view, updates automatically)
    ]
    
    print("\nClickHouse Database Deduplication")
    print("=" * 50)
    
    for table in tables:
        print(f"\nProcessing table: {table}")
        print("-" * 30)
        
        result = delete_duplicates(db, table, args.dry_run)
        
        if 'error' in result:
            print(f"Error: {result['error']}")
        else:
            print(f"Total Rows: {result['total_rows']:,}")
            print(f"Duplicate Rows: {result['duplicate_rows']:,}")
            remaining = result['rows_after'] if 'rows_after' in result else (result['total_rows'] - result['duplicate_rows'])
            print(f"Remaining Rows: {remaining:,}")
            if 'rows_after' in result:
                print(f"Rows After: {result['rows_after']:,}")
            print(f"Message: {result['message']}")
        
        print("-" * 30)
    
    db.close()

if __name__ == "__main__":
    main() 