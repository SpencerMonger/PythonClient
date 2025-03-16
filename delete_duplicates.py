from endpoints.db import ClickHouseDB
import argparse

def delete_duplicates(db: ClickHouseDB, table_name: str, dry_run: bool = True) -> dict:
    """
    Delete duplicate rows from a table, keeping only one row per timestamp.
    """
    try:
        # Determine timestamp column based on table name
        timestamp_col = 'sip_timestamp' if table_name in ['stock_trades', 'stock_quotes'] else 'timestamp'
        
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
            
        # Find the timestamp column
        if timestamp_col not in columns:
            timestamp_alternatives = ['t', 'time', 'date', 'datetime', 'created_at', 'updated', 'ex_date']
            for alt in timestamp_alternatives:
                if alt in columns:
                    timestamp_col = alt
                    break
            else:
                return {'table': table_name, 'error': f'No timestamp column found in {table_name}'}

        # Get counts before deletion with rounded timestamps
        count_query = f"""
            SELECT 
                count() as total,
                count(DISTINCT toDateTime64(toUnixTimestamp64Nano({timestamp_col}), 3)) as unique_timestamps
            FROM {db.database}.{table_name}
        """
        result = db.client.query(count_query)
        
        if not result.result_rows:
            return {'table': table_name, 'error': 'No results returned'}
            
        total_rows = result.result_rows[0][0]
        unique_timestamps = result.result_rows[0][1]
        duplicate_rows = total_rows - unique_timestamps

        if duplicate_rows == 0:
            return {
                'table': table_name,
                'total_rows': total_rows,
                'unique_timestamps': unique_timestamps,
                'duplicate_rows': 0,
                'message': 'No duplicates found'
            }

        if dry_run:
            return {
                'table': table_name,
                'total_rows': total_rows,
                'unique_timestamps': unique_timestamps,
                'duplicate_rows': duplicate_rows,
                'message': f'Would delete {duplicate_rows:,} duplicate rows'
            }
        
        # Execute the delete with rounded timestamps
        delete_query = f"""
        ALTER TABLE {db.database}.{table_name} DELETE WHERE
        (
            toDateTime64(toUnixTimestamp64Nano({timestamp_col}), 3),
            rowNumberInAllBlocks()
        ) IN 
        (
            SELECT 
                toDateTime64(toUnixTimestamp64Nano({timestamp_col}), 3),
                rowNumberInAllBlocks()
            FROM {db.database}.{table_name}
            WHERE (
                toDateTime64(toUnixTimestamp64Nano({timestamp_col}), 3),
                rowNumberInAllBlocks()
            ) NOT IN (
                SELECT 
                    toDateTime64(toUnixTimestamp64Nano({timestamp_col}), 3),
                    min(rowNumberInAllBlocks())
                FROM {db.database}.{table_name}
                GROUP BY toDateTime64(toUnixTimestamp64Nano({timestamp_col}), 3)
            )
        )
        """
        db.client.command(delete_query)
        
        # Get count after deletion
        result = db.client.query(f"SELECT count() FROM {db.database}.{table_name}")
        rows_after = result.result_rows[0][0]
        rows_deleted = total_rows - rows_after
        
        return {
            'table': table_name,
            'total_rows': total_rows,
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
            remaining = result['total_rows'] - result['duplicate_rows']
            print(f"Remaining Rows: {remaining:,}")
            if 'rows_after' in result:
                print(f"Rows After: {result['rows_after']:,}")
            print(f"Message: {result['message']}")
        
        print("-" * 30)
    
    db.close()

if __name__ == "__main__":
    main() 