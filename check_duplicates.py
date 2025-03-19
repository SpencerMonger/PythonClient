from endpoints.db import ClickHouseDB

def analyze_table(db: ClickHouseDB, table_name: str):
    print(f"\nAnalyzing {table_name}:")
    print("-" * 50)
    
    # Get sample rows
    timestamp_col = 'sip_timestamp' if table_name in ['stock_trades', 'stock_quotes'] else 'timestamp'
    sample_query = f"""
    SELECT ticker, {timestamp_col}, uni_id, count() OVER (PARTITION BY uni_id) as count_per_id
    FROM {db.database}.{table_name}
    LIMIT 10
    """
    
    # Get counts
    count_query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT uni_id) as unique_ids,
        COUNT(*) - COUNT(DISTINCT uni_id) as duplicate_count
    FROM {db.database}.{table_name}
    """
    
    # Get duplicate distribution
    dupes_query = f"""
    SELECT count_per_id, COUNT(*) as num_groups
    FROM (
        SELECT uni_id, COUNT(*) as count_per_id
        FROM {db.database}.{table_name}
        GROUP BY uni_id
    )
    GROUP BY count_per_id
    ORDER BY count_per_id
    """
    
    try:
        # Print sample rows
        print("Sample rows:")
        result = db.client.query(sample_query)
        for row in result.result_rows:
            print(f"Ticker: {row[0]}, Timestamp: {row[1]}, UniID: {row[2]}, Occurrences: {row[3]}")
        
        # Print counts
        print("\nCounts:")
        result = db.client.query(count_query)
        row = result.result_rows[0]
        print(f"Total rows: {row[0]:,}")
        print(f"Unique IDs: {row[1]:,}")
        print(f"Duplicate count: {row[2]:,}")
        
        # Print duplicate distribution
        print("\nDuplicate distribution:")
        result = db.client.query(dupes_query)
        for row in result.result_rows:
            print(f"Records with {row[0]} occurrences: {row[1]:,} unique IDs")
            
    except Exception as e:
        print(f"Error analyzing table: {str(e)}")

def main():
    db = ClickHouseDB()
    tables = ['stock_bars', 'stock_daily', 'stock_trades', 'stock_quotes', 'stock_indicators']
    
    for table in tables:
        analyze_table(db, table)
    
    db.close()

if __name__ == "__main__":
    main() 