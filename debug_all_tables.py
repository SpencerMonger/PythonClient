from endpoints.db import ClickHouseDB
from datetime import datetime
from typing import Dict, List
import argparse

def analyze_table_stats(db: ClickHouseDB, table_name: str) -> Dict:
    """
    Get basic statistics for a table
    """
    date_column = "sip_timestamp" if "stock_trades" in table_name or "stock_quotes" in table_name else "timestamp"
    stats = db.client.command(f"""
        SELECT 
            count(*) as total_rows,
            count(DISTINCT ticker) as unique_tickers,
            min({date_column}) as min_date,
            max({date_column}) as max_date,
            count(DISTINCT toDate({date_column})) as unique_days
        FROM {table_name}
    """)
    result = dict(stats[0])
    result['table'] = table_name.split('.')[-1]
    return result

def get_date_gaps(db: ClickHouseDB, table_name: str, date_column: str = "timestamp") -> List[Dict]:
    """
    Find gaps in date coverage for each ticker
    """
    query = f"""
    WITH 
    dates AS (
        SELECT 
            ticker,
            toDate({date_column}) as date,
            count(*) as records
        FROM {table_name}
        GROUP BY ticker, date
    ),
    date_diffs AS (
        SELECT 
            ticker,
            date,
            records,
            dateDiff('day', lag(date) OVER (PARTITION BY ticker ORDER BY date), date) as days_gap
        FROM dates
    )
    SELECT 
        ticker,
        date as gap_start_date,
        date + INTERVAL days_gap DAY as gap_end_date,
        days_gap
    FROM date_diffs
    WHERE days_gap > 1
    ORDER BY ticker, date
    LIMIT 100
    """
    return db.client.command(query)

def check_data_quality(db: ClickHouseDB, table_name: str) -> Dict:
    """
    Check for data quality issues like null values
    """
    # Get column names first
    columns_query = f"""
    SELECT name, type
    FROM system.columns
    WHERE table = '{table_name.split('.')[-1]}'
    """
    columns = db.client.command(columns_query)
    
    quality_stats = {}
    for col in columns:
        if 'Array' not in col['type']:  # Skip array columns
            null_count_query = f"""
            SELECT 
                count(*) as total,
                countIf({col['name']} IS NULL) as null_count,
                countIf({col['name']} = 0) as zero_count
            FROM {table_name}
            """
            stats = db.client.command(null_count_query)[0]
            quality_stats[col['name']] = stats
    
    return quality_stats

def analyze_ticker_coverage(db: ClickHouseDB) -> None:
    """
    Compare ticker coverage across all tables
    """
    tables = [
        'stock_bars',
        'stock_daily',
        'stock_trades',
        'stock_quotes',
        'stock_news',
        'stock_indicators',
        'stock_master'
    ]
    
    ticker_sets = {}
    for table in tables:
        query = f"SELECT DISTINCT ticker FROM {db.database}.{table}"
        tickers = set(row['ticker'] for row in db.client.command(query))
        ticker_sets[table] = tickers
    
    # Find tickers missing from each table compared to others
    all_tickers = set.union(*ticker_sets.values())
    missing_analysis = {}
    
    for table, tickers in ticker_sets.items():
        missing = all_tickers - tickers
        if missing:
            missing_analysis[table] = sorted(list(missing))
    
    return ticker_sets, missing_analysis

def check_master_consistency(db: ClickHouseDB) -> None:
    """
    Check if master table data is consistent with source tables
    """
    consistency_checks = []
    
    # Check bar data consistency
    bar_check = db.client.command("""
        SELECT 
            'Bars' as check_type,
            count(*) as master_count,
            (SELECT count(*) FROM stock_bars) as source_count,
            abs(count(*) - (SELECT count(*) FROM stock_bars)) as difference
        FROM stock_master
        WHERE close IS NOT NULL
    """)
    consistency_checks.extend(bar_check)
    
    # Check quote data consistency
    quote_check = db.client.command("""
        WITH quote_minutes AS (
            SELECT ticker, toStartOfMinute(sip_timestamp) as minute, count(*) as quote_count
            FROM stock_quotes
            GROUP BY ticker, minute
        )
        SELECT 
            'Quotes' as check_type,
            (SELECT count(*) FROM stock_master WHERE quote_count > 0) as master_count,
            count(*) as source_count,
            abs((SELECT count(*) FROM stock_master WHERE quote_count > 0) - count(*)) as difference
        FROM quote_minutes
    """)
    consistency_checks.extend(quote_check)
    
    # Check trade data consistency
    trade_check = db.client.command("""
        WITH trade_minutes AS (
            SELECT ticker, toStartOfMinute(sip_timestamp) as minute, count(*) as trade_count
            FROM stock_trades
            GROUP BY ticker, minute
        )
        SELECT 
            'Trades' as check_type,
            (SELECT count(*) FROM stock_master WHERE trade_count > 0) as master_count,
            count(*) as source_count,
            abs((SELECT count(*) FROM stock_master WHERE trade_count > 0) - count(*)) as difference
        FROM trade_minutes
    """)
    consistency_checks.extend(trade_check)
    
    # Check indicator data consistency
    indicator_check = db.client.command("""
        SELECT 
            'Indicators' as check_type,
            count(*) as master_count,
            (SELECT count(*) FROM (
                SELECT DISTINCT ticker, timestamp, indicator_type 
                FROM stock_indicators
            )) as source_count,
            abs(count(*) - (
                SELECT count(*) FROM (
                    SELECT DISTINCT ticker, timestamp, indicator_type 
                    FROM stock_indicators
                )
            )) as difference
        FROM stock_master
        WHERE sma_20 IS NOT NULL OR ema_20 IS NOT NULL OR macd_value IS NOT NULL OR rsi_14 IS NOT NULL
    """)
    consistency_checks.extend(indicator_check)
    
    return consistency_checks

def get_table_info(db: ClickHouseDB, table_name: str) -> dict:
    """Get basic information about a table"""
    try:
        # First check if table exists
        exists_query = f"""
        SELECT count(*) as cnt
        FROM system.tables 
        WHERE database = '{db.database}' AND name = '{table_name}'
        """
        exists_result = db.client.query(exists_query)
        if exists_result.result_rows[0][0] == 0:
            return {
                'table_name': table_name,
                'exists': False,
                'error': 'Table does not exist'
            }

        # Get column information first to determine timestamp column
        columns_query = f"""
            SELECT name, type
            FROM system.columns
            WHERE database = '{db.database}' AND table = '{table_name}'
        """
        columns = db.client.query(columns_query).result_rows
        
        if not columns:
            return {
                'table_name': table_name,
                'exists': True,
                'error': 'No columns found'
            }

        # Convert columns to list of dicts
        columns_info = [{'name': col[0], 'type': col[1]} for col in columns]

        # Determine timestamp column
        timestamp_col = 'sip_timestamp' if table_name in ['stock_trades', 'stock_quotes'] else 'timestamp'
        
        # Verify timestamp column exists
        if not any(col['name'] == timestamp_col for col in columns_info):
            return {
                'table_name': table_name,
                'exists': True,
                'columns': columns_info,
                'error': f'No {timestamp_col} column found'
            }

        # Get table statistics
        stats_query = f"""
            SELECT 
                count(*) as total_rows,
                count(DISTINCT ticker) as unique_tickers,
                min({timestamp_col}) as earliest_date,
                max({timestamp_col}) as latest_date
            FROM {db.database}.{table_name}
        """
        stats = db.client.query(stats_query)
        
        # Convert stats to dict
        stats_dict = {
            'total_rows': stats.result_rows[0][0],
            'unique_tickers': stats.result_rows[0][1],
            'earliest_date': stats.result_rows[0][2],
            'latest_date': stats.result_rows[0][3]
        }
        
        return {
            'table_name': table_name,
            'exists': True,
            'stats': stats_dict,
            'columns': columns_info
        }

    except Exception as e:
        return {
            'table_name': table_name,
            'exists': True,
            'error': str(e)
        }

def count_duplicates(db: ClickHouseDB, table_name: str) -> dict:
    """
    Count duplicate rows in a table based on timestamp only.
    For stock_indicators table, count based on timestamp and indicator_type combination.
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

        # Get counts and timestamp range in a single query
        if table_name == 'stock_indicators':
            count_query = f"""
                SELECT 
                    count() as total,
                    (
                        SELECT count()
                        FROM (
                            SELECT DISTINCT {timestamp_col}, indicator_type
                            FROM {db.database}.{table_name}
                            SETTINGS 
                                max_memory_usage = 80000000000,
                                max_bytes_before_external_group_by = 20000000000,
                                group_by_overflow_mode = 'any'
                        )
                    ) as unique_combinations,
                    min({timestamp_col}) as earliest_timestamp,
                    max({timestamp_col}) as latest_timestamp
                FROM {db.database}.{table_name}
            """
        else:
            count_query = f"""
                SELECT 
                    count() as total,
                    count(DISTINCT {timestamp_col}) as unique_timestamps,
                    min({timestamp_col}) as earliest_timestamp,
                    max({timestamp_col}) as latest_timestamp
                FROM {db.database}.{table_name}
            """
        result = db.client.query(count_query)
        
        if not result.result_rows:
            return {'table': table_name, 'error': 'No results returned'}
            
        total_rows = result.result_rows[0][0]
        unique_count = result.result_rows[0][1]
        earliest_timestamp = result.result_rows[0][2]
        latest_timestamp = result.result_rows[0][3]
        duplicate_count = total_rows - unique_count
        duplicate_percent = (duplicate_count / total_rows * 100) if total_rows > 0 else 0
        
        return {
            'table': table_name,
            'total_rows': total_rows,
            'unique_timestamps': unique_count,
            'duplicate_rows': duplicate_count,
            'duplicate_percent': duplicate_percent,
            'timestamp_column': timestamp_col,
            'earliest_timestamp': earliest_timestamp,
            'latest_timestamp': latest_timestamp
        }
        
    except Exception as e:
        return {'table': table_name, 'error': str(e)}

def analyze_all_tables():
    """Generate a simple readout of all tables in the database with focus on duplicates"""
    db = ClickHouseDB()
    
    # List of tables to analyze
    tables = [
        'stock_bars',
        'stock_daily',
        'stock_trades',
        'stock_quotes',
        'stock_indicators',
        'stock_master',
        'stock_normalized',
        'stock_news'
    ]
    
    print("\nClickHouse Database Table Analysis")
    print("=" * 50)
    
    for table in tables:
        print(f"\nAnalyzing table: {table}")
        print("-" * 30)
        
        # Count duplicates
        dup_info = count_duplicates(db, table)
        if 'error' in dup_info:
            print(f"Error: {dup_info['error']}")
        else:
            print(f"Total Rows: {dup_info['total_rows']:,}")
            print(f"Unique Timestamps: {dup_info['unique_timestamps']:,}")
            print(f"Duplicate Rows: {dup_info['duplicate_rows']:,}")
            print(f"Duplicate Percentage: {dup_info['duplicate_percent']:.2f}%")
            print(f"Using Timestamp Column: {dup_info['timestamp_column']}")
            print(f"Earliest Timestamp: {dup_info['earliest_timestamp']}")
            print(f"Latest Timestamp: {dup_info['latest_timestamp']}")
        
        print("-" * 30)
    
    db.close()

def analyze_single_table_duplicates(db: ClickHouseDB, table_name: str, ticker_col: str = None, timestamp_col: str = None) -> None:
    """
    Analyze duplicates in a single table with more detailed information.
    
    Args:
        db: ClickHouseDB instance
        table_name: Name of the table to analyze
        ticker_col: Optional override for ticker column name
        timestamp_col: Optional override for timestamp column name
    """
    print(f"\nDetailed Duplicate Analysis for Table: {table_name}")
    print("=" * 50)
    
    # First check if table exists
    exists_query = f"""
    SELECT count(*) as cnt
    FROM system.tables 
    WHERE database = '{db.database}' AND name = '{table_name}'
    """
    exists_result = db.client.query(exists_query)
    if exists_result.result_rows[0][0] == 0:
        print(f"Error: Table {table_name} does not exist")
        return
    
    # Get columns
    columns_query = f"""
        SELECT name
        FROM system.columns
        WHERE database = '{db.database}' AND table = '{table_name}'
    """
    columns = [row[0] for row in db.client.command(columns_query)]
    
    # Determine timestamp column
    if timestamp_col is None:
        timestamp_col = 'sip_timestamp' if table_name in ['stock_trades', 'stock_quotes'] else 'timestamp'
        if timestamp_col not in columns:
            # Try to find an alternative timestamp column
            timestamp_alternatives = ['t', 'time', 'date', 'datetime', 'created_at', 'updated', 'ex_date']
            for alt in timestamp_alternatives:
                if alt in columns:
                    timestamp_col = alt
                    break
            else:
                print(f"Error: No timestamp column found in {table_name}")
                print(f"Available columns: {', '.join(columns)}")
                return
    elif timestamp_col not in columns:
        print(f"Error: Specified timestamp column '{timestamp_col}' not found in {table_name}")
        print(f"Available columns: {', '.join(columns)}")
        return
    
    # Determine ticker column
    if ticker_col is None:
        ticker_col = 'ticker'
        if ticker_col not in columns:
            # Try to find an alternative ticker column
            ticker_alternatives = ['tickers', 'symbol', 'symbols']
            for alt in ticker_alternatives:
                if alt in columns:
                    ticker_col = alt
                    break
            else:
                print(f"Error: No ticker column found in {table_name}")
                print(f"Available columns: {', '.join(columns)}")
                return
    elif ticker_col not in columns:
        print(f"Error: Specified ticker column '{ticker_col}' not found in {table_name}")
        print(f"Available columns: {', '.join(columns)}")
        return
    
    print(f"Using ticker column: {ticker_col}")
    print(f"Using timestamp column: {timestamp_col}")
    
    # Count total rows
    total_query = f"SELECT count() as total FROM {db.database}.{table_name}"
    total_result = db.client.command(total_query)
    total_rows = total_result[0]['total']
    print(f"Total rows: {total_rows:,}")
    
    # Count unique combinations based on table type
    if table_name == 'stock_indicators':
        unique_query = f"""
            SELECT count() as unique_count
            FROM (
                SELECT {ticker_col}, {timestamp_col}, indicator_type
                FROM {db.database}.{table_name}
                GROUP BY {ticker_col}, {timestamp_col}, indicator_type
                SETTINGS 
                    max_memory_usage = 80000000000,
                    max_bytes_before_external_group_by = 20000000000,
                    group_by_overflow_mode = 'any'
            )
        """
    else:
        unique_query = f"""
            SELECT count() as unique_count
            FROM (
                SELECT {ticker_col}, {timestamp_col}
                FROM {db.database}.{table_name}
                GROUP BY {ticker_col}, {timestamp_col}
                SETTINGS 
                    max_memory_usage = 80000000000,
                    max_bytes_before_external_group_by = 20000000000,
                    group_by_overflow_mode = 'any'
            )
        """
    unique_result = db.client.command(unique_query)
    unique_rows = unique_result[0]['unique_count']
    
    if table_name == 'stock_indicators':
        print(f"Unique rows (by {ticker_col}, {timestamp_col}, and indicator_type): {unique_rows:,}")
    else:
        print(f"Unique rows (by {ticker_col} and {timestamp_col}): {unique_rows:,}")
    
    # Calculate duplicates
    duplicate_count = total_rows - unique_rows
    duplicate_percent = (duplicate_count / total_rows * 100) if total_rows > 0 else 0
    print(f"Duplicate rows: {duplicate_count:,} ({duplicate_percent:.2f}%)")
    
    if duplicate_count > 0:
        # Find tickers with the most duplicates
        if table_name == 'stock_indicators':
            dup_by_ticker_query = f"""
                WITH counts AS (
                    SELECT 
                        {ticker_col},
                        count(*) as total,
                        count(DISTINCT ({timestamp_col}, indicator_type)) as unique_times
                    FROM {db.database}.{table_name}
                    GROUP BY {ticker_col}
                    SETTINGS 
                        max_memory_usage = 80000000000,
                        max_bytes_before_external_group_by = 20000000000,
                        group_by_overflow_mode = 'any'
                )
                SELECT 
                    {ticker_col},
                    total,
                    unique_times,
                    total - unique_times as duplicates,
                    (total - unique_times) / total * 100 as duplicate_percent
                FROM counts
                WHERE total > unique_times
                ORDER BY duplicates DESC
                LIMIT 10
            """
        else:
            dup_by_ticker_query = f"""
                WITH counts AS (
                    SELECT 
                        {ticker_col},
                        count(*) as total,
                        count(DISTINCT {timestamp_col}) as unique_times
                    FROM {db.database}.{table_name}
                    GROUP BY {ticker_col}
                    SETTINGS 
                        max_memory_usage = 80000000000,
                        max_bytes_before_external_group_by = 20000000000,
                        group_by_overflow_mode = 'any'
                )
                SELECT 
                    {ticker_col},
                    total,
                    unique_times,
                    total - unique_times as duplicates,
                    (total - unique_times) / total * 100 as duplicate_percent
                FROM counts
                WHERE total > unique_times
                ORDER BY duplicates DESC
                LIMIT 10
            """
        
        try:
            dup_by_ticker = db.client.command(dup_by_ticker_query)
            
            if dup_by_ticker:
                print("\nTop 10 tickers with duplicates:")
                print(f"{'Ticker':<10} {'Total':<10} {'Unique':<10} {'Duplicates':<10} {'Percent':<10}")
                print("-" * 50)
                
                for row in dup_by_ticker:
                    ticker = row[ticker_col]
                    total = row['total']
                    unique = row['unique_times']
                    dups = row['duplicates']
                    percent = row['duplicate_percent']
                    print(f"{ticker:<10} {total:<10,} {unique:<10,} {dups:<10,} {percent:<10.2f}%")
        except Exception as e:
            print(f"Error analyzing duplicates by ticker: {str(e)}")
        
        # Sample some duplicate rows
        if table_name == 'stock_indicators':
            sample_query = f"""
                WITH counts AS (
                    SELECT 
                        {ticker_col},
                        {timestamp_col},
                        indicator_type,
                        count(*) as cnt
                    FROM {db.database}.{table_name}
                    GROUP BY {ticker_col}, {timestamp_col}, indicator_type
                    SETTINGS 
                        max_memory_usage = 80000000000,
                        max_bytes_before_external_group_by = 20000000000,
                        group_by_overflow_mode = 'any'
                )
                SELECT 
                    c.{ticker_col},
                    c.{timestamp_col},
                    c.indicator_type,
                    c.cnt as duplicate_count
                FROM counts c
                WHERE c.cnt > 1
                ORDER BY c.cnt DESC
                LIMIT 5
            """
        else:
            sample_query = f"""
                WITH counts AS (
                    SELECT 
                        {ticker_col},
                        {timestamp_col},
                        count(*) as cnt
                    FROM {db.database}.{table_name}
                    GROUP BY {ticker_col}, {timestamp_col}
                    SETTINGS 
                        max_memory_usage = 80000000000,
                        max_bytes_before_external_group_by = 20000000000,
                        group_by_overflow_mode = 'any'
                )
                SELECT 
                    c.{ticker_col},
                    c.{timestamp_col},
                    c.cnt as duplicate_count
                FROM counts c
                WHERE c.cnt > 1
                ORDER BY c.cnt DESC
                LIMIT 5
            """
        
        try:
            sample_dups = db.client.command(sample_query)
            
            if sample_dups:
                print("\nSample duplicate entries:")
                if table_name == 'stock_indicators':
                    print(f"{'Ticker':<10} {'Timestamp':<25} {'Type':<15} {'Count':<10}")
                    print("-" * 60)
                    
                    for row in sample_dups:
                        ticker = row[ticker_col]
                        ts = row[timestamp_col]
                        ind_type = row['indicator_type']
                        cnt = row['duplicate_count']
                        print(f"{ticker:<10} {str(ts):<25} {ind_type:<15} {cnt:<10}")
                else:
                    print(f"{'Ticker':<10} {'Timestamp':<25} {'Count':<10}")
                    print("-" * 50)
                    
                    for row in sample_dups:
                        ticker = row[ticker_col]
                        ts = row[timestamp_col]
                        cnt = row['duplicate_count']
                        print(f"{ticker:<10} {str(ts):<25} {cnt:<10}")
        except Exception as e:
            print(f"Error getting sample duplicates: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze ClickHouse database tables')
    parser.add_argument('--table', type=str, help='Analyze a specific table')
    parser.add_argument('--ticker-col', type=str, help='Override ticker column name')
    parser.add_argument('--timestamp-col', type=str, help='Override timestamp column name')
    args = parser.parse_args()
    
    db = ClickHouseDB()
    
    if args.table:
        analyze_single_table_duplicates(db, args.table, args.ticker_col, args.timestamp_col)
        db.close()
    else:
        analyze_all_tables() 