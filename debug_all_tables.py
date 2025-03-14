from endpoints.db import ClickHouseDB
from datetime import datetime
from typing import Dict, List

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
            (SELECT count(DISTINCT ticker || toString(timestamp)) FROM stock_indicators) as source_count,
            abs(count(*) - (SELECT count(DISTINCT ticker || toString(timestamp)) FROM stock_indicators)) as difference
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

def analyze_all_tables():
    """Generate a simple readout of all tables in the database"""
    db = ClickHouseDB()
    
    # List of tables to analyze
    tables = [
        'stock_bars',
        'stock_daily',
        'stock_trades',
        'stock_quotes',
        'stock_indicators',
        'stock_master',
        'stock_normalized'
    ]
    
    print("\nClickHouse Database Table Analysis")
    print("=" * 50)
    
    for table in tables:
        info = get_table_info(db, table)
        
        print(f"\nTable: {info['table_name']}")
        print("-" * 30)
        
        if 'error' in info:
            print(f"Error: {info['error']}")
            print("-" * 30)
            continue
            
        if 'stats' in info:
            print(f"Total Rows: {info['stats']['total_rows']:,}")
            print(f"Unique Tickers: {info['stats']['unique_tickers']:,}")
            print(f"Date Range: {info['stats']['earliest_date']} to {info['stats']['latest_date']}")
        
        if 'columns' in info:
            print("\nColumns:")
            for col in info['columns']:
                print(f"  - {col['name']} ({col['type']})")
        
        print("-" * 30)
    
    db.close()

if __name__ == "__main__":
    analyze_all_tables() 