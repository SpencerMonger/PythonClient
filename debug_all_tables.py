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

def analyze_all_tables():
    db = ClickHouseDB()
    
    # List of tables to check
    tables = [
        'stock_bars',
        'stock_daily',
        'stock_trades',
        'stock_quotes',
        'stock_news',
        'stock_indicators',
        'stock_master'
    ]
    
    for table in tables:
        print(f"\n{'='*50}")
        print(f"Analyzing {table}")
        print('='*50)
        
        # Get date column name
        date_col = 'sip_timestamp' if table in ['stock_trades', 'stock_quotes'] else 'timestamp'
        if table == 'stock_news':
            date_col = 'published_utc'
        
        # Get total rows and duplicates
        if table == 'stock_news':
            total_rows_query = f"""
            SELECT 
                count(*) as total_rows,
                count(*) - count(DISTINCT (arrayJoin(tickers), {date_col})) as duplicate_rows
            FROM {table}
            """
        else:
            total_rows_query = f"""
            SELECT 
                count(*) as total_rows,
                count(*) - count(DISTINCT (ticker, {date_col})) as duplicate_rows
            FROM {table}
            """
        total_stats = db.client.command(total_rows_query)
        print("\nRow statistics:")
        print(f"Total rows: {total_stats[0]}")
        print(f"Duplicate rows: {total_stats[1]}")
        
        # Check date range
        date_range = db.client.command(f"""
            SELECT 
                min({date_col}) as min_date,
                max({date_col}) as max_date
            FROM {table}
        """)
        print("\nDate range:", date_range)
        
        # Check daily counts with formatted date
        if table == 'stock_news':
            daily_counts = db.client.command(f"""
                SELECT 
                    formatDateTime(toDate({date_col}), '%m-%d') as date,
                    count(*) as total_rows,
                    count(DISTINCT arrayJoin(tickers)) as unique_tickers
                FROM {table}
                GROUP BY toDate({date_col})
                ORDER BY toDate({date_col})
            """)
        else:
            daily_counts = db.client.command(f"""
                SELECT 
                    formatDateTime(toDate({date_col}), '%m-%d') as date,
                    count(*) as total_rows,
                    count(DISTINCT ticker) as unique_tickers
                FROM {table}
                GROUP BY toDate({date_col})
                ORDER BY toDate({date_col})
            """)
        print("\nDaily counts:", daily_counts)
        
        # Special handling for each table type
        if table == 'stock_bars':
            # Check for missing prices
            price_stats = db.client.command(f"""
                SELECT 
                    count(*) as total_rows,
                    countIf(open IS NULL) as null_open,
                    countIf(close IS NULL) as null_close,
                    countIf(high IS NULL) as null_high,
                    countIf(low IS NULL) as null_low
                FROM {table}
            """)
            print("\nPrice statistics:", price_stats)
            
        elif table == 'stock_trades':
            # Check trade sizes and prices
            trade_stats = db.client.command(f"""
                SELECT
                    count(*) as total_trades,
                    avg(size) as avg_size,
                    avg(price) as avg_price,
                    count(DISTINCT exchange) as unique_exchanges
                FROM {table}
            """)
            print("\nTrade statistics:", trade_stats)
            
        elif table == 'stock_quotes':
            # Check quote spreads
            quote_stats = db.client.command(f"""
                SELECT
                    count(*) as total_quotes,
                    avg(ask_price - bid_price) as avg_spread,
                    count(DISTINCT ask_exchange) as unique_ask_exchanges,
                    count(DISTINCT bid_exchange) as unique_bid_exchanges
                FROM {table}
            """)
            print("\nQuote statistics:", quote_stats)
            
        elif table == 'stock_indicators':
            # Check indicator types and their counts
            indicator_counts = db.client.command(f"""
                SELECT 
                    indicator_type,
                    count(*) as count
                FROM {table}
                GROUP BY indicator_type
                ORDER BY count DESC
            """)
            print("\nIndicator type counts:", indicator_counts)
            
        elif table == 'stock_news':
            # Check news sources and counts
            news_stats = db.client.command(f"""
                SELECT
                    count(*) as total_news,
                    count(DISTINCT publisher) as unique_publishers,
                    count(DISTINCT id) as unique_articles,
                    count(DISTINCT arrayJoin(tickers)) as unique_tickers
                FROM {table}
            """)
            print("\nNews statistics:", news_stats)
            
        elif table == 'stock_master':
            # Check completeness of master table
            master_stats = db.client.command(f"""
                SELECT
                    count(*) as total_rows,
                    countIf(close IS NOT NULL) as rows_with_price,
                    countIf(quote_count > 0) as rows_with_quotes,
                    countIf(trade_count > 0) as rows_with_trades,
                    countIf(sma_20 IS NOT NULL) as rows_with_indicators
                FROM {table}
            """)
            print("\nMaster table completeness:", master_stats)
        
        # Get sample of tickers and their counts
        if table == 'stock_news':
            ticker_counts = db.client.command(f"""
                SELECT 
                    arrayJoin(tickers) as ticker,
                    count(*) as count
                FROM {table}
                GROUP BY ticker
                ORDER BY count DESC
                LIMIT 5
            """)
        else:
            ticker_counts = db.client.command(f"""
                SELECT 
                    ticker,
                    count(*) as count
                FROM {table}
                GROUP BY ticker
                ORDER BY count DESC
                LIMIT 5
            """)
        print("\nTop 5 tickers by count:", ticker_counts)

if __name__ == "__main__":
    analyze_all_tables() 