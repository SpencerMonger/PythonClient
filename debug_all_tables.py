from endpoints.db import ClickHouseDB
from datetime import datetime
from typing import Dict, List
import argparse
from dateutil.relativedelta import relativedelta
from collections import defaultdict

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
    Count duplicate rows in a table based on uni_id.
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
            
        # Get counts and timestamp range in a single query
        count_query = f"""
            SELECT 
                count() as total,
                count(DISTINCT uni_id) as unique_ids,
                min({timestamp_col}) as earliest_timestamp,
                max({timestamp_col}) as latest_timestamp
            FROM {db.database}.{table_name}
            SETTINGS 
                max_memory_usage = 80000000000,
                max_bytes_before_external_group_by = 20000000000,
                group_by_overflow_mode = 'any'
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
            'unique_ids': unique_count,
            'duplicate_rows': duplicate_count,
            'duplicate_percent': duplicate_percent,
            'earliest_timestamp': earliest_timestamp,
            'latest_timestamp': latest_timestamp
        }
        
    except Exception as e:
        return {'table': table_name, 'error': str(e)}

def count_duplicates_chunked(db: ClickHouseDB, table_name: str, chunk_size_months: int = 1) -> dict:
    """
    Count duplicate rows in a large table based on uni_id, processing in time chunks.
    """
    print(f"Counting duplicates in chunks for {table_name}...")
    timestamp_col = 'sip_timestamp' # Specific to trades/quotes
    total_rows_agg = 0
    total_unique_agg = 0 # Note: This will sum unique counts per chunk, not global unique
    earliest_timestamp_overall = None
    latest_timestamp_overall = None
    processed_chunks = 0

    try:
        # 1. Get overall time range
        range_query = f"""
            SELECT min({timestamp_col}) as min_ts, max({timestamp_col}) as max_ts
            FROM {db.database}.{table_name}
        """
        range_result = db.client.query(range_query).result_rows
        if not range_result or range_result[0][0] is None:
            return {'table': table_name, 'error': 'Could not determine time range or table is empty'}
        
        start_date = range_result[0][0].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date_overall = range_result[0][1]
        earliest_timestamp_overall = range_result[0][0]
        latest_timestamp_overall = end_date_overall

        current_start = start_date
        
        # 2. Iterate through chunks
        while current_start <= end_date_overall:
            chunk_start = current_start
            chunk_end = current_start + relativedelta(months=chunk_size_months)
            print(f"  Processing chunk: {chunk_start.strftime('%Y-%m')}...")

            count_query_chunk = f"""
                SELECT 
                    count() as total,
                    count(DISTINCT uni_id) as unique_ids
                FROM {db.database}.{table_name}
                WHERE {timestamp_col} >= '{chunk_start}' AND {timestamp_col} < '{chunk_end}'
                SETTINGS 
                    max_memory_usage = 80000000000,
                    max_bytes_before_external_group_by = 20000000000,
                    group_by_overflow_mode = 'any' 
            """
            
            try:
                result_chunk = db.client.query(count_query_chunk).result_rows
                if result_chunk:
                    total_rows_chunk = result_chunk[0][0]
                    unique_count_chunk = result_chunk[0][1]
                    total_rows_agg += total_rows_chunk
                    # We add unique counts per chunk. This isn't globally unique.
                    # A true global unique count would require different, potentially heavy, logic.
                    total_unique_agg += unique_count_chunk 
                    processed_chunks += 1
            except Exception as chunk_e:
                 print(f"  Error processing chunk {chunk_start.strftime('%Y-%m')}: {str(chunk_e)}")
                 # Optionally decide whether to continue or stop
            
            current_start = chunk_end

        if processed_chunks == 0:
            return {'table': table_name, 'error': 'No chunks processed successfully'}

        # Approximation: Calculate duplicates based on sum of chunk uniques
        # This will likely OVERESTIMATE the number of duplicates compared to a global distinct count.
        approx_duplicate_count = total_rows_agg - total_unique_agg
        approx_duplicate_percent = (approx_duplicate_count / total_rows_agg * 100) if total_rows_agg > 0 else 0
        
        print(f"Finished counting duplicates for {table_name}. Processed {processed_chunks} chunks.")
        return {
            'table': table_name,
            'total_rows': total_rows_agg,
            'unique_ids': f"~ {total_unique_agg:,} (Sum of chunk uniques)", # Indicate it's an approximation
            'duplicate_rows': f"~ {approx_duplicate_count:,} (Approximation)",
            'duplicate_percent': f"~ {approx_duplicate_percent:.2f}% (Approximation)",
            'earliest_timestamp': earliest_timestamp_overall,
            'latest_timestamp': latest_timestamp_overall
        }
        
    except Exception as e:
        return {'table': table_name, 'error': f"Chunked processing error: {str(e)}"}

def analyze_ticker_data(db: ClickHouseDB, table_name: str) -> List[Dict]:
    """
    Analyze data for each unique ticker in a table, showing time span and row count.
    """
    try:
        # Determine timestamp column based on table name
        timestamp_col = 'sip_timestamp' if table_name in ['stock_trades', 'stock_quotes'] else 'timestamp'
        
        # Query to get ticker-specific stats
        query = f"""
            SELECT 
                ticker,
                count(*) as total_rows,
                min({timestamp_col}) as earliest_timestamp,
                max({timestamp_col}) as latest_timestamp,
                count(DISTINCT toDate({timestamp_col})) as unique_days
            FROM {db.database}.{table_name}
            GROUP BY ticker
            ORDER BY ticker ASC
            SETTINGS 
                max_memory_usage = 80000000000,
                max_bytes_before_external_group_by = 20000000000,
                group_by_overflow_mode = 'any'
        """
        
        result = db.client.query(query)
        return result.result_rows
    except Exception as e:
        print(f"Error analyzing ticker data for {table_name}: {str(e)}")
        return []

def find_non_numeric_values(db: ClickHouseDB, table_name: str) -> List[Dict]:
    """
    Identifies non-numeric, NaN, or Infinite values in columns expected to be numeric.
    """
    print(f"\nChecking for non-numeric/Inf/NaN values in: {table_name}")
    print("-" * 30)
    
    issues_found = []
    try:
        # Get columns and types
        columns_query = f"""
            SELECT name, type
            FROM system.columns
            WHERE database = '{db.database}' AND table = '{table_name}'
        """
        columns = db.client.query(columns_query).result_rows
        
        if not columns:
            print(f"Warning: Could not retrieve columns for {table_name}")
            return []

        for col_name, col_type in columns:
            # Target numeric types (adjust list as needed)
            is_numeric_type = (
                col_type.startswith('Float') or 
                col_type.startswith('Int') or 
                col_type.startswith('UInt') or 
                col_type.startswith('Decimal')
            )
            
            if is_numeric_type:
                # Construct the check query
                check_query = f"""
                    SELECT
                        count() as bad_row_count,
                        -- Get a sample of distinct problematic values as strings
                        arrayStringConcat(arraySlice(groupUniqArray(toString({col_name})), 1, 10), ', ') as sample_bad_values 
                    FROM {db.database}.{table_name}
                    WHERE
                        ( 
                          -- Check for strings that cannot be parsed as float, but are not NULL themselves
                          toFloat64OrNull(toString({col_name})) IS NULL AND NOT isNull({col_name})
                        )
                        OR
                        ( 
                          -- Only check for Inf/NaN if the type is actually Float
                          '{col_type}' LIKE 'Float%' AND (isInfinite({col_name}) OR isNaN({col_name}))
                        )
                    SETTINGS 
                        max_memory_usage = 80000000000,
                        max_bytes_before_external_group_by = 20000000000,
                        group_by_overflow_mode = 'any',
                        max_block_size = 65505 -- Add setting often useful with groupUniqArray
                """
                
                try:
                    result = db.client.query(check_query).result_rows
                    if result:
                        bad_count = result[0][0]
                        sample_values = result[0][1]
                        if bad_count > 0:
                            print(f"  - Column '{col_name}' ({col_type}): Found {bad_count:,} problematic rows.")
                            print(f"    Sample values: [{sample_values}]")
                            issues_found.append({
                                'table': table_name,
                                'column': col_name,
                                'type': col_type,
                                'bad_count': bad_count,
                                'sample_values': sample_values
                            })
                except Exception as query_e:
                    # Handle potential errors if toString or toFloat64OrNull fails on complex types not excluded
                    if "Cannot parse string" in str(query_e) or "Cannot convert" in str(query_e) or "ILLEGAL_COLUMN" in str(query_e):
                         print(f"  - Column '{col_name}' ({col_type}): Could not run check (possibly complex type or conversion issue). Error: {str(query_e)[:100]}...")
                    else:
                        print(f"  - Column '{col_name}' ({col_type}): Error during check: {str(query_e)[:150]}...")
                        
    except Exception as e:
        print(f"Error retrieving columns or running checks for {table_name}: {str(e)}")
        
    if not issues_found:
        print("  No non-numeric/Inf/NaN issues found in numeric columns.")
        
    return issues_found

def find_non_numeric_values_chunked(db: ClickHouseDB, table_name: str, chunk_size_months: int = 1) -> List[Dict]:
    """
    Identifies non-numeric, NaN, or Infinite values in columns expected to be numeric,
    processing large tables in time chunks.
    """
    print(f"\nChecking for non-numeric/Inf/NaN values in chunks for: {table_name}")
    print("-" * 30)
    timestamp_col = 'sip_timestamp' # Specific to trades/quotes
    aggregated_issues = defaultdict(lambda: {'bad_count': 0, 'sample_values': set()})
    processed_chunks = 0
    
    try:
        # 1. Get overall time range
        range_query = f"""
            SELECT min({timestamp_col}) as min_ts, max({timestamp_col}) as max_ts
            FROM {db.database}.{table_name}
        """
        range_result = db.client.query(range_query).result_rows
        if not range_result or range_result[0][0] is None:
            print(f"Warning: Could not determine time range or table {table_name} is empty.")
            return []
            
        start_date = range_result[0][0].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date_overall = range_result[0][1]

        # 2. Get columns and types once
        columns_query = f"""
            SELECT name, type
            FROM system.columns
            WHERE database = '{db.database}' AND table = '{table_name}'
        """
        columns = db.client.query(columns_query).result_rows
        if not columns:
            print(f"Warning: Could not retrieve columns for {table_name}")
            return []

        numeric_columns = []
        for col_name, col_type in columns:
             is_numeric_type = (
                col_type.startswith('Float') or 
                col_type.startswith('Int') or 
                col_type.startswith('UInt') or 
                col_type.startswith('Decimal')
             )
             if is_numeric_type:
                 numeric_columns.append((col_name, col_type))
        
        if not numeric_columns:
            print("  No numeric columns found to check.")
            return []

        # 3. Iterate through chunks
        current_start = start_date
        while current_start <= end_date_overall:
            chunk_start = current_start
            chunk_end = current_start + relativedelta(months=chunk_size_months)
            print(f"  Checking chunk: {chunk_start.strftime('%Y-%m')}...")

            for col_name, col_type in numeric_columns:
                # Construct the check query for the chunk
                check_query_chunk = f"""
                    SELECT
                        count() as bad_row_count,
                        arrayStringConcat(arraySlice(groupUniqArray(toString({col_name})), 1, 10), ', ') as sample_bad_values 
                    FROM {db.database}.{table_name}
                    WHERE 
                        ({timestamp_col} >= '{chunk_start}' AND {timestamp_col} < '{chunk_end}')
                        AND 
                        ( 
                          (toFloat64OrNull(toString({col_name})) IS NULL AND NOT isNull({col_name}))
                          OR
                          ('{col_type}' LIKE 'Float%' AND (isInfinite({col_name}) OR isNaN({col_name})))
                        )
                    SETTINGS 
                        max_memory_usage = 80000000000,
                        max_bytes_before_external_group_by = 20000000000,
                        group_by_overflow_mode = 'any',
                        max_block_size = 65505 
                """
                
                try:
                    result_chunk = db.client.query(check_query_chunk).result_rows
                    if result_chunk:
                        bad_count_chunk = result_chunk[0][0]
                        sample_values_chunk = result_chunk[0][1]
                        if bad_count_chunk > 0:
                            key = (col_name, col_type)
                            aggregated_issues[key]['bad_count'] += bad_count_chunk
                            # Add samples, ensuring not to store too many
                            current_samples = aggregated_issues[key]['sample_values']
                            if len(current_samples) < 10:
                                # Add new samples from the chunk string if not already present
                                new_samples = set(s.strip() for s in sample_values_chunk.split(',') if s.strip())
                                for sample in new_samples:
                                    if len(current_samples) < 10:
                                        current_samples.add(sample)
                                    else:
                                        break
                                        
                except Exception as query_e:
                    print(f"    Error checking {col_name} in chunk {chunk_start.strftime('%Y-%m')}: {str(query_e)[:150]}...")
            
            processed_chunks +=1
            current_start = chunk_end

        print(f"Finished checking non-numeric values for {table_name}. Processed {processed_chunks} chunks.")

        # 4. Format and print aggregated results
        final_issues = []
        if aggregated_issues:
            print("\nAggregated Non-Numeric/Inf/NaN Issues:")
            for (col_name, col_type), data in aggregated_issues.items():
                 print(f"  - Column '{col_name}' ({col_type}): Found {data['bad_count']:,} problematic rows across all chunks.")
                 sample_str = ", ".join(sorted(list(data['sample_values'])))
                 print(f"    Sample values: [{sample_str}]")
                 final_issues.append({
                    'table': table_name,
                    'column': col_name,
                    'type': col_type,
                    'bad_count': data['bad_count'],
                    'sample_values': sample_str
                 })
        elif processed_chunks > 0:
            print("  No non-numeric/Inf/NaN issues found in numeric columns across all chunks.")
        else:
            print("  No chunks were processed for non-numeric checks.")
            
        return final_issues
        
    except Exception as e:
        print(f"Error during chunked non-numeric check for {table_name}: {str(e)}")
        return []

def analyze_all_tables():
    """Generate a simple readout of all tables in the database with focus on duplicates and ticker data"""
    db = ClickHouseDB()
    
    # List of tables to analyze
    tables = [
        'stock_bars',
        'stock_daily',
        'stock_trades', # Large table
        'stock_quotes', # Large table
        'stock_indicators',
        'stock_master',
        'stock_normalized',
        'stock_predictions'
    ]
    large_tables = {'stock_trades', 'stock_quotes'} # Set for quick lookup
    
    print("\nClickHouse Database Table Analysis")
    print("=" * 50)
    
    for table in tables:
        print(f"\nAnalyzing table: {table}")
        print("=" * 30)
        
        # --- MODIFIED: Use chunked function for large tables ---
        if table in large_tables:
            dup_info = count_duplicates_chunked(db, table)
        else:
            dup_info = count_duplicates(db, table)
        # --- END MODIFIED ---
            
        if 'error' in dup_info:
            print(f"Error counting duplicates: {dup_info['error']}")
            # Continue to next analysis step even if duplicates fail
        else:
            print(f"Total Rows: {dup_info['total_rows']:,}")
            # Handle potentially approximate unique/duplicate counts
            unique_str = str(dup_info['unique_ids']) if isinstance(dup_info['unique_ids'], str) else f"{dup_info['unique_ids']:,}"
            dups_str = str(dup_info['duplicate_rows']) if isinstance(dup_info['duplicate_rows'], str) else f"{dup_info['duplicate_rows']:,}"
            percent_str = str(dup_info['duplicate_percent']) if isinstance(dup_info['duplicate_percent'], str) else f"{dup_info['duplicate_percent']:.2f}%"
            print(f"Unique IDs: {unique_str}")
            print(f"Duplicate Rows: {dups_str}")
            print(f"Duplicate Percentage: {percent_str}")
        
        # --- MODIFIED: Use chunked function for large tables (if needed) ---
        # For now, keep original analyze_ticker_data, but add error handling
        print("\nTicker Analysis:")
        print("-" * 30)
        try:
            if table in large_tables:
                 # If analyze_ticker_data fails, implement analyze_ticker_data_chunked
                 print("  (Skipping detailed ticker analysis for large table due to potential memory limits)")
                 ticker_data = [] # Or call a chunked version if implemented
            else:
                 ticker_data = analyze_ticker_data(db, table)
                
            if ticker_data:
                print(f"{ 'Ticker':<10} {'Total Rows':<12} {'Start Date':<25} {'End Date':<25} {'Days':<8}")
                print("-" * 80)
                for row in ticker_data:
                    ticker = row[0]
                    total = row[1]
                    start = row[2]
                    end = row[3]
                    days = row[4]
                    print(f"{ticker:<10} {total:<12,} {str(start):<25} {str(end):<25} {days:<8,}")
                print("-" * 30)
            else:
                 print("  No ticker data returned or analysis skipped.")
                 
        except Exception as ticker_e:
             print(f"  Error during ticker analysis: {str(ticker_e)}")
             print("  (Consider implementing a chunked version if this fails consistently)")
        # --- END MODIFIED ---
        
        # --- MODIFIED: Use chunked function for large tables ---
        if table in large_tables:
            find_non_numeric_values_chunked(db, table)
        else:
            find_non_numeric_values(db, table)
        # --- END MODIFIED ---
    
    db.close()

def analyze_single_table_duplicates(db: ClickHouseDB, table_name: str, ticker_col: str = None, timestamp_col: str = None) -> None:
    """
    Analyze duplicates in a single table with more detailed information.
    (Note: This function still uses potentially memory-intensive queries for duplicate details)
    """
    print(f"\nDetailed Analysis for Table: {table_name}")
    print("=" * 50)

    large_tables = {'stock_trades', 'stock_quotes'} 
    is_large_table = table_name in large_tables
    
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
    
    # --- MODIFIED: Use chunked function for large tables for basic counts ---
    if is_large_table:
        dup_info = count_duplicates_chunked(db, table_name)
        if 'error' in dup_info:
            print(f"Error counting duplicates: {dup_info['error']}")
            # Fallback or stop?
        else:
            # Print approximate results from chunked function
            print(f"Total rows: {dup_info['total_rows']:,}")
            print(f"Unique IDs: {dup_info['unique_ids']}") # Already formatted as approximate
            print(f"Duplicate rows: {dup_info['duplicate_rows']}") # Already formatted as approximate
            print(f"Duplicate Percentage: {dup_info['duplicate_percent']}") # Already formatted as approximate
            print(f"(Counts based on chunked processing; uniqueness is approximate)")
            
            # Detailed duplicate analysis (by ticker, samples) might still fail on large tables
            print("\nDetailed duplicate analysis (by ticker, sample) may still hit memory limits.")
            # Optionally skip the detailed duplicate queries below for large tables
            # or accept they might fail.

    else:
        # --- Use original logic for smaller tables --- 
        # Count total rows (unchanged)
        total_query = f"SELECT count() as total FROM {db.database}.{table_name}"
        total_result = db.client.command(total_query)
        total_rows = total_result[0]['total']
        print(f"Total rows: {total_rows:,}")
        
        # Count unique combinations (unchanged)
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
        duplicate_count = total_rows - unique_rows
        duplicate_percent = (duplicate_count / total_rows * 100) if total_rows > 0 else 0
        print(f"Duplicate rows: {duplicate_count:,} ({duplicate_percent:.2f}%)")
    # --- END MODIFIED ---
    
    # --- Detailed duplicate analysis (by ticker, samples) --- 
    # This section remains largely unchanged, but add warnings for large tables
    if (not is_large_table and duplicate_count > 0) or (is_large_table and '> 0' in dup_info.get('duplicate_rows', '0')):
        if is_large_table:
             print("\nAttempting detailed duplicate analysis (might fail due to memory limits)...")
             
        # Find tickers with most duplicates (unchanged query logic)
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
            print(f"Error analyzing duplicates by ticker (potentially due to memory limits): {str(e)}")
        
        # Sample some duplicate rows (unchanged query logic)
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
                print(f"{'Ticker':<10} {'Timestamp':<25} {'Count':<10}")
                print("-" * 50)
                
                for row in sample_dups:
                    ticker = row[ticker_col]
                    ts = row[timestamp_col]
                    cnt = row['duplicate_count']
                    print(f"{ticker:<10} {str(ts):<25} {cnt:<10}")
        except Exception as e:
            print(f"Error getting sample duplicates (potentially due to memory limits): {str(e)}")

    # --- MODIFIED: Use chunked function for large tables ---
    if is_large_table:
        find_non_numeric_values_chunked(db, table_name)
    else:
        find_non_numeric_values(db, table_name)
    # --- END MODIFIED ---

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