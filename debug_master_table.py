import asyncio
from datetime import datetime, timedelta
import pytz
from endpoints.db import ClickHouseDB
from endpoints import config

async def debug_master_table():
    """Debug why new data isn't showing up in the master table"""
    db = ClickHouseDB()
    
    try:
        # Get current time in ET timezone
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        utc_now = now.astimezone(pytz.UTC)
        
        # Check the most recent data in the source tables
        print("\n=== Most Recent Data in Source Tables ===")
        tables = ["stock_bars", "stock_trades", "stock_quotes"]
        
        for table in tables:
            time_field = "timestamp" if table == "stock_bars" else "sip_timestamp"
            
            # Get the most recent entries
            query = f"""
            SELECT 
                ticker, 
                toDateTime({time_field}) as ts,
                formatDateTime(toDateTime({time_field}), '%Y-%m-%d %H:%M:%S') as ts_str
            FROM {db.database}.{table}
            WHERE {time_field} >= now() - INTERVAL 30 MINUTE
            ORDER BY {time_field} DESC
            LIMIT 5
            """
            
            result = db.client.query(query)
            
            if result.result_rows:
                print(f"\n{table} - Recent entries:")
                for row in result.result_rows:
                    print(f"  {row[0]}: {row[1]} ({row[2]})")
            else:
                print(f"\n{table} - No recent entries in the last 30 minutes")
                
                # Check most recent overall
                oldest_query = f"""
                SELECT 
                    ticker, 
                    max(toDateTime({time_field})) as latest_ts,
                    formatDateTime(max(toDateTime({time_field})), '%Y-%m-%d %H:%M:%S') as ts_str
                FROM {db.database}.{table}
                GROUP BY ticker
                ORDER BY latest_ts DESC
                LIMIT 5
                """
                
                oldest_result = db.client.query(oldest_query)
                if oldest_result.result_rows:
                    print(f"  Most recent entries overall:")
                    for row in oldest_result.result_rows:
                        print(f"    {row[0]}: {row[1]} ({row[2]})")
        
        # Now check the insert_latest_data process by examining the SQL in master_v2.py
        print("\n=== Checking Master Table Update Process ===")
        
        # First check if today's rows were deleted first
        today_str = utc_now.strftime('%Y-%m-%d')
        drop_query = f"""
        SELECT COUNT(*) FROM system.query_log 
        WHERE event_time >= now() - INTERVAL 1 HOUR
        AND query LIKE '%ALTER TABLE%{config.TABLE_STOCK_MASTER}%DELETE WHERE toDate(timestamp) = '''{today_str}'''%'
        """
        
        drop_result = db.client.query(drop_query)
        drop_count = drop_result.result_rows[0][0] if drop_result.result_rows else 0
        
        print(f"Found {drop_count} DELETE operations for today's data in the last hour")
        
        # Check if new data was inserted into the master table
        insert_query = f"""
        SELECT COUNT(*) FROM system.query_log 
        WHERE event_time >= now() - INTERVAL 1 HOUR
        AND query LIKE '%INSERT INTO%{config.TABLE_STOCK_MASTER}%'
        """
        
        insert_result = db.client.query(insert_query)
        insert_count = insert_result.result_rows[0][0] if insert_result.result_rows else 0
        
        print(f"Found {insert_count} INSERT operations for the master table in the last hour")
        
        # Check if we have any rows in the master table with today's timestamp
        master_count_query = f"""
        SELECT 
            toHour(timestamp) as hour,
            toMinute(timestamp) as minute,
            count()
        FROM {db.database}.{config.TABLE_STOCK_MASTER}
        WHERE toDate(timestamp) = '{today_str}'
        GROUP BY toHour(timestamp), toMinute(timestamp)
        ORDER BY hour DESC, minute DESC
        """
        
        master_count_result = db.client.query(master_count_query)
        
        print("\nMaster table data by hour/minute:")
        for row in master_count_result.result_rows:
            print(f"  {row[0]:02d}:{row[1]:02d} - {row[2]} rows")
        
        # Try to find what might be wrong with the master table update
        # Check if the source data exists and is being used properly
        print("\n=== Checking Source Data Availability for Master Update ===")
        
        # Check the base_data CTE from master_v2.py
        check_query = f"""
        SELECT
            count() as row_count,
            min(toDateTime(timestamp)) as min_time,
            max(toDateTime(timestamp)) as max_time,
            arrayStringConcat(groupArray(distinct ticker), ', ') as tickers
        FROM {db.database}.stock_bars
        WHERE toDate(timestamp) = '{today_str}' 
        AND timestamp > now() - INTERVAL 1 HOUR
        """
        
        check_result = db.client.query(check_query)
        
        if check_result.result_rows and check_result.result_rows[0][0] > 0:
            print(f"Found {check_result.result_rows[0][0]} rows in stock_bars table for today in the last hour")
            print(f"  Time range: {check_result.result_rows[0][1]} to {check_result.result_rows[0][2]}")
            print(f"  Tickers: {check_result.result_rows[0][3]}")
        else:
            print("No rows found in stock_bars for today in the last hour - this may explain missing master updates!")
        
        # Check the latest successful master table update
        success_query = f"""
        SELECT
            event_time,
            query_duration_ms
        FROM system.query_log
        WHERE query LIKE '%INSERT INTO%{config.TABLE_STOCK_MASTER}%'
        AND event_time >= now() - INTERVAL 2 HOUR
        AND exception = ''
        ORDER BY event_time DESC
        LIMIT 1
        """
        
        success_result = db.client.query(success_query)
        
        if success_result.result_rows:
            print(f"\nLast successful master table update: {success_result.result_rows[0][0]}")
            print(f"  Duration: {success_result.result_rows[0][1]/1000:.2f} seconds")
        else:
            print("\nNo successful master table updates found in the last 2 hours!")
        
        # Check for errors during master table updates
        error_query = f"""
        SELECT
            event_time,
            exception
        FROM system.query_log
        WHERE query LIKE '%INSERT INTO%{config.TABLE_STOCK_MASTER}%'
        AND event_time >= now() - INTERVAL 2 HOUR
        AND exception != ''
        ORDER BY event_time DESC
        LIMIT 3
        """
        
        error_result = db.client.query(error_query)
        
        if error_result.result_rows:
            print("\nFound errors during master table updates:")
            for row in error_result.result_rows:
                print(f"  {row[0]}: {row[1][:100]}...")
        else:
            print("\nNo errors found during master table updates in the last 2 hours")
        
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(debug_master_table()) 