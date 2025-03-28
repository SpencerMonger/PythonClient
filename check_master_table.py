import asyncio
from datetime import datetime, timedelta
import pytz
from endpoints.db import ClickHouseDB
from endpoints import config

async def check_master_table():
    """Check the latest data in the master table"""
    db = ClickHouseDB()
    
    try:
        # Get current time in ET timezone
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        utc_now = now.astimezone(pytz.UTC)
        
        print(f"Current time (ET): {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"Current time (UTC): {utc_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        # Get the latest records from the master table
        query = f"""
        SELECT 
            formatDateTime(timestamp, '%Y-%m-%d %H:%M:%S') as ts,
            ticker,
            open,
            high,
            low,
            close,
            volume
        FROM {db.database}.stock_master
        WHERE timestamp >= now() - INTERVAL 2 HOUR
        ORDER BY timestamp DESC, ticker
        LIMIT 20
        """
        
        result = db.client.query(query)
        
        if result.result_rows:
            print("\nLatest records in master table (last 2 hours):")
            for row in result.result_rows:
                print(f"{row[0]} | {row[1]} | O:{row[2]} H:{row[3]} L:{row[4]} C:{row[5]} V:{row[6]}")
        else:
            print("\nNo records found in master table for the last 2 hours")
            
            # Check for the most recent data
            latest_query = f"""
            SELECT 
                max(timestamp) as latest_ts,
                formatDateTime(max(timestamp), '%Y-%m-%d %H:%M:%S') as latest_ts_str
            FROM {db.database}.stock_master
            """
            
            latest_result = db.client.query(latest_query)
            if latest_result.result_rows and latest_result.result_rows[0][0]:
                print(f"Most recent data in master table: {latest_result.result_rows[0][1]}")
                
                # Get some sample recent records
                sample_query = f"""
                SELECT 
                    formatDateTime(timestamp, '%Y-%m-%d %H:%M:%S') as ts,
                    ticker,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM {db.database}.stock_master
                WHERE timestamp = (
                    SELECT max(timestamp) FROM {db.database}.stock_master
                )
                LIMIT 5
                """
                
                sample_result = db.client.query(sample_query)
                if sample_result.result_rows:
                    print("\nMost recent records in master table:")
                    for row in sample_result.result_rows:
                        print(f"{row[0]} | {row[1]} | O:{row[2]} H:{row[3]} L:{row[4]} C:{row[5]} V:{row[6]}")
            else:
                print("No data found in master table")
        
        # Now check what's in the stock_bars table
        bars_query = f"""
        SELECT 
            formatDateTime(timestamp, '%Y-%m-%d %H:%M:%S') as ts,
            ticker,
            open,
            high,
            low,
            close,
            volume
        FROM {db.database}.stock_bars
        WHERE timestamp >= now() - INTERVAL 2 HOUR
        ORDER BY timestamp DESC, ticker
        LIMIT 20
        """
        
        bars_result = db.client.query(bars_query)
        
        if bars_result.result_rows:
            print("\nLatest records in stock_bars table (last 2 hours):")
            for row in bars_result.result_rows:
                print(f"{row[0]} | {row[1]} | O:{row[2]} H:{row[3]} L:{row[4]} C:{row[5]} V:{row[6]}")
        else:
            print("\nNo records found in stock_bars table for the last 2 hours")
            
            # Check for the most recent data
            latest_bars_query = f"""
            SELECT 
                max(timestamp) as latest_ts,
                formatDateTime(max(timestamp), '%Y-%m-%d %H:%M:%S') as latest_ts_str
            FROM {db.database}.stock_bars
            """
            
            latest_bars_result = db.client.query(latest_bars_query)
            if latest_bars_result.result_rows and latest_bars_result.result_rows[0][0]:
                print(f"Most recent data in stock_bars table: {latest_bars_result.result_rows[0][1]}")
                
                # Get some sample recent records
                sample_bars_query = f"""
                SELECT 
                    formatDateTime(timestamp, '%Y-%m-%d %H:%M:%S') as ts,
                    ticker,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM {db.database}.stock_bars
                WHERE timestamp = (
                    SELECT max(timestamp) FROM {db.database}.stock_bars
                )
                LIMIT 5
                """
                
                sample_bars_result = db.client.query(sample_bars_query)
                if sample_bars_result.result_rows:
                    print("\nMost recent records in stock_bars table:")
                    for row in sample_bars_result.result_rows:
                        print(f"{row[0]} | {row[1]} | O:{row[2]} H:{row[3]} L:{row[4]} C:{row[5]} V:{row[6]}")
            else:
                print("No data found in stock_bars table")
        
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(check_master_table()) 