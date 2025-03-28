import asyncio
from datetime import datetime
import pytz
from endpoints.db import ClickHouseDB

async def check_database():
    """Check if there is any data in the database for today with correct timezone handling"""
    db = ClickHouseDB()
    
    try:
        # Get today's date in ET timezone
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        utc_now = now.astimezone(pytz.UTC)
        today_str = now.strftime('%Y-%m-%d')
        
        # For queries, we'll use both formats
        utc_today_str = utc_now.strftime('%Y-%m-%d')
        
        # Tables to check
        tables = ["stock_bars", "stock_trades", "stock_quotes", "stock_master", "stock_normalized"]
        
        print(f"\nChecking database for data on {today_str} (Local) / {utc_today_str} (UTC)...")
        
        for table in tables:
            try:
                # Check if table exists
                if not db.table_exists(table):
                    print(f"Table {table} does not exist")
                    continue
                
                # Count rows for today using both ET and UTC date strings
                time_field = "timestamp"
                if table in ["stock_trades", "stock_quotes"]:
                    time_field = "sip_timestamp"
                
                # Query 1: Using local date string
                query1 = f"SELECT COUNT(*) FROM {db.database}.{table} WHERE toDate({time_field}) = '{today_str}'"
                result1 = db.client.query(query1)
                count1 = result1.result_rows[0][0] if result1.result_rows else 0
                
                # Query 2: Using UTC date string
                query2 = f"SELECT COUNT(*) FROM {db.database}.{table} WHERE toDate({time_field}) = '{utc_today_str}'"
                result2 = db.client.query(query2)
                count2 = result2.result_rows[0][0] if result2.result_rows else 0
                
                print(f"{table}:")
                print(f"  Using local date ({today_str}): {count1} rows")
                print(f"  Using UTC date ({utc_today_str}): {count2} rows")
                
                # Query 3: Using the past hour time range with explicit UTC conversion
                past_hour_utc = utc_now - timedelta(hours=1)
                past_hour_str = past_hour_utc.strftime('%Y-%m-%d %H:%M:%S')
                now_str = utc_now.strftime('%Y-%m-%d %H:%M:%S')
                
                query3 = f"""
                SELECT COUNT(*) FROM {db.database}.{table} 
                WHERE {time_field} >= toDateTime('{past_hour_str}')
                AND {time_field} <= toDateTime('{now_str}')
                """
                result3 = db.client.query(query3)
                count3 = result3.result_rows[0][0] if result3.result_rows else 0
                
                print(f"  Past hour range (UTC {past_hour_str} to {now_str}): {count3} rows")
                
                # If we found data in the past hour, show the latest timestamps
                if count3 > 0:
                    latest_query = f"""
                    SELECT toDateTime({time_field}) as latest, ticker 
                    FROM {db.database}.{table} 
                    WHERE {time_field} >= toDateTime('{past_hour_str}')
                    ORDER BY {time_field} DESC
                    LIMIT 3
                    """
                    
                    latest_result = db.client.query(latest_query)
                    print(f"  Latest entries (UTC):")
                    for row in latest_result.result_rows:
                        print(f"    - {row[1]}: {row[0]}")
            
            except Exception as e:
                print(f"Error checking table {table}: {str(e)}")
        
        # Check when the last data was inserted across all time
        print("\nChecking for most recent data (all time):")
        for table in tables:
            try:
                if not db.table_exists(table):
                    continue
                
                time_field = "timestamp"
                if table in ["stock_trades", "stock_quotes"]:
                    time_field = "sip_timestamp"
                
                latest_query = f"""
                SELECT max(toDateTime({time_field})) as latest_time, toDate(max({time_field})) as latest_date
                FROM {db.database}.{table}
                """
                
                latest_result = db.client.query(latest_query)
                if latest_result.result_rows and latest_result.result_rows[0][0]:
                    latest_time_utc = latest_result.result_rows[0][0]
                    print(f"{table}: Latest record from {latest_result.result_rows[0][1]} at {latest_time_utc} UTC")
                    
                    # Calculate time difference from now
                    if isinstance(latest_time_utc, datetime):
                        time_diff = (utc_now - latest_time_utc).total_seconds() / 60.0
                        print(f"  Time difference: {time_diff:.1f} minutes ago")
                else:
                    print(f"{table}: No data found")
                    
            except Exception as e:
                print(f"Error checking most recent data for {table}: {str(e)}")
    
    finally:
        db.close()

if __name__ == "__main__":
    from datetime import timedelta
    asyncio.run(check_database()) 