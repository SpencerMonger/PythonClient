import asyncio
from datetime import datetime
import pytz
from endpoints.db import ClickHouseDB

async def check_database():
    """Check if there is any data in the database for today"""
    db = ClickHouseDB()
    
    try:
        # Get today's date in ET timezone
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        today_str = now.strftime('%Y-%m-%d')
        
        # Tables to check
        tables = ["stock_bars", "stock_trades", "stock_quotes", "stock_master", "stock_normalized"]
        
        print(f"\nChecking database for data on {today_str}...")
        
        for table in tables:
            try:
                # Check if table exists
                if not db.table_exists(table):
                    print(f"Table {table} does not exist")
                    continue
                
                # Count rows for today
                query = f"SELECT COUNT(*) FROM {db.database}.{table} WHERE toDate(timestamp) = '{today_str}'"
                
                # Adjust for tables using sip_timestamp
                if table in ["stock_trades", "stock_quotes"]:
                    query = f"SELECT COUNT(*) FROM {db.database}.{table} WHERE toDate(sip_timestamp) = '{today_str}'"
                
                result = db.client.query(query)
                count = result.result_rows[0][0] if result.result_rows else 0
                
                print(f"{table}: {count} rows for {today_str}")
                
                if count > 0:
                    # Get the latest timestamp
                    time_field = "timestamp"
                    if table in ["stock_trades", "stock_quotes"]:
                        time_field = "sip_timestamp"
                    
                    latest_query = f"""
                    SELECT toDateTime({time_field}) as latest, ticker 
                    FROM {db.database}.{table} 
                    WHERE toDate({time_field}) = '{today_str}'
                    ORDER BY {time_field} DESC
                    LIMIT 3
                    """
                    
                    latest_result = db.client.query(latest_query)
                    print(f"  Latest {table} entries:")
                    for row in latest_result.result_rows:
                        print(f"  - {row[1]}: {row[0]}")
            
            except Exception as e:
                print(f"Error checking table {table}: {str(e)}")
        
        # Check when the last data was inserted
        print("\nChecking for most recent data across all dates:")
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
                    print(f"{table}: Latest record from {latest_result.result_rows[0][1]} at {latest_result.result_rows[0][0]}")
                else:
                    print(f"{table}: No data found")
                    
            except Exception as e:
                print(f"Error checking most recent data for {table}: {str(e)}")
    
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(check_database()) 