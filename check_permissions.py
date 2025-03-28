import asyncio
from datetime import datetime
import pytz
from endpoints.db import ClickHouseDB
from endpoints import config

async def check_permissions():
    """Check database permissions and test basic operations"""
    db = ClickHouseDB()
    
    try:
        print("\n=== Database Connection Info ===")
        print(f"Host: {config.CLICKHOUSE_HOST}")
        print(f"Port: {config.CLICKHOUSE_HTTP_PORT}")
        print(f"Database: {config.CLICKHOUSE_DATABASE}")
        print(f"User: {config.CLICKHOUSE_USER}")
        print(f"Secure: {config.CLICKHOUSE_SECURE}")
        
        # Test basic connection
        print("\n=== Testing Basic Connection ===")
        try:
            result = db.client.command("SELECT 1")
            print("Connection test: SUCCESS")
        except Exception as e:
            print(f"Connection test: FAILED - {str(e)}")
        
        # Check database exists
        print("\n=== Checking Database Exists ===")
        try:
            result = db.client.command(f"SELECT 1 FROM system.databases WHERE name = '{config.CLICKHOUSE_DATABASE}'")
            if result:
                print(f"Database '{config.CLICKHOUSE_DATABASE}' exists: YES")
            else:
                print(f"Database '{config.CLICKHOUSE_DATABASE}' exists: NO")
        except Exception as e:
            print(f"Database check failed: {str(e)}")
        
        # Check tables exist
        print("\n=== Checking Tables Exist ===")
        tables = ["stock_bars", "stock_trades", "stock_quotes", "stock_master", "stock_normalized"]
        
        for table in tables:
            try:
                exists = db.table_exists(table)
                print(f"Table '{table}' exists: {'YES' if exists else 'NO'}")
                
                if exists:
                    # Check table structure
                    try:
                        result = db.client.command(f"DESCRIBE TABLE {db.database}.{table}")
                        print(f"  Columns: {len(result.splitlines()) - 1}")
                    except Exception as e:
                        print(f"  Could not describe table: {str(e)}")
                    
                    # Check permission to insert
                    try:
                        result = db.client.command(f"SHOW GRANTS")
                        if "INSERT" in result and table in result:
                            print(f"  Insert permission: LIKELY YES")
                        else:
                            print(f"  Insert permission: UNCERTAIN - check output below")
                            print(f"  Grants: {result}")
                    except Exception as e:
                        print(f"  Could not check grants: {str(e)}")
                    
                    # Check row count
                    try:
                        result = db.client.query(f"SELECT COUNT(*) FROM {db.database}.{table}")
                        count = result.result_rows[0][0]
                        print(f"  Row count: {count}")
                    except Exception as e:
                        print(f"  Could not get row count: {str(e)}")
            except Exception as e:
                print(f"Check for table '{table}' failed: {str(e)}")
        
        # Try an explicit insert with transaction
        print("\n=== Testing Explicit Insert ===")
        try:
            et_tz = pytz.timezone('US/Eastern')
            now = datetime.now(et_tz)
            
            # Try to insert a test record with transaction
            db.client.command("BEGIN TRANSACTION")
            
            insert_query = f"""
            INSERT INTO {db.database}.stock_bars (
                uni_id, ticker, timestamp, open, high, low, close, volume, vwap, transactions
            ) VALUES (
                {int(now.timestamp() * 1000)}, 'TEST_TX', toDateTime('{now.strftime('%Y-%m-%d %H:%M:%S')}'), 
                100.0, 101.0, 99.0, 100.5, 1000, 100.2, 50
            )
            """
            
            db.client.command(insert_query)
            db.client.command("COMMIT")
            
            # Check if the data was inserted
            result = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_bars WHERE ticker = 'TEST_TX'")
            count = result.result_rows[0][0]
            print(f"Explicit transaction insert result: {'SUCCESS' if count > 0 else 'FAILED'}")
            print(f"Found {count} test records")
            
        except Exception as e:
            print(f"Explicit insert test failed: {str(e)}")
            try:
                db.client.command("ROLLBACK")
            except:
                pass
        
        # Try a simple direct insert
        print("\n=== Testing Simple Direct Insert ===")
        try:
            et_tz = pytz.timezone('US/Eastern')
            now = datetime.now(et_tz)
            
            # Try to insert a test record directly
            insert_query = f"""
            INSERT INTO {db.database}.stock_bars (
                uni_id, ticker, timestamp, open, high, low, close, volume, vwap, transactions
            ) VALUES (
                {int(now.timestamp() * 1000) + 1}, 'TEST_DIRECT', toDateTime('{now.strftime('%Y-%m-%d %H:%M:%S')}'), 
                100.0, 101.0, 99.0, 100.5, 1000, 100.2, 50
            )
            """
            
            db.client.command(insert_query)
            
            # Check if the data was inserted
            result = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_bars WHERE ticker = 'TEST_DIRECT'")
            count = result.result_rows[0][0]
            print(f"Simple direct insert result: {'SUCCESS' if count > 0 else 'FAILED'}")
            print(f"Found {count} test records")
            
        except Exception as e:
            print(f"Direct insert test failed: {str(e)}")
        
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(check_permissions()) 