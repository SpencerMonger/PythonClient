import asyncio
from datetime import datetime
import pytz
from endpoints.db import ClickHouseDB
from endpoints import config

async def test_insert():
    """Test direct insertion into ClickHouse with different settings"""
    db = ClickHouseDB()
    
    try:
        # Create a simple test record
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        
        table_name = "stock_bars"
        
        # Verify the table exists
        if not db.table_exists(table_name):
            print(f"Table {table_name} does not exist")
            return
        
        # Create test data
        test_data = [{
            "ticker": "TEST",
            "timestamp": now,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000,
            "vwap": 100.2,
            "transactions": 50
        }]
        
        print(f"\n=== Testing insert with original settings ===")
        try:
            # Current settings in the code
            settings = {
                'async_insert': 1,
                'wait_for_async_insert': 0,
                'optimize_on_insert': 0
            }
            
            # Direct call to insert method
            db.client.insert(
                f"{db.database}.{table_name}", 
                [list(test_data[0].values())], 
                column_names=list(test_data[0].keys()), 
                settings=settings
            )
            print("Insert with original settings succeeded")
            
            # Check if the data was inserted
            query = f"""
            SELECT COUNT(*) FROM {db.database}.{table_name} 
            WHERE ticker = 'TEST' AND toDate(timestamp) = '{now.strftime('%Y-%m-%d')}'
            """
            result = db.client.query(query)
            count = result.result_rows[0][0]
            print(f"Found {count} test records in the database")
            
        except Exception as e:
            print(f"Error with original settings: {str(e)}")
        
        # Test with synchronized insert
        print(f"\n=== Testing insert with synchronized settings ===")
        try:
            # Wait for async insert to complete
            settings = {
                'async_insert': 1,
                'wait_for_async_insert': 1,  # Wait for completion
                'optimize_on_insert': 0
            }
            
            # Create a different test record
            test_data[0]["ticker"] = "TEST2"
            
            # Direct call to insert method
            db.client.insert(
                f"{db.database}.{table_name}", 
                [list(test_data[0].values())], 
                column_names=list(test_data[0].keys()), 
                settings=settings
            )
            print("Insert with synchronized settings succeeded")
            
            # Check if the data was inserted
            query = f"""
            SELECT COUNT(*) FROM {db.database}.{table_name} 
            WHERE ticker = 'TEST2' AND toDate(timestamp) = '{now.strftime('%Y-%m-%d')}'
            """
            result = db.client.query(query)
            count = result.result_rows[0][0]
            print(f"Found {count} TEST2 records in the database")
            
        except Exception as e:
            print(f"Error with synchronized settings: {str(e)}")
        
        # Test with fully synchronous insert
        print(f"\n=== Testing insert with fully synchronous settings ===")
        try:
            # Disable async insert completely
            settings = {
                'async_insert': 0,
                'wait_for_async_insert': 0,
                'optimize_on_insert': 0
            }
            
            # Create a third test record
            test_data[0]["ticker"] = "TEST3"
            
            # Direct call to insert method
            db.client.insert(
                f"{db.database}.{table_name}", 
                [list(test_data[0].values())], 
                column_names=list(test_data[0].keys()), 
                settings=settings
            )
            print("Insert with fully synchronous settings succeeded")
            
            # Check if the data was inserted
            query = f"""
            SELECT COUNT(*) FROM {db.database}.{table_name} 
            WHERE ticker = 'TEST3' AND toDate(timestamp) = '{now.strftime('%Y-%m-%d')}'
            """
            result = db.client.query(query)
            count = result.result_rows[0][0]
            print(f"Found {count} TEST3 records in the database")
            
        except Exception as e:
            print(f"Error with fully synchronous settings: {str(e)}")
        
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(test_insert())