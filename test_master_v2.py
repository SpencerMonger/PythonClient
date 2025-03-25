from endpoints.db import ClickHouseDB
from endpoints.master_v2 import reinit_current_day, insert_latest_data, init_master_v2
import asyncio
import argparse
from datetime import datetime, timedelta
import pytz

async def test_reinit_current_day(db: ClickHouseDB):
    """Test reinitializing the current day's data"""
    print("\n=== Testing reinit_current_day function ===")
    try:
        await reinit_current_day(db)
        print("Test completed successfully!")
    except Exception as e:
        print(f"Error in reinit_current_day: {e}")

async def test_insert_latest_data(db: ClickHouseDB):
    """Test inserting latest data"""
    print("\n=== Testing insert_latest_data function ===")
    try:
        # Get current datetime in Eastern Time
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        # Set from_date to one minute before now
        from_date = now - timedelta(minutes=1)
        to_date = now
        
        await insert_latest_data(db, from_date, to_date)
        print("Test completed successfully!")
    except Exception as e:
        print(f"Error in insert_latest_data: {e}")

async def test_init_master_v2(db: ClickHouseDB, force: bool = False):
    """Test initializing master_v2 tables"""
    print("\n=== Testing init_master_v2 function ===")
    try:
        if force:
            # Manually drop tables if force is True
            print("Force option enabled, dropping existing tables...")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master_mv")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized_mv")
        
        await init_master_v2(db)
        print("Test completed successfully!")
    except Exception as e:
        print(f"Error in init_master_v2: {e}")

async def main(test_type: str, force: bool = False):
    """
    Test selected functions from master_v2.
    
    Args:
        test_type: Type of test to run ('reinit', 'insert', 'init', or 'all')
        force: Whether to force recreation of tables for init test
    """
    db = ClickHouseDB()
    try:
        if test_type == 'reinit' or test_type == 'all':
            await test_reinit_current_day(db)
        
        if test_type == 'insert' or test_type == 'all':
            await test_insert_latest_data(db)
        
        if test_type == 'init' or test_type == 'all':
            await test_init_master_v2(db, force)
        
        print("\nAll selected tests completed!")
    finally:
        db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test master_v2 functionality")
    parser.add_argument("--test", type=str, choices=['reinit', 'insert', 'init', 'all'], 
                      default='insert', help="Test to run")
    parser.add_argument("--force", action="store_true", 
                      help="Force recreation of tables for init test")
    args = parser.parse_args()
    
    asyncio.run(main(args.test, args.force)) 