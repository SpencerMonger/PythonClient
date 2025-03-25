from endpoints.db import ClickHouseDB
from endpoints.master_v2 import init_master_v2
import asyncio
import argparse

async def main(force: bool = False):
    """
    Initialize the master_v2 table structures only (creates empty tables).
    This does NOT populate the tables with real stock data.
    
    For populating tables with actual data, use populate_master_v2.py instead.
    
    Args:
        force: If True, will drop and recreate tables even if they exist
    """
    db = ClickHouseDB()
    try:
        print(f"Initializing master_v2 table structures (force={force})...")
        print("Note: This will only create empty tables. To populate with actual data, use populate_master_v2.py")
        
        if force:
            # Manually drop tables if force is True
            print("Force option enabled, dropping existing tables...")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master_mv")
            db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized_mv")
            print("Existing tables dropped successfully")
            
        await init_master_v2(db)
        
        # Verify tables exist but are empty (besides system markers)
        master_count = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_master").result_rows[0][0]
        norm_count = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_normalized").result_rows[0][0]
        
        print(f"\nMaster table structure created with {master_count} system marker record(s)")
        print(f"Normalized table structure created with {norm_count} system marker record(s)")
        print("\nTo populate these tables with actual stock data, run:")
        print("  python populate_master_v2.py")
        print("  python populate_master_v2.py --date YYYY-MM-DD  # For a specific date")
        
        print("\nMaster_v2 table structures initialized successfully!")
    except Exception as e:
        print(f"Error initializing master_v2 table structures: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize master_v2 table structures (creates empty tables)")
    parser.add_argument("--force", action="store_true", help="Force recreation of tables even if they exist")
    args = parser.parse_args()
    
    asyncio.run(main(force=args.force)) 