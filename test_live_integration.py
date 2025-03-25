from endpoints.db import ClickHouseDB
from endpoints.main import process_ticker
from endpoints import master_v2
from datetime import datetime, timedelta
import asyncio
import pytz
import argparse

async def simulate_live_cycle(ticker: str, force_init: bool = False):
    """
    Simulates a single cycle of the live data workflow.
    
    Args:
        ticker: Ticker symbol to process
        force_init: Whether to force initialization of master tables
    """
    db = ClickHouseDB()
    try:
        # Setup time range for "current minute"
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        minute_end = now.replace(second=0, microsecond=0)
        minute_start = minute_end - timedelta(minutes=1)
        
        print(f"\n=== Simulating live data cycle for {ticker} ===")
        print(f"Time range: {minute_start.strftime('%Y-%m-%d %H:%M:%S')} - {minute_end.strftime('%Y-%m-%d %H:%M:%S')} ET")
        
        # 1. Check if master tables exist, initialize if needed
        if force_init:
            print("\n--- Initializing master tables (forced) ---")
            await master_v2.init_master_v2(db)
        else:
            master_exists = db.table_exists("stock_master")
            normalized_exists = db.table_exists("stock_normalized")
            
            if not master_exists or not normalized_exists:
                print("\n--- Initializing master tables (tables don't exist) ---")
                await master_v2.init_master_v2(db)
        
        # 2. Process ticker data (fetch and store)
        print(f"\n--- Processing ticker data for {ticker} ---")
        await process_ticker(db, ticker, minute_start, minute_end, store_latest_only=True)
        
        # 3. Update master tables
        print("\n--- Updating master tables ---")
        await master_v2.insert_latest_data(db, minute_start, minute_end)
        
        # 4. Verify tables were updated
        print("\n--- Verifying data in tables ---")
        
        # Check master table
        master_query = f"""
        SELECT count() FROM {db.database}.stock_master 
        WHERE ticker = '{ticker}' AND toDate(timestamp) = '{minute_start.strftime('%Y-%m-%d')}'
        """
        master_result = db.client.query(master_query)
        master_count = master_result.result_rows[0][0]
        
        # Check normalized table
        norm_query = f"""
        SELECT count() FROM {db.database}.stock_normalized 
        WHERE ticker = '{ticker}' AND toDate(timestamp) = '{minute_start.strftime('%Y-%m-%d')}'
        """
        norm_result = db.client.query(norm_query)
        norm_count = norm_result.result_rows[0][0]
        
        print(f"Master table rows for today: {master_count}")
        print(f"Normalized table rows for today: {norm_count}")
        
        print("\n=== Live data cycle simulation completed! ===")
        if master_count == 0 and norm_count == 0:
            print("Warning: No data was found for today in the master tables.")
            print("This might be normal if no data was fetched from the source.")
        
    except Exception as e:
        print(f"Error in simulation: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the live data integration")
    parser.add_argument("--ticker", type=str, default="AAPL", help="Ticker to process")
    parser.add_argument("--force-init", action="store_true", help="Force initialization of master tables")
    args = parser.parse_args()
    
    asyncio.run(simulate_live_cycle(args.ticker, args.force_init)) 