from endpoints.db import ClickHouseDB
from endpoints.master_v2 import init_master_v2, reinit_current_day, insert_latest_data
import asyncio
from datetime import datetime, timedelta
import pytz
import argparse

async def main(force: bool = False, skip_init: bool = False):
    """
    Initialize and populate the master_v2 tables with real data from today.
    
    Args:
        force: If True, will drop and recreate tables even if they exist
        skip_init: If True, will skip initialization and only populate with data
    """
    db = ClickHouseDB()
    try:
        if not skip_init:
            print(f"Initializing master_v2 tables (force={force})...")
            
            if force:
                # Manually drop tables if force is True
                print("Force option enabled, dropping existing tables...")
                db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master")
                db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized")
                db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master_mv")
                db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized_mv")
                print("Existing tables dropped successfully")
                
            await init_master_v2(db)
            print("Master_v2 tables initialized successfully!")
        
        # Now populate the tables with real data using reinit_current_day
        print("\nPopulating master_v2 tables with today's data...")
        await reinit_current_day(db)
        
        # Verify tables have data from today
        et_tz = pytz.timezone('US/Eastern')
        today = datetime.now(et_tz).date()
        today_str = today.strftime('%Y-%m-%d')
        
        master_count = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_master WHERE toDate(timestamp) = '{today_str}'").result_rows[0][0]
        norm_count = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_normalized WHERE toDate(timestamp) = '{today_str}'").result_rows[0][0]
        
        print(f"Master table has {master_count} records for today ({today_str})")
        print(f"Normalized table has {norm_count} records for today ({today_str})")
        
        if master_count == 0:
            print("\nNo data found for today. Checking if there's any data available in source tables...")
            
            bars_count = db.client.query(f"SELECT COUNT(*) FROM stock_bars WHERE toDate(timestamp) = '{today_str}'").result_rows[0][0]
            quotes_count = db.client.query(f"SELECT COUNT(*) FROM stock_quotes WHERE toDate(sip_timestamp) = '{today_str}'").result_rows[0][0]
            trades_count = db.client.query(f"SELECT COUNT(*) FROM stock_trades WHERE toDate(sip_timestamp) = '{today_str}'").result_rows[0][0]
            
            print(f"Source tables data counts for today ({today_str}):")
            print(f"  - stock_bars: {bars_count} records")
            print(f"  - stock_quotes: {quotes_count} records")
            print(f"  - stock_trades: {trades_count} records")
            
            if bars_count == 0 and quotes_count == 0 and trades_count == 0:
                print("\nNo data available for today. Looking for the most recent data date...")
                
                latest_date_query = """
                SELECT max(toDate(timestamp)) as max_date 
                FROM stock_bars
                """
                latest_date = db.client.query(latest_date_query).result_rows[0][0]
                
                if latest_date:
                    print(f"\nMost recent data found for: {latest_date}")
                    print(f"Would you like to populate tables with data from {latest_date} instead?")
                    print("To do so, run this script with: --date YYYY-MM-DD")
                else:
                    print("\nNo data found in source tables at all. Please fetch data first.")
            else:
                print("\nData exists in source tables but couldn't be processed. Check for errors above.")
        
    except Exception as e:
        print(f"Error in populate_master_v2: {e}")
    finally:
        db.close()

async def populate_specific_date(date_str: str):
    """Populate master_v2 tables with data from a specific date"""
    db = ClickHouseDB()
    try:
        print(f"Populating master_v2 tables with data from {date_str}...")
        
        # Check if tables exist
        master_exists = db.table_exists("stock_master")
        normalized_exists = db.table_exists("stock_normalized")
        
        if not (master_exists and normalized_exists):
            print("Tables don't exist. Initializing them first...")
            await init_master_v2(db)
        
        # Format date and create from_date and to_date for insert_latest_data
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        et_tz = pytz.timezone('US/Eastern')
        from_date = et_tz.localize(datetime.combine(date_obj, datetime.min.time()))
        to_date = et_tz.localize(datetime.combine(date_obj, datetime.max.time()))
        
        # Delete existing data for the date
        print(f"Cleaning up existing data for {date_str}...")
        db.client.command(f"ALTER TABLE {db.database}.stock_master DELETE WHERE toDate(timestamp) = '{date_str}'")
        db.client.command(f"ALTER TABLE {db.database}.stock_normalized DELETE WHERE toDate(timestamp) = '{date_str}'")
        
        # Insert data for the specific date
        await insert_latest_data(db, from_date, to_date)
        
        # Check if data was inserted
        master_count = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_master WHERE toDate(timestamp) = '{date_str}'").result_rows[0][0]
        norm_count = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_normalized WHERE toDate(timestamp) = '{date_str}'").result_rows[0][0]
        
        print(f"Master table now has {master_count} records for {date_str}")
        print(f"Normalized table now has {norm_count} records for {date_str}")
        
    except Exception as e:
        print(f"Error populating for specific date: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize and populate master_v2 tables with real data")
    parser.add_argument("--force", action="store_true", help="Force recreation of tables even if they exist")
    parser.add_argument("--skip-init", action="store_true", help="Skip initialization and only populate with data")
    parser.add_argument("--date", type=str, help="Populate with data from a specific date (format: YYYY-MM-DD)")
    args = parser.parse_args()
    
    if args.date:
        asyncio.run(populate_specific_date(args.date))
    else:
        asyncio.run(main(force=args.force, skip_init=args.skip_init)) 