import asyncio
import pytz
from datetime import datetime, timedelta
import json
import aiohttp
from endpoints.db import ClickHouseDB
from endpoints import config, bars, trades, quotes
from endpoints.master_v2 import insert_latest_data

async def test_full_pipeline():
    """Test the entire data collection and storage pipeline for one ticker"""
    et_tz = pytz.timezone('US/Eastern')
    now = datetime.now(et_tz)
    
    # Use a recent time window (last 5 minutes)
    to_date = now.replace(second=0, microsecond=0)
    from_date = to_date - timedelta(minutes=5)
    
    ticker = "AAPL"
    
    print(f"\nTesting data pipeline for {ticker} from {from_date.strftime('%H:%M:%S')} to {to_date.strftime('%H:%M:%S')}")
    
    # Step 1: Fetch data
    print("\n=== Step 1: Fetching data ===")
    
    # Fetch bars
    print(f"Fetching bars for {ticker}...")
    bar_data = await bars.fetch_bars(ticker, from_date, to_date)
    print(f"Retrieved {len(bar_data)} bars")
    
    # Fetch trades
    print(f"Fetching trades for {ticker}...")
    trade_data = await trades.fetch_trades(ticker, from_date, to_date)
    print(f"Retrieved {len(trade_data)} trades")
    
    # Fetch quotes
    print(f"Fetching quotes for {ticker}...")
    quote_data = await quotes.fetch_quotes(ticker, from_date, to_date)
    print(f"Retrieved {len(quote_data)} quotes")
    
    if len(bar_data) == 0 and len(trade_data) == 0 and len(quote_data) == 0:
        print("\nNo data fetched from API. Cannot proceed with storage test.")
        return
    
    # Step 2: Store data
    print("\n=== Step 2: Testing storage ===")
    
    # Create dedicated connections for each operation
    bars_db = ClickHouseDB()
    trades_db = ClickHouseDB()
    quotes_db = ClickHouseDB()
    
    try:
        # Store bars
        if bar_data:
            print(f"Storing {len(bar_data)} bars...")
            try:
                await bars.store_bars(bars_db, bar_data)
                print("Bars stored successfully")
                # Verify with a direct query
                count_query = f"""
                SELECT COUNT(*) FROM {bars_db.database}.stock_bars 
                WHERE ticker = '{ticker}' 
                AND timestamp >= toDateTime64('{from_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
                AND timestamp <= toDateTime64('{to_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
                """
                result = bars_db.client.query(count_query)
                stored_count = result.result_rows[0][0]
                print(f"Database confirms {stored_count} bars stored")
            except Exception as e:
                print(f"Error storing bars: {str(e)}")
        
        # Store trades
        if trade_data:
            print(f"Storing {len(trade_data)} trades...")
            try:
                await trades.store_trades(trades_db, trade_data)
                print("Trades stored successfully")
                # Verify with a direct query
                count_query = f"""
                SELECT COUNT(*) FROM {trades_db.database}.stock_trades 
                WHERE ticker = '{ticker}' 
                AND sip_timestamp >= toDateTime64('{from_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
                AND sip_timestamp <= toDateTime64('{to_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
                """
                result = trades_db.client.query(count_query)
                stored_count = result.result_rows[0][0]
                print(f"Database confirms {stored_count} trades stored")
            except Exception as e:
                print(f"Error storing trades: {str(e)}")
        
        # Store quotes
        if quote_data:
            print(f"Storing {len(quote_data)} quotes...")
            try:
                await quotes.store_quotes(quotes_db, quote_data)
                print("Quotes stored successfully")
                # Verify with a direct query
                count_query = f"""
                SELECT COUNT(*) FROM {quotes_db.database}.stock_quotes 
                WHERE ticker = '{ticker}' 
                AND sip_timestamp >= toDateTime64('{from_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
                AND sip_timestamp <= toDateTime64('{to_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
                """
                result = quotes_db.client.query(count_query)
                stored_count = result.result_rows[0][0]
                print(f"Database confirms {stored_count} quotes stored")
            except Exception as e:
                print(f"Error storing quotes: {str(e)}")
    
    finally:
        bars_db.close()
        trades_db.close()
        quotes_db.close()
    
    # Step 3: Test master table update
    print("\n=== Step 3: Testing master table update ===")
    master_db = ClickHouseDB()
    
    try:
        # Count records in master table before update
        before_query = f"""
        SELECT COUNT(*) FROM {master_db.database}.stock_master 
        WHERE ticker = '{ticker}' 
        AND timestamp >= toDateTime64('{from_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
        AND timestamp <= toDateTime64('{to_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
        """
        before_result = master_db.client.query(before_query)
        before_count = before_result.result_rows[0][0]
        
        print(f"Master table has {before_count} records for this time range before update")
        
        # Update master table
        print("Updating master table...")
        try:
            await insert_latest_data(master_db, from_date, to_date)
            print("Master table updated successfully")
            
            # Count records after update
            after_query = f"""
            SELECT COUNT(*) FROM {master_db.database}.stock_master 
            WHERE ticker = '{ticker}' 
            AND timestamp >= toDateTime64('{from_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
            AND timestamp <= toDateTime64('{to_date.strftime('%Y-%m-%d %H:%M:%S')}', 9)
            """
            after_result = master_db.client.query(after_query)
            after_count = after_result.result_rows[0][0]
            
            print(f"Master table has {after_count} records for this time range after update")
            
            if after_count > before_count:
                print(f"✅ Success: {after_count - before_count} new records added to master table")
            elif after_count == before_count:
                print("⚠️ Warning: No new records added to master table")
            else:
                print(f"❌ Error: Master table has {before_count - after_count} fewer records after update")
        except Exception as e:
            print(f"Error updating master table: {str(e)}")
    finally:
        master_db.close()
    
    print("\n=== Test complete ===")

if __name__ == "__main__":
    asyncio.run(test_full_pipeline()) 