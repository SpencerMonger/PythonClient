import asyncio
from datetime import datetime, timedelta
import pytz
from endpoints.db import ClickHouseDB
from endpoints import config, bars, trades, quotes
from endpoints.master_v2 import insert_latest_data

async def fix_test_pipeline():
    """Test the entire data pipeline with fixes for timezone issues"""
    et_tz = pytz.timezone('US/Eastern')
    now = datetime.now(et_tz)
    
    # Use a recent time window (last 5 minutes)
    to_date = now.replace(second=0, microsecond=0)
    from_date = to_date - timedelta(minutes=5)
    
    ticker = "AAPL"
    
    print(f"\nTesting fixed data pipeline for {ticker} from {from_date.strftime('%H:%M:%S')} to {to_date.strftime('%H:%M:%S')}")
    
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
    
    # Step 2: Store data with modified ticker names for testing
    print("\n=== Step 2: Testing storage with modified tickers ===")
    
    # Modify tickers to make them unique for this test
    test_suffix = now.strftime('%H%M')
    for i in range(len(bar_data)):
        bar_data[i]['ticker'] = f"{bar_data[i]['ticker']}_TEST_{test_suffix}"
    
    for i in range(len(trade_data)):
        trade_data[i]['ticker'] = f"{trade_data[i]['ticker']}_TEST_{test_suffix}"
    
    for i in range(len(quote_data)):
        quote_data[i]['ticker'] = f"{quote_data[i]['ticker']}_TEST_{test_suffix}"
    
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
                
                # Verify with a direct query - use UTC conversion
                utc_from = from_date.astimezone(pytz.UTC)
                utc_to = to_date.astimezone(pytz.UTC)
                from_str = utc_from.strftime('%Y-%m-%d %H:%M:%S')
                to_str = utc_to.strftime('%Y-%m-%d %H:%M:%S')
                
                count_query = f"""
                SELECT COUNT(*) FROM {bars_db.database}.stock_bars 
                WHERE ticker LIKE '%_TEST_{test_suffix}' 
                AND timestamp >= toDateTime('{from_str}')
                AND timestamp <= toDateTime('{to_str}')
                """
                result = bars_db.client.query(count_query)
                stored_count = result.result_rows[0][0]
                print(f"Database confirms {stored_count} bars stored")
                
                if stored_count > 0:
                    # Get some examples of what was stored
                    examples_query = f"""
                    SELECT ticker, timestamp FROM {bars_db.database}.stock_bars 
                    WHERE ticker LIKE '%_TEST_{test_suffix}' 
                    LIMIT 3
                    """
                    examples_result = bars_db.client.query(examples_query)
                    print("Example stored records:")
                    for row in examples_result.result_rows:
                        print(f"  {row[0]}: {row[1]}")
            except Exception as e:
                print(f"Error storing bars: {str(e)}")
        
        # Store trades
        if trade_data:
            print(f"Storing {len(trade_data)} trades...")
            try:
                await trades.store_trades(trades_db, trade_data)
                print("Trades stored successfully")
                
                # Verify with a direct query - use UTC conversion
                utc_from = from_date.astimezone(pytz.UTC)
                utc_to = to_date.astimezone(pytz.UTC)
                from_str = utc_from.strftime('%Y-%m-%d %H:%M:%S')
                to_str = utc_to.strftime('%Y-%m-%d %H:%M:%S')
                
                count_query = f"""
                SELECT COUNT(*) FROM {trades_db.database}.stock_trades 
                WHERE ticker LIKE '%_TEST_{test_suffix}' 
                AND sip_timestamp >= toDateTime('{from_str}')
                AND sip_timestamp <= toDateTime('{to_str}')
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
                
                # Verify with a direct query - use UTC conversion
                utc_from = from_date.astimezone(pytz.UTC)
                utc_to = to_date.astimezone(pytz.UTC)
                from_str = utc_from.strftime('%Y-%m-%d %H:%M:%S')
                to_str = utc_to.strftime('%Y-%m-%d %H:%M:%S')
                
                count_query = f"""
                SELECT COUNT(*) FROM {quotes_db.database}.stock_quotes 
                WHERE ticker LIKE '%_TEST_{test_suffix}' 
                AND sip_timestamp >= toDateTime('{from_str}')
                AND sip_timestamp <= toDateTime('{to_str}')
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
        utc_from = from_date.astimezone(pytz.UTC)
        utc_to = to_date.astimezone(pytz.UTC)
        from_str = utc_from.strftime('%Y-%m-%d %H:%M:%S')
        to_str = utc_to.strftime('%Y-%m-%d %H:%M:%S')
        
        before_query = f"""
        SELECT COUNT(*) FROM {master_db.database}.stock_master 
        WHERE ticker LIKE '%_TEST_{test_suffix}' 
        AND timestamp >= toDateTime('{from_str}')
        AND timestamp <= toDateTime('{to_str}')
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
            WHERE ticker LIKE '%_TEST_{test_suffix}' 
            AND timestamp >= toDateTime('{from_str}')
            AND timestamp <= toDateTime('{to_str}')
            """
            after_result = master_db.client.query(after_query)
            after_count = after_result.result_rows[0][0]
            
            print(f"Master table has {after_count} records for this time range after update")
            
            if after_count > before_count:
                print(f"✅ Success: {after_count - before_count} new records added to master table")
            elif after_count == before_count:
                print("⚠️ Warning: No new records added to master table")
                
                # Check if any records exist for these tickers
                any_query = f"""
                SELECT COUNT(*) FROM {master_db.database}.stock_master 
                WHERE ticker LIKE '%_TEST_{test_suffix}'
                """
                any_result = master_db.client.query(any_query)
                any_count = any_result.result_rows[0][0]
                
                if any_count > 0:
                    print(f"Found {any_count} records with these test tickers but outside the time range")
                    
                    # Get sample records
                    sample_query = f"""
                    SELECT ticker, timestamp FROM {master_db.database}.stock_master 
                    WHERE ticker LIKE '%_TEST_{test_suffix}'
                    LIMIT 3
                    """
                    sample_result = master_db.client.query(sample_query)
                    print("Sample records:")
                    for row in sample_result.result_rows:
                        print(f"  {row[0]}: {row[1]}")
            else:
                print(f"❌ Error: Master table has {before_count - after_count} fewer records after update")
        except Exception as e:
            print(f"Error updating master table: {str(e)}")
    finally:
        master_db.close()
    
    print("\n=== Test complete ===")

if __name__ == "__main__":
    from datetime import timedelta
    asyncio.run(fix_test_pipeline()) 