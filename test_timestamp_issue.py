import asyncio
from datetime import datetime, timedelta
import pytz
from endpoints.db import ClickHouseDB
from endpoints import config, bars

async def test_timestamp_issue():
    """Test if our timestamp handling is causing problems"""
    db = ClickHouseDB()
    
    try:
        # Set up the test
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        ticker = "AAPL"
        
        # Fetch some real data
        print(f"Fetching bars for {ticker}...")
        from_date = now - timedelta(minutes=5)
        to_date = now
        bar_data = await bars.fetch_bars(ticker, from_date, to_date)
        print(f"Retrieved {len(bar_data)} bars")
        
        if not bar_data:
            print("No data fetched, cannot proceed with test")
            return
        
        # Test inserting a bar with a specific timestamp format
        print("\n=== Testing timestamp handling ===")
        
        # Test data: use a bar but add a unique timestamp
        test_bar = bar_data[0].copy()
        
        # Create timestamps in various formats for testing
        test_timestamps = {
            "current_time": now,
            "minus_5min": now - timedelta(minutes=5),
            "minus_1hr": now - timedelta(hours=1),
            "minus_1day": now - timedelta(days=1),
            "plus_5min": now + timedelta(minutes=5),  # Future timestamp - might cause issues
        }
        
        results = {}
        for name, timestamp in test_timestamps.items():
            # Format for logging
            timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')
            date_str = timestamp.strftime('%Y-%m-%d')
            
            # Create a unique ticker for this test
            unique_ticker = f"{ticker}_TS_{name}"
            
            # Create test record with this timestamp
            test_data = {
                "uni_id": int(timestamp.timestamp() * 1000),
                "ticker": unique_ticker,
                "timestamp": timestamp,
                "open": test_bar["open"],
                "high": test_bar["high"],
                "low": test_bar["low"],
                "close": test_bar["close"],
                "volume": test_bar["volume"],
                "vwap": test_bar["vwap"],
                "transactions": test_bar["transactions"]
            }
            
            # Insert using direct SQL to ensure it works
            columns = ", ".join(test_data.keys())
            values = []
            for k, v in test_data.items():
                if k == "timestamp":
                    values.append(f"toDateTime('{timestamp.strftime('%Y-%m-%d %H:%M:%S')}')")
                elif k == "ticker":
                    values.append(f"'{v}'")
                else:
                    values.append(str(v))
                    
            values_str = ", ".join(values)
            
            insert_query = f"""
            INSERT INTO {db.database}.stock_bars ({columns}) VALUES ({values_str})
            """
            
            print(f"\nInserting test record with timestamp {timestamp_str}")
            try:
                db.client.command(insert_query)
                print("Insert succeeded")
                
                # Now check if we can find it with different date checks
                print("Checking with different date query formats...")
                
                # Method 1: Using original format with toDate()
                query1 = f"""
                SELECT COUNT(*) FROM {db.database}.stock_bars 
                WHERE ticker = '{unique_ticker}' AND toDate(timestamp) = '{date_str}'
                """
                result1 = db.client.query(query1)
                count1 = result1.result_rows[0][0]
                print(f"Query 1 (toDate): Found {count1} records")
                
                # Method 2: Using exact timestamp comparison
                exact_ts = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                query2 = f"""
                SELECT COUNT(*) FROM {db.database}.stock_bars 
                WHERE ticker = '{unique_ticker}' AND timestamp = toDateTime('{exact_ts}')
                """
                result2 = db.client.query(query2)
                count2 = result2.result_rows[0][0]
                print(f"Query 2 (exact match): Found {count2} records")
                
                # Method 3: Using date bounds
                start_of_day = datetime.combine(timestamp.date(), datetime.min.time()).replace(tzinfo=et_tz)
                end_of_day = datetime.combine(timestamp.date(), datetime.max.time()).replace(tzinfo=et_tz)
                
                start_str = start_of_day.strftime('%Y-%m-%d %H:%M:%S')
                end_str = end_of_day.strftime('%Y-%m-%d %H:%M:%S')
                
                query3 = f"""
                SELECT COUNT(*) FROM {db.database}.stock_bars 
                WHERE ticker = '{unique_ticker}' 
                AND timestamp >= toDateTime('{start_str}')
                AND timestamp <= toDateTime('{end_str}')
                """
                result3 = db.client.query(query3)
                count3 = result3.result_rows[0][0]
                print(f"Query 3 (date bounds): Found {count3} records")
                
                # Store results
                results[name] = {
                    "timestamp": timestamp_str,
                    "date": date_str,
                    "toDate": count1,
                    "exact": count2,
                    "bounds": count3
                }
                
            except Exception as e:
                print(f"Error with timestamp {timestamp_str}: {str(e)}")
                results[name] = {
                    "timestamp": timestamp_str,
                    "date": date_str,
                    "error": str(e)
                }
        
        # Summary
        print("\n=== Results Summary ===")
        for name, result in results.items():
            print(f"\n{name}: {result['timestamp']}")
            if "error" in result:
                print(f"  Error: {result['error']}")
            else:
                print(f"  Date: {result['date']}")
                print(f"  toDate query: {result['toDate']} records")
                print(f"  Exact match: {result['exact']} records")
                print(f"  Date bounds: {result['bounds']} records")
        
        # Check the date/time settings in the database
        print("\n=== Checking Database Time Settings ===")
        try:
            # Check database timezone
            timezone_query = "SELECT timezone()"
            timezone_result = db.client.query(timezone_query)
            db_timezone = timezone_result.result_rows[0][0]
            print(f"Database timezone: {db_timezone}")
            
            # Check current database time
            now_query = "SELECT now()"
            now_result = db.client.query(now_query)
            db_now = now_result.result_rows[0][0]
            print(f"Database current time: {db_now}")
            
            # Convert our local time to database timezone for comparison
            local_now = datetime.now(et_tz)
            print(f"Local current time: {local_now}")
            
            # Calculate time difference
            if isinstance(db_now, datetime):
                time_diff = abs((db_now - local_now).total_seconds())
                print(f"Time difference: {time_diff:.2f} seconds")
                
                if time_diff > 120:  # More than 2 minutes difference
                    print("⚠️ Warning: Significant time difference between local and database time")
            
        except Exception as e:
            print(f"Error checking database time settings: {str(e)}")
        
    finally:
        db.close()

if __name__ == "__main__":
    from datetime import timedelta
    asyncio.run(test_timestamp_issue()) 