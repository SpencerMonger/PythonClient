import asyncio
from datetime import datetime
import pytz
from endpoints.db import ClickHouseDB
from endpoints import config, bars

async def test_insert_direct():
    """Test inserting data using direct SQL commands"""
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
            print("No data fetched, cannot proceed with insert test")
            return
        
        # Insert a single bar using direct SQL
        print("\n=== Testing direct SQL insert ===")
        test_bar = bar_data[0]
        
        # Convert timestamp to a proper SQL format
        if isinstance(test_bar["timestamp"], (int, float)):
            timestamp = datetime.fromtimestamp(test_bar["timestamp"] / 1e9)
            timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        else:
            timestamp = test_bar["timestamp"].strftime('%Y-%m-%d %H:%M:%S')
        
        # Generate a unique ID
        uni_id = int(now.timestamp() * 1000)
        
        insert_query = f"""
        INSERT INTO {db.database}.stock_bars (
            uni_id, ticker, timestamp, open, high, low, close, volume, vwap, transactions
        ) VALUES (
            {uni_id}, '{ticker}_TEST', '{timestamp}', 
            {test_bar["open"]}, {test_bar["high"]}, {test_bar["low"]}, {test_bar["close"]}, 
            {test_bar["volume"]}, {test_bar["vwap"]}, {test_bar["transactions"]}
        )
        """
        
        try:
            db.client.command(insert_query)
            print("Direct SQL insert succeeded")
            
            # Check if it was inserted
            result = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_bars WHERE ticker = '{ticker}_TEST'")
            count = result.result_rows[0][0]
            print(f"Found {count} test records after direct SQL insert")
        except Exception as e:
            print(f"Direct SQL insert failed: {str(e)}")
        
        # Test our bulk insert method with a modified wait_for_async_insert flag
        print("\n=== Testing bulk insert with modified settings ===")
        
        # Modify the data to use a different ticker name
        modified_data = []
        for i, bar in enumerate(bar_data[:5]):  # Only use first 5 bars
            bar_copy = bar.copy()
            bar_copy["ticker"] = f"{ticker}_BULK_{i}"
            modified_data.append(bar_copy)
        
        # Try different settings for bulk insert
        try:
            # Prepare direct insert without using the helper function
            columns = list(modified_data[0].keys())
            values = []
            for row in modified_data:
                try:
                    row_values = []
                    for col in columns:
                        val = row.get(col)
                        # Convert timestamp to datetime string
                        if col == 'timestamp' and isinstance(val, (int, float)):
                            dt = datetime.fromtimestamp(val / 1e9)
                            val = dt.strftime('%Y-%m-%d %H:%M:%S')
                        row_values.append(val)
                    values.append(row_values)
                except Exception as e:
                    print(f"Error preparing row: {str(e)}")
            
            # Insert with explicit wait_for_async_insert=1
            settings = {
                'async_insert': 0,  # Disable async insert completely
                'wait_for_async_insert': 0,
                'optimize_on_insert': 0
            }
            
            # Direct insert using Python client
            db.client.insert(f"{db.database}.stock_bars", values, column_names=columns, settings=settings)
            print("Bulk insert succeeded")
            
            # Check if they were inserted
            result = db.client.query(f"SELECT COUNT(*) FROM {db.database}.stock_bars WHERE ticker LIKE '{ticker}_BULK_%'")
            count = result.result_rows[0][0]
            print(f"Found {count} test records after bulk insert")
        except Exception as e:
            print(f"Bulk insert failed: {str(e)}")
        
    finally:
        db.close()

if __name__ == "__main__":
    from datetime import timedelta
    asyncio.run(test_insert_direct()) 