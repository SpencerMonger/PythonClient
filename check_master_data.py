from endpoints.db import ClickHouseDB
from datetime import datetime
import pytz

def format_row(row):
    # Format timestamps and other values nicely
    timestamp = row[1]
    if hasattr(timestamp, 'strftime'):
        timestamp_str = timestamp.strftime('%H:%M:%S')
    else:
        timestamp_str = str(timestamp)
    
    # Format floating point values to 2 decimal places
    formatted_values = []
    for i, val in enumerate(row):
        if i == 1:  # timestamp column
            formatted_values.append(timestamp_str)
        elif isinstance(val, float):
            formatted_values.append(f"{val:.2f}")
        else:
            formatted_values.append(str(val))
    
    return "\t".join(formatted_values)

def main():
    db = ClickHouseDB()
    try:
        print('=== Stock Master Table Summary ===')
        
        # Check total records
        total_query = 'SELECT COUNT(*) FROM stock_master'
        total_result = db.client.query(total_query)
        total_count = total_result.result_rows[0][0]
        print(f"Total Records: {total_count}")
        
        # Check today's records
        today = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d')
        today_query = f"SELECT COUNT(*) FROM stock_master WHERE toDate(timestamp) = '{today}'"
        today_result = db.client.query(today_query)
        today_count = today_result.result_rows[0][0]
        print(f"Today's Records: {today_count}")
        
        # Count by ticker
        tickers_query = f"SELECT ticker, COUNT(*) FROM stock_master WHERE toDate(timestamp) = '{today}' GROUP BY ticker ORDER BY ticker"
        tickers_result = db.client.query(tickers_query)
        print("\nRecords by Ticker:")
        for row in tickers_result.result_rows:
            print(f"  {row[0]}: {row[1]}")
        
        # Get sample data, ordered by timestamp to verify ASC order
        print("\n=== Sample Data (ordered by timestamp ASC, ticker ASC) ===")
        sample_query = f"""
        SELECT ticker, timestamp, open, high, low, close, price_diff, target, daily_high, daily_low 
        FROM stock_master 
        WHERE toDate(timestamp) = '{today}' 
        ORDER BY timestamp ASC, ticker ASC 
        LIMIT 10
        """
        sample_result = db.client.query(sample_query)
        print("Ticker\tTime\tOpen\tHigh\tLow\tClose\tDiff\tTarget\tDaily H\tDaily L")
        for row in sample_result.result_rows:
            print(format_row(row))
        
        # Check normalized data
        print("\n=== Stock Normalized Table Summary ===")
        norm_query = f"SELECT COUNT(*) FROM stock_normalized WHERE toDate(timestamp) = '{today}'"
        norm_result = db.client.query(norm_query)
        norm_count = norm_result.result_rows[0][0] 
        print(f"Today's Normalized Records: {norm_count}")
        
        # Get sample normalized data
        print("\n=== Sample Normalized Data ===")
        norm_sample_query = f"""
        SELECT ticker, timestamp, open, high, low, close, price_diff, target 
        FROM stock_normalized 
        WHERE toDate(timestamp) = '{today}' 
        ORDER BY timestamp ASC, ticker ASC 
        LIMIT 10
        """
        norm_sample_result = db.client.query(norm_sample_query)
        print("Ticker\tTime\tOpen\tHigh\tLow\tClose\tDiff\tTarget")
        for row in norm_sample_result.result_rows:
            print(format_row(row))
            
    finally:
        db.close()

if __name__ == "__main__":
    main() 