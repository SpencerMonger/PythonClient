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
        # Historical date to check
        historical_date = '2025-03-24'  # Yesterday
        today = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d')
        
        print(f'=== Historical Data Comparison: {historical_date} vs {today} ===')
        
        # Check historical record count
        historical_query = f"SELECT COUNT(*) FROM stock_master WHERE toDate(timestamp) = '{historical_date}'"
        historical_result = db.client.query(historical_query)
        historical_count = historical_result.result_rows[0][0]
        print(f"Historical Records ({historical_date}): {historical_count}")
        
        # Get sample historical data
        print(f"\n=== Sample Historical Data ({historical_date}) ===")
        hist_sample_query = f"""
        SELECT ticker, timestamp, open, high, low, close, price_diff, target, daily_high, daily_low 
        FROM stock_master 
        WHERE toDate(timestamp) = '{historical_date}' 
        ORDER BY timestamp ASC, ticker ASC 
        LIMIT 10
        """
        hist_sample_result = db.client.query(hist_sample_query)
        print("Ticker\tTime\tOpen\tHigh\tLow\tClose\tDiff\tTarget\tDaily H\tDaily L")
        for row in hist_sample_result.result_rows:
            print(format_row(row))
        
        # Get sample current data 
        print(f"\n=== Sample Current Data ({today}) ===")
        current_sample_query = f"""
        SELECT ticker, timestamp, open, high, low, close, price_diff, target, daily_high, daily_low 
        FROM stock_master 
        WHERE toDate(timestamp) = '{today}' 
        ORDER BY timestamp ASC, ticker ASC 
        LIMIT 10
        """
        current_sample_result = db.client.query(current_sample_query)
        print("Ticker\tTime\tOpen\tHigh\tLow\tClose\tDiff\tTarget\tDaily H\tDaily L")
        for row in current_sample_result.result_rows:
            print(format_row(row))
            
        # Check structure consistency - column order and types
        print("\n=== Column Structure Verification ===")
        info_query = f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{db.database}' AND table = 'stock_master'
        ORDER BY position
        """
        info_result = db.client.query(info_query)
        print("Column Name\tData Type")
        for row in info_result.result_rows:
            print(f"{row[0]}\t{row[1]}")
            
    finally:
        db.close()

if __name__ == "__main__":
    main() 