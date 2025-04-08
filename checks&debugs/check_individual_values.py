from endpoints.db import ClickHouseDB
from datetime import datetime
import pytz

def check_individual_values():
    db = ClickHouseDB()
    try:
        # Get today's date in ET timezone
        today = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d')
        
        # Check normalized values
        query = f"""
        SELECT 
            ticker, 
            timestamp, 
            open, 
            high, 
            low, 
            close,
            price_diff
        FROM stock_normalized 
        WHERE toDate(timestamp) = '{today}'
        ORDER BY timestamp ASC, ticker ASC 
        """
        result = db.client.query(query)
        
        print("\nINDIVIDUAL NORMALIZED VALUES:")
        print("Ticker  Time      Open    High    Low     Close   Price_Diff")
        for row in result.result_rows:
            time_str = row[1].strftime("%H:%M:%S")
            open_val = f"{row[2]:.3f}" if row[2] is not None else "None"
            high_val = f"{row[3]:.3f}" if row[3] is not None else "None"
            low_val = f"{row[4]:.3f}" if row[4] is not None else "None"
            close_val = f"{row[5]:.3f}" if row[5] is not None else "None"
            price_diff_val = f"{row[6]:.3f}" if row[6] is not None else "None"
            print(f"{row[0]}  {time_str}  {open_val}   {high_val}   {low_val}   {close_val}   {price_diff_val}")
            
        # Compare with original values
        orig_query = f"""
        SELECT 
            ticker, 
            timestamp, 
            open, 
            high, 
            low, 
            close,
            price_diff
        FROM stock_master 
        WHERE toDate(timestamp) = '{today}'
        ORDER BY timestamp ASC, ticker ASC 
        """
        orig_result = db.client.query(orig_query)
        
        print("\nORIGINAL VALUES:")
        print("Ticker  Time      Open    High    Low     Close   Price_Diff")
        for row in orig_result.result_rows:
            time_str = row[1].strftime("%H:%M:%S")
            open_val = f"{row[2]:.3f}" if row[2] is not None else "None"
            high_val = f"{row[3]:.3f}" if row[3] is not None else "None"
            low_val = f"{row[4]:.3f}" if row[4] is not None else "None"
            close_val = f"{row[5]:.3f}" if row[5] is not None else "None"
            price_diff_val = f"{row[6]:.3f}" if row[6] is not None else "None"
            print(f"{row[0]}  {time_str}  {open_val}   {high_val}   {low_val}   {close_val}   {price_diff_val}")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_individual_values() 