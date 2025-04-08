from endpoints.db import ClickHouseDB
from datetime import datetime
import pytz

def check_statistics():
    db = ClickHouseDB()
    try:
        # Get today's date in ET timezone
        today = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d')
        
        # Check value ranges for key columns
        metrics = ['open', 'high', 'low', 'close', 'volume', 'vwap', 'price_diff']
        for metric in metrics:
            query = f"""
            SELECT 
                ticker, 
                min({metric}) as min_val, 
                max({metric}) as max_val, 
                avg({metric}) as avg_val, 
                stddevPop({metric}) as std_val,
                count(*) as count
            FROM stock_master 
            WHERE toDate(timestamp) = '{today}'
            GROUP BY ticker
            """
            result = db.client.query(query)
            print(f"\n{metric.upper()} Statistics:")
            print("Ticker\tMin\tMax\tAvg\tStdDev\tCount")
            for row in result.result_rows:
                print(f"{row[0]}\t{row[1]:.2f}\t{row[2]:.2f}\t{row[3]:.2f}\t{row[4]:.4f}\t{row[5]}")
        
        # Check normalized values
        for metric in metrics:
            norm_query = f"""
            SELECT 
                ticker, 
                min({metric}) as min_val, 
                max({metric}) as max_val, 
                avg({metric}) as avg_val, 
                stddevPop({metric}) as std_val,
                count(*) as count
            FROM stock_normalized 
            WHERE toDate(timestamp) = '{today}'
            GROUP BY ticker
            """
            norm_result = db.client.query(norm_query)
            print(f"\nNormalized {metric.upper()} Statistics:")
            print("Ticker\tMin\tMax\tAvg\tStdDev\tCount")
            for row in norm_result.result_rows:
                print(f"{row[0]}\t{row[1]:.2f}\t{row[2]:.2f}\t{row[3]:.2f}\t{row[4]:.4f}\t{row[5]}")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_statistics() 