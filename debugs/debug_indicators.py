from endpoints.db import ClickHouseDB

def analyze_indicators():
    db = ClickHouseDB()
    
    # Check date range
    date_range = db.client.command("""
        SELECT 
            min(timestamp) as min_date,
            max(timestamp) as max_date
        FROM stock_indicators
    """)
    print("Date range:", date_range)
    
    # Check indicator types and their counts
    indicator_counts = db.client.command("""
        SELECT 
            indicator_type,
            count(*) as count,
            min(timestamp) as first_date,
            max(timestamp) as last_date
        FROM stock_indicators
        GROUP BY indicator_type
        ORDER BY count DESC
    """)
    print("\nIndicator type counts:", indicator_counts)
    
    # Check data around January 10th
    jan_10_data = db.client.command("""
        SELECT 
            indicator_type,
            count(*) as count
        FROM stock_indicators
        WHERE timestamp >= '2025-01-10 00:00:00'
        GROUP BY indicator_type
        ORDER BY count DESC
    """)
    print("\nData after Jan 10th:", jan_10_data)
    
    # Compare with master table
    master_counts = db.client.command("""
        SELECT 
            count(*) as total_rows,
            count(DISTINCT ticker) as unique_tickers,
            min(timestamp) as min_date,
            max(timestamp) as max_date
        FROM stock_master
    """)
    print("\nMaster table stats:", master_counts)
    
    # Check daily counts
    daily_counts = db.client.command("""
        SELECT 
            toDate(timestamp) as date,
            count(*) as total_rows,
            count(DISTINCT indicator_type) as unique_indicators,
            count(DISTINCT ticker) as unique_tickers
        FROM stock_indicators
        GROUP BY date
        ORDER BY date
    """)
    print("\nDaily counts:", daily_counts)
    
    # Compare with master table daily counts
    master_daily = db.client.command("""
        SELECT 
            toDate(timestamp) as date,
            count(*) as total_rows,
            count(DISTINCT ticker) as unique_tickers
        FROM stock_master
        GROUP BY date
        ORDER BY date
    """)
    print("\nMaster table daily counts:", master_daily)
    
    # Check which tickers are present in early February
    feb_tickers = db.client.command("""
        SELECT 
            toDate(timestamp) as date,
            arraySort(groupUniqArray(ticker)) as tickers,
            count(DISTINCT ticker) as ticker_count
        FROM stock_indicators
        WHERE timestamp >= '2025-02-01' AND timestamp <= '2025-02-05'
        GROUP BY date
        ORDER BY date
    """)
    print("\nFebruary tickers in indicators:", feb_tickers)
    
    # Compare with master table tickers
    master_tickers = db.client.command("""
        SELECT DISTINCT ticker
        FROM stock_master
        ORDER BY ticker
    """)
    print("\nAll tickers in master:", master_tickers)

if __name__ == "__main__":
    analyze_indicators() 