from endpoints.db import ClickHouseDB
from endpoints import config

def main():
    db = ClickHouseDB()
    try:
        # Drop master and normalized tables first (since they depend on other tables)
        print("Dropping stock_normalized table...")
        db.drop_table_if_exists('stock_normalized')
        
        print("Dropping stock_master table...")
        db.drop_table_if_exists(config.TABLE_STOCK_MASTER)
        
        # Drop core tables
        print("Dropping stock_trades table...")
        db.drop_table_if_exists('stock_trades')
        
        print("Dropping stock_quotes table...")
        db.drop_table_if_exists('stock_quotes')
        
        print("Dropping stock_bars table...")
        db.drop_table_if_exists('stock_bars')
        
        print("Dropping stock_daily table...")  # Added daily bars table
        db.drop_table_if_exists('stock_daily')

        print("Dropping stock_indicators table...")
        db.drop_table_if_exists('stock_indicators')
        
        print("Dropping stock_news table...")
        db.drop_table_if_exists('stock_news')

        print("All tables dropped successfully!")
    finally:
        db.close()

if __name__ == "__main__":
    main() 
