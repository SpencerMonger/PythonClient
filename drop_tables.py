from endpoints.db import ClickHouseDB

def main():
    db = ClickHouseDB()
    try:
        print("Dropping stock_trades table...")
        db.drop_table_if_exists('stock_trades')
        
        print("Dropping stock_quotes table...")
        db.drop_table_if_exists('stock_quotes')
        
        print("Dropping stock_bars table...")
        db.drop_table_if_exists('stock_bars')

        print("Dropping stock_indicators table...")
        db.drop_table_if_exists('stock_indicators')
        
        print("Dropping stock_news table...")
        db.drop_table_if_exists('stock_news')

        print("All tables dropped successfully!")
    finally:
        db.close()

if __name__ == "__main__":
    main() 


Make sure you really want to run this code