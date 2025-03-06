from endpoints.db import ClickHouseDB
from endpoints import config

def main():
    db = ClickHouseDB()
    try:
        print("Dropping stock_master_mv materialized view...")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master_mv")
        print("Materialized view dropped successfully")
        
        print("\nDropping stock_master table...")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.{config.TABLE_STOCK_MASTER}")
        print("Master table dropped successfully")
        
        print("\nAll master tables dropped successfully!")
    except Exception as e:
        print(f"Error dropping master tables: {str(e)}")
        raise e
    finally:
        db.close()

if __name__ == "__main__":
    main() 