from endpoints.db import ClickHouseDB
from endpoints import config

def main():
    db = ClickHouseDB()
    try:
        print("Dropping stock_normalized_mv materialized view...")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized_mv")
        print("Normalized materialized view dropped successfully")
        
        print("\nDropping stock_normalized table...")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_normalized")
        print("Normalized table dropped successfully")
        
        print("\nDropping stock_master_mv materialized view...")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.stock_master_mv")
        print("Master materialized view dropped successfully")
        
        print("\nDropping stock_master table...")
        db.client.command(f"DROP TABLE IF EXISTS {db.database}.{config.TABLE_STOCK_MASTER}")
        print("Master table dropped successfully")
        
        print("\nAll tables dropped successfully!")
    except Exception as e:
        print(f"Error dropping tables: {str(e)}")
        raise e
    finally:
        db.close()

if __name__ == "__main__":
    main() 