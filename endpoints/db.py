import asyncio
from typing import Any, Dict, List
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from endpoints import config
import time

class ClickHouseDB:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_HTTP_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DATABASE,
            secure=config.CLICKHOUSE_SECURE
        )
        self.database = config.CLICKHOUSE_DATABASE

    async def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """
        Insert data into ClickHouse table asynchronously
        """
        if not data:
            return
            
        # Get column names from first row
        prep_start = time.time()
        columns = list(data[0].keys())
        
        # Extract values in the same order as columns
        print(f"\nPreparing data for {table_name}...")
        values = [[row[col] for col in columns] for row in data]
        print(f"Data preparation took: {time.time() - prep_start:.2f} seconds")
        
        print(f"Table: {table_name}")
        print(f"Number of records: {len(data)}")
        print(f"Number of columns: {len(columns)}")
        print(f"Column names: {columns}")
        print(f"First row values: {values[0]}")
        
        # Create a function that will be executed in the thread pool
        def insert_func():
            try:
                print(f"\nStarting actual insert for {table_name}...")
                insert_start = time.time()
                
                # Try to get table schema
                try:
                    schema_start = time.time()
                    schema = self.client.command(f"DESCRIBE TABLE {self.database}.{table_name}")
                    print(f"Schema retrieval took: {time.time() - schema_start:.2f} seconds")
                    print(f"Table schema: {schema}")
                except Exception as e:
                    print(f"Error getting schema: {str(e)}")
                
                # For bars table, use optimized insert settings
                if table_name == config.TABLE_STOCK_BARS:
                    settings = {
                        'async_insert': 1,
                        'wait_for_async_insert': 0,
                        'optimize_on_insert': 0
                    }
                    self.client.insert(f"{self.database}.{table_name}", values, column_names=columns, settings=settings)
                else:
                    self.client.insert(f"{self.database}.{table_name}", values, column_names=columns)
                
                print(f"Actual insert operation took: {time.time() - insert_start:.2f} seconds")
                
            except Exception as e:
                print(f"Error during insert: {str(e)}")
                print(f"Error type: {type(e)}")
                raise e
        
        # Run insert in thread pool to not block
        print("\nStarting async insert...")
        async_start = time.time()
        await asyncio.get_event_loop().run_in_executor(None, insert_func)
        print(f"Total async operation took: {time.time() - async_start:.2f} seconds")

    def drop_table_if_exists(self, table_name: str) -> None:
        """
        Drop table if it exists
        """
        try:
            self.client.command(f"DROP TABLE IF EXISTS {self.database}.{table_name}")
        except Exception as e:
            print(f"Error dropping table {table_name}: {str(e)}")

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists
        """
        try:
            result = self.client.command(f"SELECT 1 FROM {self.database}.{table_name} WHERE 1=0")
            return True
        except Exception as e:
            if "Table" in str(e) and "doesn't exist" in str(e):
                return False
            print(f"Error checking table existence: {str(e)}")
            return False

    def create_table_if_not_exists(self, table_name: str, schema: Dict[str, str]) -> None:
        """
        Create table if it doesn't exist using provided schema
        """
        try:
            if not self.table_exists(table_name):
                columns_def = ", ".join(f"{col} {type_}" for col, type_ in schema.items())
                
                # Add optimized settings for bars table
                if table_name == config.TABLE_STOCK_BARS:
                    query = f"""
                    CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                        {columns_def}
                    ) ENGINE = MergeTree()
                    PRIMARY KEY (timestamp, ticker)
                    ORDER BY (timestamp, ticker)
                    SETTINGS 
                        index_granularity = 8192,
                        min_bytes_for_wide_part = 0,
                        min_rows_for_wide_part = 0,
                        parts_to_delay_insert = 0,
                        max_parts_in_total = 100000,
                        max_insert_block_size = 1048576,
                        optimize_on_insert = 0
                    """
                else:
                    # Default settings for other tables
                    query = f"""
                    CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                        {columns_def}
                    ) ENGINE = MergeTree()
                    PRIMARY KEY (timestamp, ticker)
                    ORDER BY (timestamp, ticker)
                    SETTINGS index_granularity = 8192
                    """
                    
                self.client.command(query)
                print(f"Created table {table_name}")
            else:
                print(f"Table {table_name} already exists")
                
                # If it's the bars table, try to optimize the settings
                if table_name == config.TABLE_STOCK_BARS:
                    optimize_query = f"""
                    ALTER TABLE {self.database}.{table_name}
                    MODIFY SETTING
                        min_bytes_for_wide_part = 0,
                        min_rows_for_wide_part = 0,
                        parts_to_delay_insert = 0,
                        max_parts_in_total = 100000,
                        max_insert_block_size = 1048576,
                        optimize_on_insert = 0
                    """
                    try:
                        self.client.command(optimize_query)
                        print(f"Optimized settings for {table_name}")
                    except Exception as e:
                        print(f"Could not optimize settings: {str(e)}")
                    
        except Exception as e:
            print(f"Error creating table {table_name}: {str(e)}")

    def recreate_table(self, table_name: str, schema: Dict[str, str]) -> None:
        """
        Drop and recreate table with new schema
        """
        self.drop_table_if_exists(table_name)
        self.create_table_if_not_exists(table_name, schema)

    def close(self) -> None:
        """
        Close database connection
        """
        self.client.close() 