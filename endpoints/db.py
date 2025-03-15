import asyncio
from typing import Any, Dict, List
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from endpoints import config

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
        columns = list(data[0].keys())
        
        # Extract values in the same order as columns
        values = [[row[col] for col in columns] for row in data]
        
        # Create a function that will be executed in the thread pool
        def insert_func():
            self.client.insert(f"{self.database}.{table_name}", values, column_names=columns)
        
        # Run insert in thread pool to not block
        await asyncio.get_event_loop().run_in_executor(None, insert_func)

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