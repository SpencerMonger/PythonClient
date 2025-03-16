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
        columns = list(data[0].keys())
        
        # Extract values in the same order as columns
        values = [[row[col] for col in columns] for row in data]
        
        print(f"\nInserting {len(data)} records into {table_name}...")
        
        # Create a function that will be executed in the thread pool
        def insert_func():
            try:
                insert_start = time.time()
                
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
                
                print(f"Data inserted successfully in {time.time() - insert_start:.2f} seconds")
                
            except Exception as e:
                print(f"Error during insert: {str(e)}")
                raise e
        
        # Run insert in thread pool to not block
        async_start = time.time()
        await asyncio.get_event_loop().run_in_executor(None, insert_func)
        print(f"Total operation took: {time.time() - async_start:.2f} seconds")

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

    def deduplicate_tables(self) -> None:
        """
        Remove duplicate entries from source tables based on timestamp and ticker
        using ReplacingMergeTree engine. Note: Materialized views (stock_master, stock_normalized) 
        are not deduplicated as they automatically update with their source tables.
        """
        try:
            print("\nStarting table deduplication...")
            
            # List of source tables to deduplicate (excluding materialized views)
            tables = [
                'stock_bars',
                'stock_daily',
                'stock_trades',
                'stock_quotes',
                'stock_news',
                'stock_indicators'
            ]
            
            # Map of tables to their timestamp column names
            timestamp_cols = {
                'stock_bars': 'timestamp',
                'stock_daily': 'timestamp',
                'stock_trades': 'sip_timestamp',
                'stock_quotes': 'sip_timestamp',
                'stock_news': 'created_at',
                'stock_indicators': 'timestamp'
            }

            # Map of ClickHouse type abbreviations to full type names
            type_mapping = {
                'i': 'Int64',
                'd': 'DateTime64(9)',
                'f': 'Float64',
                's': 'String',
                'b': 'Boolean',
                'dt': 'DateTime',
                'ts': 'DateTime64(9)',
                't': 'DateTime64(9)',
                'dec': 'Decimal(18,6)',
                'a': 'String',      # Used for text fields like ticker, article_url
                'u': 'Float64',     # Used for numeric fields like price, size
                'r': 'Array(String)', # Used for arrays like conditions, keywords
                'n': 'Int32',       # Used for window sizes
                'l': 'Float64',     # Used for float values
                'h': 'Float64',     # Used for histogram values
                'v': 'Float64',     # Used for volume, value
                'p': 'Float64',     # Used for price
                'o': 'Float64',     # Used for open price
                'c': 'Float64',     # Used for close price
                'e': 'String',      # Used for exchange
                'k': 'Array(String)', # Used for keywords
                'w': 'Int32'        # Used for window
            }
            
            for table in tables:
                print(f"\nDeduplicating {table}...")
                
                try:
                    # First verify table exists
                    if not self.table_exists(table):
                        print(f"Table {table} does not exist, skipping...")
                        continue
                    
                    # Get table schema
                    print(f"Fetching schema for {table}...")
                    schema_result = self.client.command(f"DESCRIBE {self.database}.{table}")
                    
                    if not schema_result:
                        print(f"No schema found for {table}, skipping...")
                        continue
                    
                    # Build column definitions directly from DESCRIBE result
                    columns_def = []
                    print("\nProcessing schema:")
                    for row in schema_result:
                        if not row or len(row) < 2:
                            continue
                            
                        # Extract column name and type from schema result
                        col_name = str(row[0]).strip().replace('\n', '')
                        col_type = str(row[1]).strip().replace('\n', '')
                        
                        print(f"Column: {col_name}, Original Type: {col_type}")
                        
                        # Skip empty or invalid definitions
                        if not col_name or not col_type:
                            print(f"Skipping invalid column definition: {row}")
                            continue
                        
                        # Get the base type (first word) and check if it needs mapping
                        base_type = col_type.split()[0].lower()
                        print(f"Base type: {base_type}")
                        
                        if base_type in type_mapping:
                            col_type = type_mapping[base_type]
                            print(f"Mapped to: {col_type}")
                        elif base_type.startswith('nullable'):
                            inner_type = base_type[9:-1]  # Remove Nullable() wrapper
                            print(f"Nullable inner type: {inner_type}")
                            if inner_type in type_mapping:
                                col_type = f"Nullable({type_mapping[inner_type]})"
                                print(f"Mapped nullable to: {col_type}")
                        else:
                            # Keep the original type if no mapping exists
                            print(f"Using original type: {col_type}")
                        
                        columns_def.append(f"{col_name} {col_type}")
                    
                    if not columns_def:
                        print(f"No valid columns found for {table}, skipping...")
                        continue
                    
                    print("\nFinal column definitions:")
                    for col_def in columns_def:
                        print(col_def)
                    
                    # Create temporary ReplacingMergeTree table
                    print(f"Creating temporary table for {table}...")
                    temp_table = f"{table}_dedup"
                    
                    # Use standard settings that we know work
                    settings_str = """
                    SETTINGS 
                        index_granularity = 8192,
                        min_bytes_for_wide_part = 0,
                        min_rows_for_wide_part = 0,
                        parts_to_delay_insert = 0,
                        max_parts_in_total = 100000,
                        max_insert_block_size = 1048576
                    """
                    
                    create_query = f"""
                    CREATE TABLE IF NOT EXISTS {self.database}.{temp_table}
                    (
                        {', '.join(columns_def)}
                    ) ENGINE = ReplacingMergeTree({timestamp_cols[table]})
                    PRIMARY KEY (ticker, {timestamp_cols[table]})
                    ORDER BY (ticker, {timestamp_cols[table]})
                    {settings_str}
                    """
                    print(f"\nCreating table with query:\n{create_query}")
                    self.client.command(create_query)
                    
                    # Insert data from original table
                    print(f"Copying data to temporary table...")
                    insert_query = f"""
                    INSERT INTO {self.database}.{temp_table}
                    SELECT * FROM {self.database}.{table}
                    """
                    self.client.command(insert_query)
                    
                    # Optimize to force merge and deduplication
                    print(f"Optimizing temporary table...")
                    self.client.command(f"OPTIMIZE TABLE {self.database}.{temp_table} FINAL")
                    
                    # Replace original table with deduplicated data
                    print(f"Replacing original table with deduplicated data...")
                    self.client.command(f"DROP TABLE IF EXISTS {self.database}.{table}")
                    self.client.command(f"RENAME TABLE {self.database}.{temp_table} TO {self.database}.{table}")
                    
                    print(f"Successfully deduplicated {table}")
                    
                except Exception as e:
                    print(f"Error deduplicating {table}: {str(e)}")
                    # Continue with other tables even if one fails
                    continue
                    
            print("\nTable deduplication completed")
            
        except Exception as e:
            print(f"Error during deduplication: {str(e)}")
            raise e 