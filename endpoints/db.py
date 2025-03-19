import asyncio
from typing import Any, Dict, List
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from endpoints import config
import time
from datetime import datetime
import hashlib
import pytz

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
        self.timezone = pytz.timezone('America/New_York')  # EST timezone

    def _generate_consistent_hash(self, *args) -> int:
        """Generate a consistent hash from the given arguments"""
        # Convert all arguments to strings and join with a delimiter
        hash_str = ':'.join(str(arg or '') for arg in args)
        # Use SHA256 for consistent hashing
        hash_obj = hashlib.sha256(hash_str.encode('utf-8'))
        # Convert first 8 bytes to integer and ensure it's positive
        return int.from_bytes(hash_obj.digest()[:8], byteorder='big') % (2**63)

    def _convert_to_est(self, timestamp: datetime) -> datetime:
        """Convert a datetime to EST timezone"""
        if timestamp.tzinfo is None:
            # If timestamp is naive, assume it's in UTC
            utc_dt = pytz.UTC.localize(timestamp)
        else:
            # If timestamp has timezone, convert to UTC first
            utc_dt = timestamp.astimezone(pytz.UTC)
        # Convert to EST
        return utc_dt.astimezone(self.timezone)

    async def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """
        Insert data into ClickHouse table asynchronously
        """
        if not data:
            return
            
        try:
            # Get column names from first row
            prep_start = time.time()
            columns = list(data[0].keys())
            
            # Convert timestamps to datetime if they're integers
            for row in data:
                # Handle different timestamp field names
                timestamp_fields = ['timestamp', 'sip_timestamp', 'participant_timestamp', 'trf_timestamp']
                for field in timestamp_fields:
                    try:
                        if field in row and row.get(field) is not None:
                            val = row.get(field)
                            if isinstance(val, (int, float)):
                                # Convert nanoseconds to datetime
                                if len(str(int(val))) >= 19:  # nanoseconds
                                    dt = datetime.fromtimestamp(val / 1e9)
                                # Convert milliseconds to datetime
                                elif len(str(int(val))) >= 13:  # milliseconds
                                    dt = datetime.fromtimestamp(val / 1e3)
                                else:  # seconds
                                    dt = datetime.fromtimestamp(val)
                                # Convert to EST
                                row[field] = self._convert_to_est(dt)
                            elif isinstance(val, str):
                                # Try to parse string timestamps
                                try:
                                    dt = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
                                    row[field] = self._convert_to_est(dt)
                                except ValueError:
                                    try:
                                        dt = datetime.strptime(val, '%Y-%m-%d')
                                        row[field] = self._convert_to_est(dt)
                                    except ValueError:
                                        print(f"Warning: Could not parse timestamp string: {val}")
                    except Exception as e:
                        print(f"Error converting timestamp {field}: {str(e)}")
            
            # Add uni_id if not present
            if 'uni_id' not in columns:
                # For indicators table, use ticker + timestamp + indicator_type
                if table_name == config.TABLE_STOCK_INDICATORS:
                    for row in data:
                        try:
                            timestamp = row.get('timestamp')
                            if isinstance(timestamp, datetime):
                                # Use full precision for timestamp string
                                timestamp_str = timestamp.isoformat(' ', 'microseconds')
                            else:
                                timestamp_str = str(timestamp or '')
                            
                            # Generate consistent hash using dedicated method
                            ticker = str(row.get('ticker', ''))
                            indicator_type = str(row.get('indicator_type', ''))
                            row['uni_id'] = self._generate_consistent_hash(ticker, timestamp_str, indicator_type)
                        except Exception as e:
                            print(f"Error generating uni_id for indicators: {str(e)}")
                            row['uni_id'] = 0  # Fallback value
                else:
                    # For all other tables, use ticker + timestamp
                    for row in data:
                        try:
                            main_timestamp = row.get('timestamp') or row.get('sip_timestamp')
                            if isinstance(main_timestamp, datetime):
                                # Use full precision for timestamp string
                                timestamp_str = main_timestamp.isoformat(' ', 'microseconds')
                            else:
                                timestamp_str = str(main_timestamp or '')
                            
                            # Generate consistent hash using dedicated method
                            ticker = str(row.get('ticker', ''))
                            row['uni_id'] = self._generate_consistent_hash(ticker, timestamp_str)
                        except Exception as e:
                            print(f"Error generating uni_id: {str(e)}")
                            row['uni_id'] = 0  # Fallback value
                columns = list(data[0].keys())  # Update columns list
            
            # Extract values in the same order as columns
            print(f"\nPreparing data for {table_name}...")
            values = []
            for row in data:
                try:
                    row_values = []
                    for col in columns:
                        val = row.get(col)
                        # Convert any remaining integer timestamps
                        if col in timestamp_fields and isinstance(val, (int, float)):
                            try:
                                if len(str(int(val))) >= 19:  # nanoseconds
                                    dt = datetime.fromtimestamp(val / 1e9)
                                elif len(str(int(val))) >= 13:  # milliseconds
                                    dt = datetime.fromtimestamp(val / 1e3)
                                else:  # seconds
                                    dt = datetime.fromtimestamp(val)
                                # Convert to EST
                                val = self._convert_to_est(dt)
                            except Exception as e:
                                print(f"Error converting timestamp {col}: {str(e)}")
                                val = None
                        row_values.append(val)
                    values.append(row_values)
                except Exception as e:
                    print(f"Error processing row: {str(e)}")
                    continue  # Skip problematic rows
                
            print(f"Data preparation took: {time.time() - prep_start:.2f} seconds")
            
            print(f"Table: {table_name}")
            print(f"Number of records: {len(data)}")
            print(f"Number of columns: {len(columns)}")
            print(f"Column names: {columns}")
            if values:
                print(f"First row values: {values[0]}")
            
            if not values:
                print(f"No valid data to insert for {table_name}")
                return
            
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
            
        except Exception as e:
            print(f"Error storing {table_name}: {str(e)}")
            print(f"Error type: {type(e)}")
            # Don't re-raise the error to allow the process to continue

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
                # Add uni_id to schema if not present
                if 'uni_id' not in schema:
                    schema = {'uni_id': 'UInt64', **schema}
                
                # Ensure timestamp and ticker are first in the schema
                ordered_schema = {}
                
                # First add timestamp and ticker if they exist
                # Handle different timestamp column names
                timestamp_col = None
                if table_name in ['stock_trades', 'stock_quotes']:
                    timestamp_col = 'sip_timestamp'
                elif 'timestamp' in schema:
                    timestamp_col = 'timestamp'
                elif 'sip_timestamp' in schema:
                    timestamp_col = 'sip_timestamp'
                
                # For tables that might not have timestamp/ticker, use a different ordering
                if table_name == config.TABLE_STOCK_NEWS:
                    # For news table, just use the schema as is
                    ordered_schema = schema
                    columns_def = ", ".join(f"{col} {type_}" for col, type_ in ordered_schema.items())
                    query = f"""
                    CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                        {columns_def}
                    ) ENGINE = MergeTree()
                    ORDER BY id
                    SETTINGS index_granularity = 8192
                    """
                elif table_name == config.TABLE_STOCK_INDICATORS:
                    # For indicators table, use timestamp, ticker, and indicator_type
                    if timestamp_col:
                        ordered_schema[timestamp_col] = schema.pop(timestamp_col)
                    if 'ticker' in schema:
                        ordered_schema['ticker'] = schema.pop('ticker')
                    if 'indicator_type' in schema:
                        ordered_schema['indicator_type'] = schema.pop('indicator_type')
                    ordered_schema.update(schema)
                    
                    columns_def = ", ".join(f"{col} {type_}" for col, type_ in ordered_schema.items())
                    query = f"""
                    CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                        {columns_def}
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp, ticker, indicator_type)
                    SETTINGS index_granularity = 8192
                    """
                else:
                    # For all other tables, enforce timestamp/ticker ordering
                    if timestamp_col:
                        ordered_schema[timestamp_col] = schema.pop(timestamp_col)
                    if 'ticker' in schema:
                        ordered_schema['ticker'] = schema.pop('ticker')
                    
                    # Then add uni_id and the rest
                    ordered_schema.update(schema)
                    
                    columns_def = ", ".join(f"{col} {type_}" for col, type_ in ordered_schema.items())
                    
                    # Add optimized settings for bars table
                    if table_name == config.TABLE_STOCK_BARS:
                        query = f"""
                        CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                            {columns_def}
                        ) ENGINE = MergeTree()
                        ORDER BY (timestamp, ticker, uni_id)
                        SETTINGS 
                            index_granularity = 8192,
                            min_bytes_for_wide_part = 0,
                            min_rows_for_wide_part = 0,
                            parts_to_delay_insert = 0,
                            max_parts_in_total = 100000
                        """
                    elif table_name in ['stock_trades', 'stock_quotes']:
                        # For trades and quotes, use sip_timestamp
                        query = f"""
                        CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                            {columns_def}
                        ) ENGINE = MergeTree()
                        ORDER BY (sip_timestamp, ticker, uni_id)
                        SETTINGS index_granularity = 8192
                        """
                    else:
                        # Default settings for other tables
                        query = f"""
                        CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                            {columns_def}
                        ) ENGINE = MergeTree()
                        ORDER BY ({timestamp_col or 'timestamp'}, ticker, uni_id)
                        SETTINGS index_granularity = 8192
                        """
                
                self.client.command(query)
                print(f"Created table {table_name}")
            else:
                print(f"Table {table_name} already exists")
                
                # If uni_id column doesn't exist, add it
                try:
                    self.client.command(f"SELECT uni_id FROM {self.database}.{table_name} WHERE 1=0")
                except Exception as e:
                    if "Missing columns" in str(e):
                        print(f"Adding uni_id column to {table_name}...")
                        # For indicators table, use ticker + timestamp + indicator_type
                        if table_name == config.TABLE_STOCK_INDICATORS:
                            add_column_query = f"""
                            ALTER TABLE {self.database}.{table_name}
                            ADD COLUMN IF NOT EXISTS uni_id UInt64
                            MATERIALIZED cityHash64(ticker, toString(timestamp), indicator_type)
                            """
                        elif table_name == config.TABLE_STOCK_NEWS:
                            # For news table, just use the id
                            add_column_query = f"""
                            ALTER TABLE {self.database}.{table_name}
                            ADD COLUMN IF NOT EXISTS uni_id UInt64
                            MATERIALIZED cityHash64(toString(id))
                            """
                        else:
                            # For all other tables, use ticker + timestamp
                            # Handle different timestamp column names
                            timestamp_col = 'timestamp'
                            try:
                                self.client.command(f"SELECT timestamp FROM {self.database}.{table_name} WHERE 1=0")
                            except Exception as e:
                                if "Missing columns" in str(e):
                                    timestamp_col = 'sip_timestamp'
                            
                            add_column_query = f"""
                            ALTER TABLE {self.database}.{table_name}
                            ADD COLUMN IF NOT EXISTS uni_id UInt64
                            MATERIALIZED cityHash64(ticker, toString({timestamp_col}))
                            """
                        self.client.command(add_column_query)
                        print(f"Added uni_id column to {table_name}")
                
                # If it's the bars table, try to optimize the settings
                if table_name == config.TABLE_STOCK_BARS:
                    optimize_query = f"""
                    ALTER TABLE {self.database}.{table_name}
                    MODIFY SETTING
                        min_bytes_for_wide_part = 0,
                        min_rows_for_wide_part = 0,
                        parts_to_delay_insert = 0,
                        max_parts_in_total = 100000
                    """
                    try:
                        self.client.command(optimize_query)
                        print(f"Optimized settings for {table_name}")
                    except Exception as e:
                        print(f"Could not optimize settings: {str(e)}")
                    
        except Exception as e:
            print(f"Error creating table {table_name}: {str(e)}")
            raise e

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