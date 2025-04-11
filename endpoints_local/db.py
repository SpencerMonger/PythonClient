import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from endpoints import config
import time
from datetime import datetime, timezone
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
        self._executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix='ClickHouseWorker')

    def _generate_consistent_hash(self, *args) -> int:
        hash_str = ':'.join(str(arg or '') for arg in args)
        hash_obj = hashlib.sha256(hash_str.encode('utf-8'))
        return int.from_bytes(hash_obj.digest()[:8], byteorder='big') % (2**63)

    def _ensure_utc(self, timestamp: Any) -> datetime | None:
        if timestamp is None: return None
        dt = None
        if isinstance(timestamp, datetime):
            if timestamp.tzinfo is None: dt = pytz.UTC.localize(timestamp)
            else: dt = timestamp.astimezone(pytz.UTC)
        elif isinstance(timestamp, (int, float)):
            try:
                ts_val = float(timestamp)
                if len(str(int(ts_val))) >= 17: dt = datetime.fromtimestamp(ts_val / 1e9, tz=timezone.utc)
                elif len(str(int(ts_val))) >= 11: dt = datetime.fromtimestamp(ts_val / 1e3, tz=timezone.utc)
                else: dt = datetime.fromtimestamp(ts_val, tz=timezone.utc)
            except (ValueError, OSError): return None
        elif isinstance(timestamp, str):
            formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S.%fZ']
            for fmt in formats:
                try:
                    dt_naive = datetime.strptime(timestamp, fmt)
                    dt = pytz.UTC.localize(dt_naive); break
                except ValueError: continue
            if dt is None:
                try:
                    dt_aware = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    dt = dt_aware.astimezone(pytz.UTC)
                except ValueError: return None
        if dt is not None and isinstance(timestamp, (int, float)) and len(str(int(timestamp))) >= 17:
             nanos = int(timestamp % 1e9); dt = dt.replace(microsecond=nanos // 1000)
        return dt

    async def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        if not data: return
        processed_data = []
        original_cols_had_uni_id = False
        try:
             columns = list(data[0].keys()); original_cols_had_uni_id = 'uni_id' in columns
        except IndexError:
             print(f"Empty data list passed to insert_data for {table_name}")
             return

        timestamp_fields = ['timestamp', 'sip_timestamp', 'participant_timestamp', 'trf_timestamp']

        for i, row_orig in enumerate(data):
            row = row_orig.copy()
            try:
                for field in timestamp_fields:
                    if field in row: row[field] = self._ensure_utc(row[field])

                if not original_cols_had_uni_id:
                    main_ts_dt = row.get('timestamp') or row.get('sip_timestamp')
                    ts_ns = int(main_ts_dt.timestamp() * 1e9) if isinstance(main_ts_dt, datetime) else str(main_ts_dt or '')
                    ticker = str(row.get('ticker', ''))
                    hash_args = [ticker, ts_ns]
                    if table_name == config.TABLE_STOCK_INDICATORS: hash_args.append(str(row.get('indicator_type','')))
                    elif table_name == config.TABLE_STOCK_NEWS: hash_args = [str(row.get('id',''))]
                    elif table_name == config.TABLE_STOCK_PREDICTIONS:
                         prediction_time = self._ensure_utc(row.get('prediction_time'))
                         pred_ns = int(prediction_time.timestamp()*1e9) if prediction_time else ''
                         hash_args.extend([pred_ns, str(row.get('predicted_value',''))])
                    row['uni_id'] = self._generate_consistent_hash(*hash_args)

                processed_data.append(row)
            except Exception as e:
                 # print(f"Error processing row {i} for {table_name}: {e}")
                 continue

        if not processed_data: return
        # Ensure columns list includes uni_id if it was added AFTER checking processed_data
        if not original_cols_had_uni_id and 'uni_id' in processed_data[0]:
            columns = list(processed_data[0].keys())

        values = []
        for row in processed_data:
             try: values.append([row.get(col) for col in columns])
             except Exception: continue

        if not values: return

        loop = asyncio.get_running_loop()
        def _blocking_insert():
            try:
                settings = {'async_insert': 1, 'wait_for_async_insert': 0, 'optimize_on_insert': 0}
                if self.client and hasattr(self.client, 'insert'):
                     self.client.insert(f"`{self.database}`.`{table_name}`", values, column_names=columns, settings=settings)
                else: print(f"DB client invalid in executor for {table_name}")
            except Exception as e: print(f"Blocking insert error for {table_name}: {type(e).__name__} - {e}")

        try:
            await loop.run_in_executor(self._executor, _blocking_insert)
        except Exception as e:
            print(f"Executor scheduling error for {table_name}: {type(e).__name__} - {e}")


    def drop_table_if_exists(self, table_name: str) -> None:
        try: self.client.command(f"DROP TABLE IF EXISTS `{self.database}`.`{table_name}`")
        except Exception as e: print(f"Error dropping table {table_name}: {str(e)}")

    def table_exists(self, table_name: str) -> bool:
        try: self.client.command(f"SELECT 1 FROM `{self.database}`.`{table_name}` WHERE 1=0"); return True
        except Exception as e:
            if "Table" in str(e) and "doesn't exist" in str(e): return False
            if "Unknown table expression identifier" in str(e): return False
            # print(f"Error checking table existence {table_name}: {str(e)}") # Less verbose
            return False

    def create_table_if_not_exists(self, table_name: str, schema: Dict[str, str]) -> None:
        # Keep implementation from previous correct edit (using backticks, etc.)
        try:
            if not self.table_exists(table_name):
                if 'uni_id' not in schema: schema = {'uni_id': 'UInt64', **schema}
                timestamp_col = None
                if table_name in [config.TABLE_STOCK_TRADES, config.TABLE_STOCK_QUOTES]: timestamp_col = 'sip_timestamp'
                elif 'timestamp' in schema: timestamp_col = 'timestamp'

                engine_clause = "ENGINE = MergeTree()"
                order_by_cols, primary_key_cols = [], []
                if timestamp_col: order_by_cols.append(timestamp_col)
                if 'ticker' in schema: order_by_cols.append('ticker')

                if table_name == config.TABLE_STOCK_NEWS:
                    order_by_cols, primary_key_cols = ['id'], ['id']
                elif table_name == config.TABLE_STOCK_INDICATORS:
                    order_by_cols = ['timestamp', 'ticker', 'indicator_type']
                    primary_key_cols = ['timestamp', 'ticker', 'indicator_type']
                elif table_name in [config.TABLE_STOCK_MASTER, config.TABLE_STOCK_NORMALIZED, config.TABLE_STOCK_PREDICTIONS]:
                    engine_clause = "ENGINE = ReplacingMergeTree()"
                    key_ts = timestamp_col or 'timestamp'
                    primary_key_cols = [key_ts, 'ticker']
                    if table_name == config.TABLE_STOCK_PREDICTIONS: primary_key_cols.append('uni_id')
                    order_by_cols = primary_key_cols[:]
                else: # Default Bars, Trades, Quotes
                    order_by_cols.append('uni_id')
                    key_ts = timestamp_col or 'timestamp'
                    primary_key_cols = [key_ts, 'ticker']

                final_ordered_schema = {}
                if 'uni_id' in schema: final_ordered_schema['uni_id'] = schema.pop('uni_id')
                if timestamp_col and timestamp_col in schema: final_ordered_schema[timestamp_col] = schema.pop(timestamp_col)
                if 'ticker' in schema: final_ordered_schema['ticker'] = schema.pop('ticker')
                final_ordered_schema.update(schema)

                columns_def = ", ".join(f"`{col}` {type_}" for col, type_ in final_ordered_schema.items())
                order_by_clause = f"ORDER BY ({', '.join(f'`{c}`' for c in order_by_cols)})" # Quote cols
                primary_key_clause = f"PRIMARY KEY ({', '.join(f'`{c}`' for c in primary_key_cols)})" if primary_key_cols else "" # Quote cols

                query = f"CREATE TABLE IF NOT EXISTS `{self.database}`.`{table_name}` ({columns_def}) {engine_clause} {primary_key_clause} {order_by_clause} SETTINGS index_granularity = 8192"
                if table_name == config.TABLE_STOCK_BARS:
                     query += ", min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, parts_to_delay_insert = 0, max_parts_in_total = 100000"
                self.client.command(query)
                print(f"Created table {table_name}")
            else:
                # print(f"Table {table_name} already exists") # Less verbose
                try: # Add uni_id if missing
                    self.client.command(f"SELECT uni_id FROM `{self.database}`.`{table_name}` WHERE 1=0")
                except Exception as e:
                    if "Missing columns" in str(e) or "Unknown identifier" in str(e):
                        print(f"Adding uni_id column to {table_name}...")
                        ts_col_hash = 'timestamp' # Default
                        if table_name in [config.TABLE_STOCK_TRADES, config.TABLE_STOCK_QUOTES]: ts_col_hash = 'sip_timestamp'
                        if table_name == config.TABLE_STOCK_NEWS: hash_expr = "cityHash64(toString(id))"
                        elif table_name == config.TABLE_STOCK_INDICATORS: hash_expr = "cityHash64(ticker, toUnixTimestamp64Nano(timestamp), indicator_type)"
                        else: hash_expr = f"cityHash64(ticker, toUnixTimestamp64Nano(`{ts_col_hash}`))"
                        add_col_q = f"ALTER TABLE `{self.database}`.`{table_name}` ADD COLUMN IF NOT EXISTS uni_id UInt64 MATERIALIZED {hash_expr}"
                        self.client.command(add_col_q)
                        print(f"Added uni_id column to {table_name}")
                # Optimize bars settings
                if table_name == config.TABLE_STOCK_BARS:
                    opt_q = f"ALTER TABLE `{self.database}`.`{table_name}` MODIFY SETTING min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, parts_to_delay_insert = 0, max_parts_in_total = 100000"
                    try: self.client.command(opt_q)
                    except Exception: pass # Ignore if fails
        except Exception as e:
            print(f"Error creating/altering table {table_name}: {str(e)}")
            raise e


    def recreate_table(self, table_name: str, schema: Dict[str, str]) -> None:
        self.drop_table_if_exists(table_name)
        self.create_table_if_not_exists(table_name, schema)

    def close(self) -> None:
        if self.client:
            try: self.client.close(); print("Closed ClickHouse client.")
            except Exception as e: print(f"Error closing ClickHouse client: {e}")
        if self._executor:
             try: self._executor.shutdown(wait=False, cancel_futures=True); print("Shutdown DB executor.")
             except Exception as e: print(f"Error shutting down DB executor: {e}")
