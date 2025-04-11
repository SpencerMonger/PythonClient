import os
import json
import pickle
from datetime import datetime, timezone
import pandas as pd
from typing import Dict, Any, List
import numpy as np

from endpoints.db import ClickHouseDB
from endpoints import config
from endpoints.main_run import tickers  # Import tickers list from main_run

class ModelPredictor:
    def __init__(self):
        # Initialize paths
        self.base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.models_path = os.path.join(self.base_path, "saved_models")
        
        # Load model and metadata
        print("\nLoading model and metadata...")
        self.model = self._load_model()
        self.metadata = self._load_metadata()
        self.feature_columns = self._load_feature_columns()
        
        # Initialize database connection
        self.db = ClickHouseDB()
        self._init_predictions_table()
        
    def _load_model(self):
        """Load the saved Random Forest model"""
        model_path = os.path.join(self.models_path, "random_forest_stock_prediction_model.pkl")
        print(f"Loading model from: {model_path}")
        with open(model_path, 'rb') as f:
            return pickle.load(f)
            
    def _load_metadata(self):
        """Load model metadata"""
        metadata_path = os.path.join(self.models_path, "random_forest_metadata.json")
        print(f"Loading metadata from: {metadata_path}")
        with open(metadata_path, 'r') as f:
            return json.load(f)
            
    def _load_feature_columns(self):
        """Load feature columns used by the model"""
        columns_path = os.path.join(self.models_path, "feature_columns.pkl")
        print(f"Loading feature columns from: {columns_path}")
        with open(columns_path, 'rb') as f:
            return pickle.load(f)
            
    def _init_predictions_table(self):
        """Initialize the predictions table in ClickHouse"""
        schema = {
            'timestamp': 'DateTime',
            'ticker': 'String',
            'predicted_value': 'Float64',
            'actual_close': 'Float64',
            'prediction_time': 'DateTime',
            'uni_id': 'UInt64'
        }
        
        print("\nInitializing predictions table...")
        # Only create the table if it doesn't exist
        self.db.create_table_if_not_exists(config.TABLE_STOCK_PREDICTIONS, schema)
        
    async def get_latest_normalized_data(self) -> pd.DataFrame:
        """Fetch the latest rows for each ticker from the normalized table (expects UTC timestamps)"""
        print(f"\nFetching latest normalized data for {len(tickers)} tickers...")

        # Query to get the latest data for each ticker based on MAX UTC timestamp
        query = f"""
        WITH latest_timestamps AS (
            SELECT ticker, MAX(timestamp) as max_timestamp
            FROM `{config.CLICKHOUSE_DATABASE}`.`{config.TABLE_STOCK_NORMALIZED}`
            WHERE ticker IN ({', '.join([f"'{ticker}'" for ticker in tickers])})
              AND timestamp >= (now('UTC') - INTERVAL 2 MINUTE) -- Add time filter for efficiency
            GROUP BY ticker
        )
        SELECT n.*
        FROM `{config.CLICKHOUSE_DATABASE}`.`{config.TABLE_STOCK_NORMALIZED}` n
        INNER JOIN latest_timestamps l
        ON n.ticker = l.ticker AND n.timestamp = l.max_timestamp
        ORDER BY n.timestamp DESC
        """

        try:
            result = self.db.client.query(query)
            if result.row_count == 0:
                print("No recent data found in normalized table")
                return None # Changed from returning pd.DataFrame()

            df = pd.DataFrame(result.result_rows, columns=result.column_names)
            print(f"Retrieved data for {len(df)} tickers")
            print(f"Tickers found: {', '.join(df['ticker'].unique())}")

            # Convert timestamp column to timezone-aware UTC pandas datetime
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')

            latest_timestamp_utc = df['timestamp'].max()
            now_utc = datetime.now(timezone.utc)
            print(f"Latest timestamp (UTC): {latest_timestamp_utc}")
            print(f"Current time (UTC):   {now_utc}")
            print(f"Data age: {now_utc - latest_timestamp_utc}")
            return df

        except Exception as e:
            print(f"Error fetching normalized data: {str(e)}")
            return None # Changed from returning pd.DataFrame()
            
    def make_prediction(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Make prediction using the loaded model"""
        if data is None or len(data) == 0:
            return None
            
        print(f"\nPreparing data for prediction for ticker: {data['ticker'].iloc[0]}")
        # Ensure data has all required features
        missing_features = set(self.feature_columns) - set(data.columns)
        if missing_features:
            print(f"Warning: Missing features: {missing_features}")
            return None
            
        # Clean and convert data types
        print("Converting data types...")
        data_clean = data.copy()
        for col in data_clean.columns:
            if col not in ['ticker', 'timestamp']:  # Skip non-numeric columns
                try:
                    # Standard conversion for all numeric columns
                    data_clean[col] = pd.to_numeric(data_clean[col], errors='coerce')
                except Exception as e:
                    print(f"Warning: Could not convert column {col}: {str(e)}")
        
        # Select only the features used by the model
        X = data_clean[self.feature_columns]
        
        # Check for any remaining NaN values and also cap extremely large values
        # that could cause overflow or infinity errors
        for col in X.columns:
            # Replace NaN values with 2.5
            if X[col].isna().any():
                print(f"Warning: NaN values found in column {col}, replacing with 2.5")
                X[col] = X[col].fillna(2.5)
                
            # Use general limits for all columns (removed specific trade_conditions capping)
            max_val = 1e6  # Cap at a million
            min_val = -1e6 # Cap at negative million
            if (X[col] > max_val).any() or (X[col] < min_val).any():
                print(f"Warning: Extreme values found in column {col}, capping to range [{min_val}, {max_val}]")
                X[col] = X[col].clip(min_val, max_val)
                
        # Check for any remaining NaN or infinity values
        if not X.isna().any().any() and np.isfinite(X).all().all():
            print("All values are finite and non-NaN after cleaning")
        else:
            print("Warning: Data still contains NaN or infinite values after cleaning")
            cols_with_issues = X.columns[(X.isna().any()) | (~np.isfinite(X).all())]
            print(f"Columns with issues: {cols_with_issues}")
            # More aggressive cleaning - replace all problematic values with 2.5
            X = X.fillna(2.5).replace([np.inf, -np.inf], 2.5)
        
        print("Making prediction...")
        try:
            prediction = self.model.predict(X)[0]
            
            result = {
                'timestamp': data['timestamp'].iloc[0],
                'ticker': data['ticker'].iloc[0],
                'predicted_value': float(prediction),
                'actual_close': float(data_clean['close'].iloc[0]),
                'prediction_time': datetime.now()
            }
            
            print(f"Prediction for {result['ticker']}: {prediction:.4f}")
            return result
            
        except Exception as e:
            print(f"Error making prediction: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None
            
    async def store_prediction(self, prediction: Dict[str, Any]) -> None:
        """Store the prediction in ClickHouse, ensuring UTC timestamps are used for uni_id"""
        if prediction is None:
            return

        print(f"\nStoring prediction for {prediction['ticker']} in database...")
        try:
            # Ensure timestamps are aware UTC datetimes
            timestamp_dt_utc = self.db._ensure_utc(prediction['timestamp'])
            prediction_time_dt_utc = self.db._ensure_utc(prediction['prediction_time'])

            if timestamp_dt_utc is None or prediction_time_dt_utc is None:
                 print(f"Error: Could not ensure UTC for prediction timestamps for {prediction['ticker']}")
                 return

            # Use nanosecond epoch for hashing consistency
            timestamp_ns_epoch = int(timestamp_dt_utc.timestamp() * 1e9)
            prediction_time_ns = int(prediction_time_dt_utc.timestamp() * 1e9)

            # Add uni_id using the ClickHouseDB's consistent hash method
            prediction['uni_id'] = self.db._generate_consistent_hash(
                prediction['ticker'],
                timestamp_ns_epoch, # Use ns epoch
                prediction_time_ns, # Use ns epoch
                str(prediction['predicted_value']) # Ensure value is string for hash
            )

            # Ensure the datetime objects are passed for insertion
            prediction['timestamp'] = timestamp_dt_utc
            prediction['prediction_time'] = prediction_time_dt_utc


            await self.db.insert_data(config.TABLE_STOCK_PREDICTIONS, [prediction])
            print(f"Successfully stored prediction for {prediction['ticker']}")

        except Exception as e:
            print(f"Error storing prediction: {str(e)}")
            import traceback
            print(traceback.format_exc())
            
    async def process_latest_data(self) -> None:
        """Main function to process latest data and make predictions for all tickers"""
        print("\n=== Starting prediction process ===")

        # Get latest normalized data for all tickers
        df = await self.get_latest_normalized_data()
        if df is None or len(df) == 0:
            print("No recent normalized data to process for predictions") # Adjusted message
            return

        # Process each ticker's data
        predictions_made = 0
        for ticker in tickers:
            # Filter data for current ticker
            ticker_data = df[df['ticker'] == ticker]

            if len(ticker_data) == 0:
                # print(f"No data found for ticker {ticker}") # Less verbose
                continue

            print(f"\nProcessing prediction for {ticker}...")

            # Make prediction
            prediction_result = self.make_prediction(ticker_data) # Renamed variable
            if prediction_result is None:
                print(f"Could not make prediction for {ticker}")
                continue

            # Store prediction
            await self.store_prediction(prediction_result) # Use renamed variable
            predictions_made += 1

        print(f"\n=== Prediction process completed - {predictions_made}/{len(tickers)} predictions made ===")
        
    def close(self):
        """Close database connection"""
        self.db.close()

async def run_model_feed():
    """Main function to run the model feed process"""
    predictor = ModelPredictor()
    try:
        await predictor.process_latest_data()
    finally:
        predictor.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_model_feed()) 