import os
import json
import pickle
from datetime import datetime
import pandas as pd
from typing import Dict, Any

from endpoints.db import ClickHouseDB
from endpoints import config

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
            'prediction_time': 'DateTime'
        }
        
        print("\nInitializing predictions table...")
        self.db.create_table_if_not_exists(config.TABLE_STOCK_PREDICTIONS, schema)
        
    async def get_latest_normalized_data(self) -> pd.DataFrame:
        """Fetch the latest row from the normalized table"""
        print("\nFetching latest normalized data...")
        query = f"""
        SELECT *
        FROM {config.CLICKHOUSE_DATABASE}.{config.TABLE_STOCK_NORMALIZED}
        ORDER BY timestamp DESC
        LIMIT 1
        """
        
        try:
            result = self.db.client.query(query)
            if result.row_count == 0:
                print("No data found in normalized table")
                return None
                
            df = pd.DataFrame(result.result_rows, columns=result.column_names)
            print(f"Retrieved data for timestamp: {df['timestamp'].iloc[0]}")
            return df
            
        except Exception as e:
            print(f"Error fetching normalized data: {str(e)}")
            return None
            
    def make_prediction(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Make prediction using the loaded model"""
        if data is None or len(data) == 0:
            return None
            
        print("\nPreparing data for prediction...")
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
                    data_clean[col] = pd.to_numeric(data_clean[col].replace('', '0'), errors='coerce')
                except Exception as e:
                    print(f"Warning: Could not convert column {col}: {str(e)}")
        
        # Select only the features used by the model
        X = data_clean[self.feature_columns]
        
        # Check for any remaining NaN values
        nan_cols = X.columns[X.isna().any()].tolist()
        if nan_cols:
            print(f"Warning: NaN values found in columns: {nan_cols}")
            # Fill NaN values with 0
            X = X.fillna(0)
        
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
            
            print(f"Prediction value: {prediction:.4f}")
            return result
            
        except Exception as e:
            print(f"Error making prediction: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None
            
    async def store_prediction(self, prediction: Dict[str, Any]) -> None:
        """Store the prediction in ClickHouse"""
        if prediction is None:
            return
            
        print("\nStoring prediction in database...")
        try:
            await self.db.insert_data(config.TABLE_STOCK_PREDICTIONS, [prediction])
            print("Successfully stored prediction")
            
        except Exception as e:
            print(f"Error storing prediction: {str(e)}")
            
    async def process_latest_data(self) -> None:
        """Main function to process latest data and make prediction"""
        print("\n=== Starting prediction process ===")
        
        # Get latest normalized data
        data = await self.get_latest_normalized_data()
        if data is None:
            print("No data to process")
            return
            
        # Make prediction
        prediction = self.make_prediction(data)
        if prediction is None:
            print("Could not make prediction")
            return
            
        # Store prediction
        await self.store_prediction(prediction)
        print("\n=== Prediction process completed ===")
        
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