import os
import pickle
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any

# Assuming db_utils is in the same directory
from db_utils import ClickHouseClient

# --- Configuration ---
# Database table names
SOURCE_TABLE = "stock_normalized" # Table with features for prediction
TARGET_TABLE = "stock_historical_predictions"

# Model and feature paths (relative to this script's location)
MODEL_DIR = os.path.join(os.path.dirname(__file__), "saved_models")
MODEL_FILENAME = "random_forest_stock_prediction_model.pkl" # <<< CHANGE IF YOUR MODEL NAME IS DIFFERENT
FEATURES_FILENAME = "feature_columns.pkl" # <<< CHANGE IF YOUR FEATURES FILE NAME IS DIFFERENT
MODEL_PATH = os.path.join(MODEL_DIR, MODEL_FILENAME)
FEATURES_PATH = os.path.join(MODEL_DIR, FEATURES_FILENAME)

# Data fetching chunk size
CHUNK_SIZE = 10000
# ---------------------

# Define the schema for the target table
# Using DateTime64(9, 'UTC') for high precision timestamps compatible with nanoseconds
TARGET_SCHEMA = {
    'timestamp': "DateTime64(9, 'UTC')",
    'ticker': 'String',
    'prediction_raw': 'Float64', # The direct output from the model
    'prediction_cat': 'UInt8'    # Categorized prediction (0-5)
}

# Define table engine and sorting/primary key
# ReplacingMergeTree allows updating/replacing rows with the same sorting key
TARGET_ENGINE = "ENGINE = ReplacingMergeTree()"
TARGET_ORDER_BY = "ORDER BY (ticker, timestamp)" # Order matters for ReplacingMergeTree
TARGET_PRIMARY_KEY = "PRIMARY KEY (ticker, timestamp)"

def load_model_and_features(model_path: str, features_path: str) -> tuple:
    """Loads the pickled model and feature list."""
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found at: {model_path}")
    if not os.path.exists(features_path):
        raise FileNotFoundError(f"Features file not found at: {features_path}")

    print(f"Loading model from: {model_path}")
    with open(model_path, 'rb') as f:
        model = pickle.load(f)

    print(f"Loading feature columns from: {features_path}")
    with open(features_path, 'rb') as f:
        feature_columns = pickle.load(f)
        if not isinstance(feature_columns, list):
             raise TypeError(f"Expected feature_columns.pkl to contain a list, got {type(feature_columns)}")

    print(f"Loaded model and {len(feature_columns)} feature columns.")
    return model, feature_columns

def categorize_prediction(raw_prediction: float) -> int:
    """Categorizes raw prediction score into 0-5."""
    # --- Placeholder Logic --- 
    # Adjust this based on your model's output characteristics and desired category mapping.
    # Example: Simple rounding and clipping
    category = round(raw_prediction)
    category = max(0, min(5, category)) # Ensure category is within 0-5 range
    return int(category)
    # -------------------------

def prepare_data_for_prediction(df: pd.DataFrame, feature_columns: List[str]) -> pd.DataFrame:
    """Prepares the DataFrame for prediction (subsetting, cleaning)."""
    # Ensure all required columns are present
    missing_features = set(feature_columns) - set(df.columns)
    if missing_features:
        # This should ideally not happen if the source table is correct
        raise ValueError(f"Missing required feature columns in source data: {missing_features}")

    X = df[feature_columns].copy()

    # Basic cleaning (similar to model_feed.py, adapt as needed)
    for col in X.columns:
        # Convert to numeric, coercing errors
        X[col] = pd.to_numeric(X[col], errors='coerce')

    # Handle NaNs - Using median fill as an example, could use 0, mean, or specific values
    if X.isna().any().any():
        print(f"Warning: NaNs found in prediction features. Filling with median.")
        for col in X.columns[X.isna().any()]:
            median_val = X[col].median()
            # If median is also NaN (e.g., all NaNs in column), fill with 0
            fill_value = median_val if pd.notna(median_val) else 0
            X[col] = X[col].fillna(fill_value)

    # Handle infinities (replace with large finite numbers or NaN then fill)
    X.replace([np.inf, -np.inf], np.nan, inplace=True)
    if X.isna().any().any(): # Re-check NaNs after infinity replacement
        print(f"Warning: NaNs found after replacing infinities. Filling with median.")
        for col in X.columns[X.isna().any()]:
            median_val = X[col].median()
            fill_value = median_val if pd.notna(median_val) else 0
            X[col] = X[col].fillna(fill_value)

    # Optional: Clip extreme values if necessary
    # Example: clip_threshold = 1e9
    # X = X.clip(-clip_threshold, clip_threshold)

    return X

def run_predictions():
    """Main function to fetch data, run predictions, and store results."""
    db_client = None
    try:
        # 1. Load Model and Features
        model, feature_columns = load_model_and_features(MODEL_PATH, FEATURES_PATH)

        # 2. Initialize Database Connection
        db_client = ClickHouseClient()

        # 3. Create Target Table if it doesn't exist
        db_client.create_table_if_not_exists(
            TARGET_TABLE,
            TARGET_SCHEMA,
            TARGET_ENGINE,
            TARGET_ORDER_BY,
            TARGET_PRIMARY_KEY
        )

        # 4. Fetch data from source table in chunks
        print(f"Fetching data from {SOURCE_TABLE} in chunks of {CHUNK_SIZE}...")
        total_rows_processed = 0
        offset = 0
        while True:
            query = f"""
            SELECT * FROM `{db_client.database}`.`{SOURCE_TABLE}`
            ORDER BY ticker, timestamp # Ensure consistent order for chunking
            LIMIT {CHUNK_SIZE} OFFSET {offset}
            """
            print(f"Fetching rows from offset {offset}...")
            source_df = db_client.query_dataframe(query)

            if source_df is None or source_df.empty:
                print("No more data to fetch.")
                break

            print(f"Fetched {len(source_df)} rows.")

            # Ensure 'timestamp' is in UTC datetime format
            # The db_utils._ensure_utc can handle various input types
            source_df['timestamp'] = source_df['timestamp'].apply(db_client._ensure_utc)
            # Drop rows where timestamp conversion failed
            source_df.dropna(subset=['timestamp'], inplace=True)
            if source_df.empty:
                print("Skipping chunk due to timestamp conversion issues.")
                offset += CHUNK_SIZE
                continue

            # 5. Prepare Data
            X = prepare_data_for_prediction(source_df, feature_columns)

            # 6. Make Predictions
            print(f"Making predictions for {len(X)} rows...")
            try:
                raw_predictions = model.predict(X)
            except Exception as e:
                print(f"Error during model prediction: {e}")
                # Option: Skip chunk or stop? Stopping for safety.
                raise

            # 7. Prepare Results for Insertion
            results = []
            for i, index in enumerate(X.index):
                raw_pred = float(raw_predictions[i])
                category = categorize_prediction(raw_pred)
                result_row = {
                    'timestamp': source_df.loc[index, 'timestamp'],
                    'ticker': source_df.loc[index, 'ticker'],
                    'prediction_raw': raw_pred,
                    'prediction_cat': category
                }
                results.append(result_row)

            # 8. Insert Results into Target Table
            if results:
                print(f"Inserting {len(results)} predictions into {TARGET_TABLE}...")
                db_client.insert(TARGET_TABLE, results, column_names=list(TARGET_SCHEMA.keys()))
                total_rows_processed += len(results)
            else:
                print("No results to insert for this chunk.")

            # Move to the next chunk
            offset += CHUNK_SIZE
            # Small safety break for testing, remove for full run
            # if offset >= CHUNK_SIZE * 2: # Process only 2 chunks for testing
            #     print("Stopping after 2 chunks for testing.")
            #     break

        print(f"\nPrediction process finished. Total rows processed: {total_rows_processed}")

    except FileNotFoundError as e:
        print(f"Error: {e}. Please ensure model and feature files exist at expected paths.")
    except ValueError as e:
        print(f"Configuration or data error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if db_client:
            db_client.close()

if __name__ == "__main__":
    print("=== Starting Historical Prediction Generation ===")
    run_predictions()
    print("=================================================") 