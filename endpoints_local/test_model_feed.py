import asyncio
import os
import sys
import pandas as pd
from datetime import datetime

# Add parent directory to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import sklearn
except ImportError:
    print("\nError: scikit-learn is not installed.")
    print("Please install it using: pip install scikit-learn pandas numpy")
    sys.exit(1)

from endpoints.model_feed import ModelPredictor
from endpoints.db import ClickHouseDB
from endpoints import config

async def test_model_feed():
    """Test the model feed functionality with sample data"""
    print("\n=== Starting Model Feed Test ===")
    
    # First, verify we can load the model
    print("\nStep 1: Testing model loading...")
    try:
        predictor = ModelPredictor()
        print("✓ Successfully loaded model and initialized predictor")
    except Exception as e:
        print(f"✗ Failed to load model: {str(e)}")
        return
    
    # Test database connection
    print("\nStep 2: Testing database connection...")
    try:
        db = ClickHouseDB()
        db.client.command('SELECT 1')
        print("✓ Successfully connected to database")
        db.close()
    except Exception as e:
        print(f"✗ Failed to connect to database: {str(e)}")
        return
    
    # Test getting latest normalized data
    print("\nStep 3: Testing data retrieval...")
    try:
        data = await predictor.get_latest_normalized_data()
        if data is not None:
            print(f"✓ Successfully retrieved data with shape: {data.shape}")
            print(f"Available columns: {', '.join(data.columns)}")
            
            # Print sample of data types
            print("\nData types of columns:")
            for col in data.columns:
                print(f"  {col}: {data[col].dtype}")
        else:
            print("✗ No data found in normalized table")
    except Exception as e:
        print(f"✗ Failed to retrieve data: {str(e)}")
        return
    
    # Test making prediction
    if data is not None:
        print("\nStep 4: Testing prediction...")
        try:
            # Clean the data before prediction
            for col in data.select_dtypes(include=['object']).columns:
                if col not in ['ticker', 'timestamp']:  # Skip non-numeric columns
                    data[col] = pd.to_numeric(data[col].replace('', '0'), errors='coerce')
            
            prediction = predictor.make_prediction(data)
            if prediction:
                print("✓ Successfully made prediction:")
                for key, value in prediction.items():
                    print(f"  {key}: {value}")
            else:
                print("✗ Failed to make prediction")
        except Exception as e:
            print(f"✗ Error making prediction: {str(e)}")
            import traceback
            print(traceback.format_exc())
    
    # Test storing prediction
    if data is not None and prediction:
        print("\nStep 5: Testing prediction storage...")
        try:
            await predictor.store_prediction(prediction)
            print("✓ Successfully stored prediction")
        except Exception as e:
            print(f"✗ Failed to store prediction: {str(e)}")
    
    predictor.close()
    print("\n=== Model Feed Test Complete ===")

if __name__ == "__main__":
    asyncio.run(test_model_feed()) 