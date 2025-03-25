from endpoints.db import ClickHouseDB
from endpoints.master_v2 import insert_latest_data
from datetime import datetime, timedelta
import asyncio
import pytz

async def main():
    db = ClickHouseDB()
    try:
        print("Testing insert_latest_data function...")
        # Get current datetime in Eastern Time
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        # Set from_date to one minute before now
        from_date = now - timedelta(minutes=1)
        to_date = now
        
        await insert_latest_data(db, from_date, to_date)
        print("Test completed successfully!")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(main()) 