from endpoints.db import ClickHouseDB
from endpoints.main import init_master_only
from datetime import datetime, timedelta
import asyncio
import pytz

async def main():
    db = ClickHouseDB()
    try:
        print("Testing live data workflow...")
        # Get current datetime in Eastern Time
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        # Set from_date to one minute before now
        from_date = now - timedelta(minutes=1)
        to_date = now
        
        # Simulate the live mode workflow
        await init_master_only(db, from_date, to_date, store_latest_only=True)
        print("Live workflow test completed successfully!")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(main()) 