from endpoints.db import ClickHouseDB
from endpoints.master import init_master_table
import asyncio

async def main():
    db = ClickHouseDB()
    try:
        await init_master_table(db)
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(main()) 