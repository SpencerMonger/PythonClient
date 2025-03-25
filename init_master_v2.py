from endpoints.db import ClickHouseDB
from endpoints.master_v2 import init_master_v2
import asyncio

async def main():
    db = ClickHouseDB()
    try:
        await init_master_v2(db)
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(main()) 