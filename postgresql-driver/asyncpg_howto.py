import asyncio
import time

import asyncpg
from asyncpg import Connection

from conf import settings


async def main():
    # Connection 不能with, autocommit为 True
    conn: Connection = await asyncpg.connect(settings.db_url)
    await conn.execute(
            "INSERT INTO public.author (name,org,books,nickname) VALUES ($1,$2,$3,$4)", *["a", "org_a", [33], "xyz"]
            )
    await conn.close()


async def conn_pool():
    async with asyncpg.create_pool(settings.db_url) as pool:
        print(await pool.fetch("SELECT * FROM public.author"))


async def conn_pool2():
    async with asyncpg.create_pool(settings.db_url) as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                print(await pool.fetch("SELECT * FROM public.user"))


async def t_commit():
    # 即使需显式事务，也不用手动commit
    conn: Connection = await asyncpg.connect(settings.db_url)
    async with conn.transaction():
        await conn.execute('INSERT INTO "user" ("name") VALUES ($1)', "a")
        await conn.execute('INSERT INTO "user" ("name") VALUES ($1)', "b")
    await conn.close()


if __name__ == "__main__":
    t1 = time.perf_counter()
    asyncio.run(t_commit())
    print(f"{time.perf_counter() - t1:.2f}")
