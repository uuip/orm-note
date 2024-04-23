import asyncio

import asyncpg
import psycopg
import psycopg_pool

from conf import settings


async def asyncpg_iter():
    async with asyncpg.create_pool(settings.db_url) as pool:
        async with pool.acquire() as conn:  # type:asyncpg.Connection
            async with conn.transaction():
                async for x in conn.cursor("SELECT * FROM author", prefetch=1000):
                    ...


async def psycopg_iter():
    # cursor 指定name时，使用 server side cursor
    async with psycopg_pool.AsyncConnectionPool(settings.db_url) as pool:
        async with pool.connection() as conn:  # type: psycopg.AsyncConnection
            async with conn.cursor("iter") as cur:
                cur.itersize = 1000
                await cur.execute("SELECT * FROM author")
                async for x in cur:
                    ...


def psycopg_iter2():
    with psycopg.connect(settings.db_url) as conn:  # type: psycopg.Connection
        with conn.cursor("iter") as cur:
            cur.itersize = 100
            cur.execute("SELECT * FROM author")
            return cur


if __name__ == "__main__":
    # with Session() as s:
    #     iter()
    asyncio.run(asyncpg_iter())
