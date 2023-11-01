import asyncio
import time
from datetime import datetime, UTC

import asyncpg
from asyncpg import Connection

from setting import settings


async def test_dml():
    # Connection 不能with
    conn: Connection = await asyncpg.connect(settings.db_standard)
    await conn.execute(
        "INSERT INTO public.user (name,org,books,nickname) VALUES ($1,$2,$3,$4)", *["a", "org_a", [33], "xyz"]
    )
    await conn.close()


def timestamptz_endocder(v):
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=UTC).isoformat()
    if isinstance(v, datetime):
        return v.astimezone(UTC).isoformat()
    if isinstance(v, str):
        return datetime.fromisoformat(v).astimezone(UTC).isoformat()
    raise ValueError


async def transform(conn: asyncpg.Connection):
    await conn.set_type_codec(
        schema="pg_catalog", typename="timestamptz", encoder=timestamptz_endocder, decoder=lambda x: x
    )


async def type_codec():
    async with asyncpg.create_pool(settings.db_standard, min_size=1, command_timeout=60, init=transform) as pool:
        ...


async def asyncpg_iter():
    async with asyncpg.create_pool(settings.db_standard) as pool:
        async with pool.acquire() as conn:  # type:asyncpg.Connection
            async with conn.transaction():
                async for u in conn.cursor("SELECT * FROM ship_transfer", prefetch=500):
                    yield u


async def main():
    async for x in asyncpg_iter():
        pass


if __name__ == "__main__":
    t1 = time.perf_counter()
    asyncio.run(main())
    print(f"{time.perf_counter() - t1:.2f}")
