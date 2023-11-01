import asyncio
import platform
import time

import psycopg
import psycopg_pool
from psycopg.rows import class_row
from psycopg.sql import SQL, Identifier  # noqa
from pydantic import BaseModel
from sqlalchemy import Engine

from setting import settings


class User(BaseModel):
    id: int
    address: str
    private_key: str


def demo():
    with psycopg.connect(settings.db_standard, options="-c timezone=America/Curacao") as conn:
        conn.execute("SELECT * FROM transactions_pool")
        cur = conn.execute("SELECT * FROM pg_tables WHERE schemaname='public'")
        for x in cur:
            print(x)


async def psycopg_iter():
    # cursor 指定name时，使用 server side cursor
    async with psycopg_pool.AsyncConnectionPool(settings.db_standard) as pool:
        async with pool.connection() as conn:  # type: psycopg.AsyncConnection
            async with conn.cursor("iter") as cur:
                cur.itersize = 500
                await cur.execute("SELECT * FROM ship_transfer")
                async for x in cur:
                    yield x


def psycopg_row_factory():
    with psycopg_pool.ConnectionPool(settings.db_standard) as pool:
        with pool.connection() as conn:  # type: psycopg.Connection
            with conn.cursor("iter", row_factory=class_row(User)) as cur:
                for x in cur.execute("SELECT * FROM userinfo"):
                    print(x)


def copy_from_psycopg2(db: Engine):
    assert db.driver == "psycopg2"
    with db.raw_connection().driver_connection as conn:
        with conn.cursor() as cursor:
            cursor.copy_from(open(r"data.dump", "rb"), "table")
            conn.commit()


def copy_to_psycopg2(db: Engine):
    assert db.driver == "psycopg2"
    with db.raw_connection().driver_connection as conn:
        with conn.cursor() as cursor:
            cursor.copy_to(open(r"data.dump", "wb+"), "table")
            conn.commit()


def copy_from_psycopg():
    src_file = "/Users/sharp/Downloads/GeoLite2-City-Blocks-IPv4.csv"
    with psycopg.connect(settings.db_standard) as conn, open(src_file, "rb") as f:
        with conn.cursor().copy("COPY geoip2_network FROM STDIN with (format csv, header)") as copy:
            while data := f.read(20 * 1024**2):
                copy.write(data)


def copy_to_psycopg():
    dst_file = "data.dump"
    with psycopg.connect(settings.db_standard) as conn, open(dst_file, "wb+") as f:
        with conn.cursor().copy("COPY geoip2_network TO STDOUT") as copy:
            for data in copy:
                f.write(data)


async def main():
    async for x in psycopg_iter():
        pass


if __name__ == "__main__":
    if platform.system() == "Windows":
        from asyncio import WindowsSelectorEventLoopPolicy

        asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    t1 = time.perf_counter()
    asyncio.run(main())
    print(f"{time.perf_counter() - t1:.2f}")
