import time
from urllib.parse import quote

import psycopg
import psycopg2
import psycopg_pool
from psycopg.rows import class_row, dict_row
from psycopg.sql import SQL, Identifier  # noqa
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

from conf import settings

options = "-c search_path=another -c timezone=America/Curacao"
config = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "fastapi-demo",
    "user": "postgres",
    "password": "postgres",
    # "options": options,
}
options = f"?options={quote(options)}"


class User(BaseModel):
    id: int
    name: str
    org: str


def main():
    # cursor 指定name时，使用 server side cursor

    with psycopg.connect(settings.db_url) as conn:
        cur = conn.execute("SELECT * FROM author LIMIT 1")
        rst = cur.fetchone()
        print(rst)
    with psycopg_pool.ConnectionPool(settings.db_url) as pool:
        with pool.connection() as conn:  # type: psycopg.Connection
            with conn.cursor("iter") as cur:
                ...


def t_commit():
    # autocommit为False, 但在with结束时会commit; 不使用with时需显示commit
    with psycopg.connect(settings.db_url) as conn:  # type: psycopg.Connection
        conn.execute('INSERT INTO "user" ("name") VALUES (%s)', ["a"])


def row_factory():
    with psycopg.connect(**config) as conn:
        with conn.cursor("iter", row_factory=class_row(User)) as cur:
            pass
    with psycopg.connect(**config, row_factory=dict_row) as conn:
        ...
    with psycopg2.connect(settings.db_url + options) as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            ...


async def async_main():
    async with await psycopg.AsyncConnection.connect(settings.db_url) as conn:  # type: psycopg.AsyncConnection
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM author LIMIT 2")
            rst = await cur.fetchall()
            print(rst)
    async with psycopg_pool.AsyncConnectionPool(settings.db_url) as pool:
        async with pool.connection() as conn:  # type: psycopg.AsyncConnection
            ...


if __name__ == "__main__":
    t1 = time.perf_counter()
    t_commit()
    print(f"{time.perf_counter() - t1:.2f}")
