import time
from urllib.parse import quote, quote_plus

import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

from conf import settings

# psycopg和aiomysql都使用%s为占位符, 而asyncpg使用$1

options = "-c search_path=another -c timezone=America/Curacao"
config = {
        "host"    : "127.0.0.1",
        "port"    : 5432,
        "dbname"  : "fastapi-demo",
        "user"    : "postgres",
        "password": "postgres",
        # "options": options,
        }
options = f"?options={quote_plus('-c timezone=America/Curacao')}"


class User(BaseModel):
    id: int
    name: str
    org: str


def row_factory():
    with psycopg2.connect(settings.db_url + options) as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            ...


if __name__ == "__main__":
    t1 = time.perf_counter()
    print(f"{time.perf_counter() - t1:.2f}")
