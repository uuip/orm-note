from sqlalchemy import create_engine, text

from config import settings

engine = create_engine(settings.db_url, echo=False)


def by_text():
    with engine.connect() as conn:
        rows = conn.execute(text("select now() as now")).mappings().all()
        print(rows)


def by_exec_driver_sql():
    with engine.connect() as conn:
        rows = conn.exec_driver_sql("select now() as now").fetchall()
        print(rows)


def by_raw_connection_cursor():
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("select now()")
            print(cursor.fetchall())
    finally:
        conn.close()


def by_connection_cursor():
    with engine.connect() as conn:
        with conn.connection.cursor() as cursor:
            cursor.execute("select now()")
            print(cursor.fetchall())


def by_transaction():
    with engine.begin() as conn:
        conn.execute(text("select now()"))


if __name__ == "__main__":
    by_text()
    by_exec_driver_sql()
    by_raw_connection_cursor()
    by_connection_cursor()
    by_transaction()
