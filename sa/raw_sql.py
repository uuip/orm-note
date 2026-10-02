from sqlalchemy import create_engine, text


# 推荐用法（普通原生 SQL）：execute(text(...))，使用 SQLAlchemy 的参数绑定与结果接口。
def by_text(engine):
    with engine.connect() as conn:  # sqlalchemy.engine.base.Connection
        rows = conn.execute(text("select now() as now")).mappings().all()
        print(rows)


# 条件用法：需要直接使用驱动 SQL 时用 exec_driver_sql，参数占位符须遵循对应 DBAPI。
def by_exec_driver_sql(engine):
    with engine.connect() as conn:  # sqlalchemy.engine.base.Connection
        rows = conn.exec_driver_sql("select now() as now").fetchall()
        print(rows)


# 条件用法：只有需要驱动专有游标接口时才直接使用 DBAPI，并自行管理事务。
def by_raw_connection_cursor(engine):
    conn = engine.raw_connection()  # sqlalchemy.pool.base._ConnectionFairy
    # PostgreSQL + psycopg: conn.dbapi_connection / conn.driver_connection is psycopg.Connection.
    try:
        with conn.cursor() as cursor:
            cursor.execute("select now()")
            print(cursor.fetchall())
    finally:
        conn.close()


# 备选用法：已有 SQLAlchemy 连接时取得其 DBAPI 游标；事务仍由外层连接管理。
def by_connection_cursor(engine):
    with engine.connect() as conn:  # sqlalchemy.engine.base.Connection
        # conn.connection is sqlalchemy.pool.base._ConnectionFairy; its DBAPI connection is psycopg.Connection.
        with conn.connection.cursor() as cursor:
            cursor.execute("select now()")
            print(cursor.fetchall())


# 推荐用法（事务内执行原生 SQL）：engine.begin() 正常退出时提交，异常时回滚。
def by_transaction(engine):
    with engine.begin() as conn:  # sqlalchemy.engine.base.Connection
        conn.execute(text("select now()"))


if __name__ == "__main__":
    from config import settings

    engine = create_engine(settings.db_url, echo=False)
    try:
        by_text(engine)
        by_exec_driver_sql(engine)
        by_raw_connection_cursor(engine)
        by_connection_cursor(engine)
        by_transaction(engine)
    finally:
        engine.dispose()
