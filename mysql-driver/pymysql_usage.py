import pymysql
from pymysql.connections import Connection
from pymysql.cursors import Cursor

from config import settings

stmt = "SELECT version();"


# psycopg和aiomysql都使用%s为占位符, 而asyncpg使用$1


def t_mysqldb():
    with pymysql.connect(**settings.db_dict,
                         init_command="SET SESSION time_zone = 'Asia/Shanghai'") as conn:  # type:  Connection
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:  # type: Cursor
            cursor.execute(stmt)
            for x in cursor:
                print(x)


def t_mysql_pool():
    from dbutils.pooled_db import PooledDB

    pool = PooledDB(pymysql, **settings.db_dict, maxconnections=5)
    with pool.connection() as conn:  # type:  Connection
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:  # type: Cursor
            cursor.execute(stmt)
            for x in cursor:
                print(x)


def t_mysqldb_commit():
    with pymysql.connect(**settings.db_dict) as conn:  # type:  Connection
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:  # type: Cursor
            cursor.execute("INSERT INTO auth_group (name) VALUES ('aaabbb')")
        conn.commit()
