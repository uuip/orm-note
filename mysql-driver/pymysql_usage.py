from pymysql.connections import Connection
from pymysql.cursors import Cursor
import pymysql
from conf import settings

stmt = "SELECT version();"


def t_mysqldb():

    with pymysql.connect(**settings.db_dict) as conn:  # type:  Connection
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
