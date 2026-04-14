import mysql.connector
from mysql.connector import CMySQLConnection
from mysql.connector.cursor_cext import CMySQLCursorDict
from conf import settings

stmt = "SELECT version();"
# psycopg和aiomysql都使用%s为占位符, 而asyncpg使用$1

def t_connector():
    with mysql.connector.connect(**settings.db_dict,time_zone=None) as conn:  # type: CMySQLConnection
        with conn.cursor(dictionary=True) as cursor:  # type: CMySQLCursorDict
            cursor.execute(stmt)
            for x in cursor:
                print(x)
        conn.commit()


def t_connector_pool():
    pool = mysql.connector.pooling.MySQLConnectionPool(**settings.db_dict)
    with pool.get_connection() as conn:  # type: CMySQLConnection
        with conn.cursor(dictionary=True) as cursor:  # type: CMySQLCursorDict
            cursor.execute(stmt)
            for x in cursor:
                print(x)
