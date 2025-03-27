import mysql.connector
from mysql.connector import CMySQLConnection
from mysql.connector.cursor_cext import CMySQLCursorDict
from conf import settings

stmt = "SELECT version();"

print("%s")
def t_connector():
    with mysql.connector.connect(**settings.db_dict) as conn:  # type: CMySQLConnection
        with conn.cursor(dictionary=True) as cursor:  # type: CMySQLCursorDict
            cursor.execute(stmt)
            for x in cursor:
                print(x)
        conn.commit()


def t_connector_pool():
    pool = mysql.connector.pooling.MySQLConnectionPool(**settings.db_dict)
    with pool.get_connection() as conn:  # type: CMySQLConnection
        ...
