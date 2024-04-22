from peewee import *

# sqlite_db = SqliteDatabase('/path/to/app.db')

db = PostgresqlDatabase(
    host="localhost",
    port="5432",
    database="prac",
    user="postgres",
    password="postgres",
)
