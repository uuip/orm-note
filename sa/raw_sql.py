from sa.session import engine

engine.connect().exec_driver_sql()
# engine.connect()._cursor_execute()
engine.raw_connection().cursor() # psycopg.Cursor
engine.connect().connection.cursor() #psycopg.Cursor