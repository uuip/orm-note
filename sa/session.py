from sqlalchemy import *
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import *

from conf import settings

# 修改默认方言为psycopg
# from sqlalchemy.dialects import postgresql as pg
# pg.dialect = pg.base.dialect = pg.psycopg.dialect

# os.environ["PGTZ"] = "utc"
# psycopg: connect_args={"options": "-c TimeZone=Asia/Tokyo"}
# url = URL.create()
# default pool_size=5
db = create_engine(settings.db, echo=False, pool_size=10)
Session = sessionmaker(bind=db)


def async_session():
    async_db = create_async_engine(settings.db_asyncpg, echo=False, pool_size=10)
    async_session = async_sessionmaker(bind=async_db)
    return async_session


def usage_a():
    with Session() as session:
        session.execute(...)
        session.commit()


def usage_b():
    # inner context calls session.commit(), if there were no exceptions
    # outer context calls session.close()
    with Session() as session:
        with session.begin():
            session.execute(...)


def usage_c():
    # include begin()/commit()/rollback()
    # commits the transaction, closes the session
    with Session.begin() as session:
        session.execute(...)
