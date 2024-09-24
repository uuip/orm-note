from sqlalchemy import *
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import *

from conf import settings


# os.environ["PGTZ"] = "utc"
# psycopg: connect_args={"options": "-c TimeZone=Asia/Tokyo"}
# url = URL.create()
# default pool_size=5
engine = create_engine(settings.db, echo=False, pool_size=10)
SessionMaker = sessionmaker(bind=engine)


def async_session():
    async_engine = create_async_engine(settings.db_asyncpg, echo=False, pool_size=10)
    AsyncSessionMaker = async_sessionmaker(bind=async_engine)
    return AsyncSessionMaker


def usage_a():
    with SessionMaker() as session:  # type: Session
        session.execute(...)
        session.commit()


def usage_b():
    # inner context calls session.commit(), if there were no exceptions
    # outer context calls session.close()
    with SessionMaker() as session:
        with session.begin():
            session.execute(...)


def usage_c():
    # include begin()/commit()/rollback()
    # commits the transaction, closes the session
    with SessionMaker.begin() as session:
        session.execute(...)
