from sqlalchemy import *
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import *

from conf import settings

# os.environ["PGTZ"] = "utc"
# psycopg: connect_args={"options": "-c TimeZone=Asia/Tokyo"}

# URL.create()
# default pool_size=5
# sessionmaker: expire_on_commit=False
engine = create_engine(settings.db_url, echo=False)
SessionMaker = sessionmaker(bind=engine)

async_engine = create_async_engine(settings.db_asyncpg, echo=False)
AsyncSessionMaker = async_sessionmaker(bind=async_engine)


def usage_a():
    with SessionMaker() as session:  # type: Session
        session.execute(...)
        session.commit()
    # outer context calls session.close()


def usage_c():
    # include begin()/commit()/rollback()
    with SessionMaker.begin() as session:
        session.execute(...)
    # commits the transaction, closes the session


# 与usage_c等价
def usage_b():
    with SessionMaker() as session:
        with session.begin():
            session.execute(...)
        # inner context calls session.commit(), if there were no exceptions
    # outer context calls session.close()
