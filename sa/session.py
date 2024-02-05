from sqlalchemy import *
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import *

from sa.model.usercase import Base
from setting import settings

# from sqlalchemy.dialects import postgresql as pg
# pg.dialect = pg.base.dialect = pg.psycopg.dialect

# os.environ["PGTZ"] = "utc"
#  -c statement_timeout=300
# url = URL.create()
db = create_engine(settings.db, echo=True, pool_size=5, connect_args={"options": "-c TimeZone=Asia/Tokyo"})
Session = sessionmaker(bind=db)
s = Session()

Base.metadata.drop_all(bind=db, tables=[])
Base.metadata.create_all(bind=db)


async def asession():
    async_db = create_async_engine(settings.db, echo=False, pool_size=5)
    Session = async_sessionmaker(bind=async_db)

    async with Session.begin() as s:
        await s.run_sync(Base.metadata.create_all)


def a():
    with Session() as session:
        session.execute(...)
        session.commit()


def b():
    # inner context calls session.commit(), if there were no exceptions
    # outer context calls session.close()
    with Session() as session:
        with session.begin():
            session.execute(...)


def c():
    # include begin()/commit()/rollback()
    # commits the transaction, closes the session
    with Session.begin() as session:
        session.execute(...)
