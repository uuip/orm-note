import asyncio

from sqlalchemy import *
from sqlalchemy.orm import DeclarativeBase

from sa.session import SessionMaker, AsyncSessionMaker


class Base(DeclarativeBase):
    pass


class Transactions(Base):
    __tablename__ = "transactions_20230916"

    tag_id = Column(Text, primary_key=True)
    tx_hash = Column(Text)


statement = select(Transactions).limit(5000).execution_options(yield_per=500)


async def async_iter():
    async with AsyncSessionMaker() as s:
        async for _ in await s.stream_scalars(statement):
            ...


def iter():
    with SessionMaker() as s:
        for _ in s.scalars(statement):
            ...


if __name__ == "__main__":
    iter()
    asyncio.run(async_iter())
