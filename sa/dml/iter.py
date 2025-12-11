import asyncio

from sqlalchemy import Column, Text, select
from sqlalchemy.orm import DeclarativeBase

from sa.session import SessionMaker, AsyncSessionMaker


class Base(DeclarativeBase):
    pass


class Transactions(Base):
    __tablename__ = "transactions_20230916"

    tag_id = Column(Text, primary_key=True)
    tx_hash = Column(Text)


# 这个过程不能commit
stmt = select(Transactions).execution_options(yield_per=500)


def iter():
    with SessionMaker() as s:
        for _ in s.scalars(stmt):
            ...
        for p in s.scalars(stmt).partitions():
            for wf in p:
                ...


async def async_iter():
    async with AsyncSessionMaker() as s:
        async for _ in await s.stream_scalars(stmt):
            ...


if __name__ == "__main__":
    iter()
    asyncio.run(async_iter())
