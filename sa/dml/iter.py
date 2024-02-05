from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session


async def async_s():
    s: AsyncSession
    async with s.begin() as s:
        async for x in await s.stream_scalars(select(...).execution_options(yield_per=500)):
            ...


def iter():
    s: Session
    for obj in s.scalars(select(...).execution_options(yield_per=500)):
        ...
