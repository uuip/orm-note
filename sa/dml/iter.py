import asyncio

from sqlalchemy import select

from sa.model.example import Author
from sa.session import Session, async_session


async def async_s():
    async with async_session().begin() as s:
        async for x in await s.stream_scalars(select(Author).execution_options(yield_per=500)):
            print(x.id)


def iter():
    for obj in s.scalars(select(Author).execution_options(yield_per=500)):
        print(obj.id)


if __name__ == "__main__":
    with Session() as s:
        iter()
    asyncio.run(async_s())
