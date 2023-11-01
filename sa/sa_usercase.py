import time

from sqlalchemy import *
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import *

from setting import settings


class Base(DeclarativeBase):
    pass


class EventBase(Base):
    __abstract__ = True

    id = mapped_column(BigInteger, primary_key=True)
    transactionHash = mapped_column(VARCHAR(66))
    logIndex = mapped_column(Integer)

    event = mapped_column(Text, nullable=False)
    transactionIndex = mapped_column(Integer)
    blockNumber = mapped_column(Integer)

    from_ = mapped_column(VARCHAR(42), name="from")
    to = mapped_column(VARCHAR(42))
    token_id = mapped_column(Integer, index=True)


class ShipTransfer(EventBase):
    __tablename__ = "ship_transfer"
    __table_args__ = (
        # 下面两行等价
        # UniqueConstraint("transactionHash", "logIndex", name="unique_ship_transfer"),
        Index("uniqueidx", "transactionHash", "logIndex", unique=True),
    )


class ShipTransfer2(EventBase):
    __tablename__ = "ship_transfer2"
    __table_args__ = (Index("uniqueidx2", "transactionHash", "logIndex", unique=True),)


def element_not_in_another_not_exists(model1, model2):
    # 在当前测试场景中与leftjoin相当
    return select(model1.token_id).where(~exists(select(model2.token_id).where(model2.token_id == model1.token_id)))


def element_not_in_another_leftjoin(model1, model2):
    # join字段与查询字段都有索引
    return (
        select(model1.token_id)
        .join(model2, model1.token_id == model2.token_id, isouter=True)
        .where(model2.token_id.is_(None))
    )


def element_not_in_another_except(model1, model2):
    # 返回去重的token_id，若返回其他字段需要where token_id in ...
    return select(model1.token_id).except_(select(model2.token_id))


def element_not_in_another_not_in(model1, model2):
    # 性能最差
    subq = select(model2.token_id).scalar_subquery()
    return select(model1).where(model1.token_id.not_in(subq))


def get_first_row_every_group_distinct_on():
    # 获取分组第一条：distinct on,性能好
    t = aliased(ShipTransfer)
    owner_case = case(
        (t.to.in_(["aa", "bb"]) & t.from_.not_in(["cc", "dd"]), t.from_),
        else_=t.to,
    ).label("owner")
    owner_table = (
        select(t.token_id, owner_case).distinct(t.token_id).order_by(t.token_id, desc(t.blockNumber), desc(t.logIndex))
    )
    return owner_table


def get_first_row_every_group_window_func():
    # 获取分组第一条：窗口函数
    t = aliased(ShipTransfer)
    win = select(
        t.token_id,
        t.from_,
        t.to,
        func.row_number()
        .over(partition_by=t.token_id, order_by=[desc(t.blockNumber), desc(t.logIndex)])
        .label("new_index"),
    ).cte("win")
    owner_case = case(
        (win.c.to.in_(["aa", "bb"]) & win.c.from_.not_in(["cc", "dd"]), win.c.from_),
        else_=win.c.to,
    ).label("owner")
    owner_table = select(win.c.token_id, owner_case).where(win.c.new_index == 1)
    return owner_table


async def async_s():
    # async session的定义与iter
    async_db = create_async_engine(settings.db, echo=False, pool_size=5)
    Session = async_sessionmaker(bind=async_db)
    async with Session.begin() as s:
        # s.run_sync(Base.metadata.create_all)
        # async iter
        async for x in await s.stream_scalars(select(ShipTransfer).execution_options(yield_per=500)):
            ...


if __name__ == "__main__":
    db = create_engine(settings.db, echo=False, pool_size=1, connect_args={"options": "-c TimeZone=utc"})
    # Base.metadata.drop_all(bind=db, tables=[ShipTransfer.__table__,ShipTransfer2.__table__])
    # Base.metadata.create_all(bind=db)
    Session = sessionmaker(bind=db)
    s = Session()

    t1 = time.perf_counter()
    for obj in s.scalars(select(ShipTransfer).execution_options(yield_per=500)):
        ...
    print(f"{time.perf_counter() - t1:.2f}")
