# https://github.com/agronholm/sqlacodegen
# https://github.com/sqlalchemy/alembic

# https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#table-valued-functions
# https://docs.sqlalchemy.org/en/20/orm/mapped_attributes.html#synonyms
# https://docs.sqlalchemy.org/en/20/orm/nonstandard_mappings.html#mapping-a-class-against-arbitrary-subqueries

# 都有Enum
# from sqlalchemy import Enum
# from enum import Enum

import enum
from typing import Optional

from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import *
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql import operators

from fakedata import make, setup  # noqa
from setting import settings


class Status(enum.StrEnum):
    open = enum.auto()
    close = enum.auto()


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"
    id = mapped_column(BigInteger, primary_key=True)
    name = mapped_column(Text, unique=True)
    org = mapped_column(Text)
    books = mapped_column(ARRAY(item_type=Integer))
    nickname: Mapped[Optional[str]]

    order_collection = relationship("Order", back_populates="user")

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name}>"


class Order(Base):
    __tablename__ = "order"

    id = mapped_column(BigInteger, primary_key=True)
    updated_at = mapped_column(
        DateTime(timezone=True),
        server_default=func.current_timestamp(0),
        onupdate=func.current_timestamp(0),
    )
    created_at = mapped_column(
        DateTime(timezone=True),
        server_default=func.current_timestamp(),
    )
    quantity = mapped_column(BigInteger)
    price_num = mapped_column(Numeric(precision=10, scale=2))  # 总10位，小数2位
    price = mapped_column(Float)  # cast(...,Numeric(10,2)后再比较相等
    status = mapped_column(Enum(Status, native_enum=False))  # Enum 参数默认native_enum=True，等价pg ENUM type
    # status = Column(Enum(Status, values_callable=lambda obj: [str(x.value) for x in obj]))
    block_time = mapped_column(BigInteger)

    user_id = mapped_column(ForeignKey(User.id, ondelete="CASCADE"))
    user = relationship("User", back_populates="order_collection")

    # https://docs.sqlalchemy.org/en/20/orm/mapped_sql_expr.html
    @hybrid_property
    def charge(self):
        return self.price * self.quantity

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.id}>"


class GeoIp(Base):
    __tablename__ = "geoip2_network"
    __table_args__ = (
        Index("network_gist", "network", postgresql_using="gist", postgresql_ops={"network": "inet_ops"}),
    )

    network = mapped_column(CIDR, nullable=False, primary_key=True)
    geoname_id = mapped_column(Integer)
    registered_country_geoname_id = mapped_column(Integer)
    represented_country_geoname_id = mapped_column(Integer)
    is_anonymous_proxy = mapped_column(Boolean)
    is_satellite_provider = mapped_column(Boolean)
    postal_code = mapped_column(Text)
    latitude = mapped_column(Numeric)
    longitude = mapped_column(Numeric)
    accuracy_radius = mapped_column(Integer)


# os.environ["PGTZ"] = "utc"
#  -c statement_timeout=300
db = create_engine(settings.db, echo=True, pool_size=5, connect_args={"options": "-c TimeZone=Asia/Tokyo"})
Base.metadata.drop_all(bind=db, tables=[User.__table__, Order.__table__, GeoIp.__table__])
Base.metadata.create_all(bind=db)

Session = sessionmaker(bind=db)

with Session() as session:
    with session.begin():
        session.execute(select(User).where(User.id == 1500))

with Session.begin() as session:
    session.execute(select(User).where(User.id == 1500))


async def async_s():
    # async session的定义与iter
    async_db = create_async_engine(settings.db_asyncpg, echo=False, pool_size=5)
    Session = async_sessionmaker(bind=async_db)
    async with Session.begin() as s:
        # s.run_sync(Base.metadata.create_all)
        # async iter
        async for x in await s.stream_scalars(select(...).execution_options(yield_per=500)):
            ...


s = Session()
# 在重复查询相同的对象时，直接先查询本地的缓存；需要expire或refresh，或expire_on_commit
setup(s)
make(User, 2)
make(Order, 50)
make(GeoIp, 50)

# &, | and ~
literal_column
literal
column
# raw sql
text("select 1")

# 基本用法
obj = s.scalars(select(User)).first()
s.query(Order).where(Order.id == 1).first()
select(User).where(User.id == 1).with_for_update()

# 迭代
for obj in s.scalars(select(User).execution_options(yield_per=500)):
    ...

# 返回结果组装
st = (
    select(Bundle("attr", User.name, Order.quantity, single_entity=True))
    .join_from(User, Order)
    .where(Order.price < 159)
)
for x in s.scalars(st):
    (x.name, x.quantity)
# named tuple
st = select(User.name, Order.quantity).join_from(User, Order).where(Order.price < 159)
for x in s.execute(st):
    (x.name, x.quantity)

# 统计
select(func.count()).select_from(User)
select(func.sum(Order.quantity))
select(User.org).group_by(User.org)

# 排序
select(User.name).order_by(User.id.desc())
select(User.name).order_by(asc("id"))

# distinct 非重复值， + order_by增强为distinct on，组内最...的记录
select(GeoIp).distinct(GeoIp.geoname_id)
select(GeoIp).distinct(GeoIp.geoname_id).order_by(GeoIp.geoname_id)

# 指定输出字段
s.execute(select(User).with_only_columns(User.name))  # 不包含id，使用execute
select(User).options(load_only(User.name))  # 延迟加载

# 列别名, execute
select(Order.id, (Order.quantity * Order.price).label("total"))

# 刷新对象
s.refresh(obj)
select(User).execution_options(populate_existing=True)

# 关联查询
select(User).join(Order, User.id == Order.user_id).where(Order.price < 159)
st = select(User.name, Order.quantity).join_from(User, Order).where(Order.price < 159)
for x in s.execute(st):
    (x.name, x.quantity)

# n+1
# select(...).join(...).options(joinedload())的优化版，需要显式join， 总1次查询
select(Order).join(User).options(contains_eager(Order.user)).limit(5)
# 显式join(User)，joinedload User表join两次
# st = select(Order).join(User).options(joinedload(Order.user)).limit(5)
# 不指定join(User)，正常
# st = select(Order).options(joinedload(Order.user)).limit(5)
# 下面方式，是否指定join(User)无影响
# 先查出limit数量的子表，然后访问属性时发起第二条查询，父表id in 集合，总2次查询
select(Order).join(User).options(selectinload(Order.user)).limit(5)
# 先查出limit数量的子表，然后访问属性时发起第二条查询，父表join(前面查子表语句为子查询)，总2次查询
select(Order).join(User).options(subqueryload(Order.user)).limit(5)
# immediateload 若user已查询，则无操作，否则单独sql查询user，总查询 "不同user" 数量
# st = select(Order).join(User).options(immediateload(Order.user)).limit(5)

# where过滤关联表时，需要显式join
select(User).join(Order).options(selectinload(User.order_collection)).where(Order.quantity == 2)

# any 如何使用
select(Order).where(Order.user_id == any_([1]))
select(Order).where(Order.user_id.in_([1]))
# not_in

# ARRAY 字段
# https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.ARRAY.Comparator
# https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.ARRAY
# 被另一个array包含
User.books.contained_by([1, 2, 3, 4, 5])
# 包含另一个array
User.books.contains([1])
# 包含一个值
User.books.any(1)
User.books.any(7, operator=operators.gt)  # 7>any(field)
# 与另一个array存在交集 overlap
#  jsonb 对象方式修改实例的array或者jsonb属性，需要标记是否已修改
obj = s.scalar(select(User).order_by(User.id).limit(1))
obj.books.append(23)
flag_modified(obj, "books")
s.commit()
s.execute(update(User).where(User.id == 1).values({User.books: User.books + [5]}))

# datetime 字段
updated_at = func.current_timestamp() + cast("20s", INTERVAL)
select(Order).where(Order.updated_at.cast(Date) == "2023-7-23")  # 客户端时区
select(Order).where(func.date_trunc("second", Order.updated_at) == "2023-7-22 22:24:39")
select(
    Order.block_time,
    func.to_char(
        func.to_timestamp(Order.block_time).op("AT TIME ZONE")("Asia/Shanghai"),
        "YYYY-MM-DD HH24:MI:SS",
    ),
)
select(
    Order.block_time,
    func.to_char(
        func.timezone("Asia/Shanghai", func.to_timestamp(Order.block_time)),
        "YYYY-MM-DD HH24:MI:SS +8",
    ),
)

# bool字段
GeoIp.is_anonymous_proxy.is_(True)
GeoIp.is_anonymous_proxy.is_not(None)

# 字符串
User.org.ilike("bsc99%")  # 包含%
User.org.istartswith("bsc")
User.org.icontains("bsc")
User.org.regexp_match("^bsc.*", "i")

# 数值
select(Order).where((Order.id <= 8) & (Order.id >= 4))
select(Order).where(Order.id.between(4, 8))  # 闭区间
# 浮点比较
select(func.round(cast(Order.price * 3, Numeric), 2)).where(cast(Order.price, Numeric(10, 2)) == 57.63)

# inet 字段, params可以是str，也可以是ipaddress对象
one = s.scalar(select(GeoIp).limit(1))
select(GeoIp.network).where(text("network >>= :ip")).params(ip=one.network)
select(GeoIp.network).where(GeoIp.network.op(">>=")(one.network))
# inet'192.168.31.1'; cidr 192.168.31.1/32
# inet'192.168.31.1/24'; cidr'192.168.31.1/24' error


user_data = {"name": "myname", "org": "some", "books": []}


# 创建单个
def insert_single():
    user = User(**user_data)
    s.add(user)
    s.commit()
    print(user.id)


# 批量创建
def bulk_insert():
    user = User(**user_data)
    s.add_all([user])
    s.commit()

    # # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#updating-using-the-excluded-insert-values
    # returning 触发 use_insertmanyvalues 优化；否则会将params拆开，每个执行insert
    s.execute(insert(User).on_conflict_do_nothing().returning(User.id), [user_data])
    s.commit()

    # 不需 returning，values是一个整体
    st = (
        insert(User)
        .values(
            [
                {User.name: "cccc", "org": "2022-1-1", "books": []},
            ]
        )
        .on_conflict_do_nothing()
    )
    s.execute(st)
    s.commit()


def update_single():
    obj = s.scalar(select(User).limit(1))
    obj.name = "other"
    s.commit()

    st = update(User).where(User.name == "other").values(org="other org")
    s.execute(st)
    s.commit()
    print(obj.org)


def bulk_update(model, other):
    # 批量更新 1.x 风格
    for obj in s.scalars(select(User).execution_options(yield_per=500)):
        obj.nickname += "sometext"
    s.commit()

    obj = s.scalar(select(User).limit(1))
    s.query(User).filter(User.id == obj.id).update({"org": "dskdkdkd"})

    # 批量更新 UPDATE..FROM values
    vst = values(
        column("name", Text),
        column("nickname", Text),
        name="vst",
    ).data(
        [
            ["a", "aaa1"],
            ["e", "bbb1"],
        ]
    )
    st = update(User).where(User.name == vst.c.name).values({User.nickname: vst.c.nickname})
    s.execute(st)
    s.commit()

    # 指定pk
    s.execute(update(model), [{"id": ..., "something": "somevalue"}])

    # https://docs.sqlalchemy.org/en/20/tutorial/data_update.html
    # 批量更新: 子查询
    query_owner = select(User).where(other.token_id == model.token_id).scalar_subquery()
    update(User).values(owner=query_owner)

    # 批量更新-关联查询 UPDATE..FROM
    update(User).where(other.token_id == model.token_id).values({model.owner: other.owner})

    # 批量更新：where与value绑定到字典的key; 拆分成多条语句执行
    to_update = [{"v_name": ..., "v_nickname": ...}]
    st = update(User).where(User.name == bindparam("v_name")).values(nickname=bindparam("v_nickname")).returning()
    with db.begin() as conn:
        conn.execute(st, to_update)


def update_with_case_value():
    st = (
        update(Order)
        .where(Order.price == 191)
        .values(
            quantity=case(
                (Order.price == 191, 550),
                else_=Order.quantity,
            )
        )
    )
    s.execute(st)
    s.commit()


def delete_single():
    s.delete(obj)
    s.commit()


def bulk_delete():
    s.query(User).filter(...).delete()
    s.commit()
    s.execute(delete(User).where(...))
    s.commit()
