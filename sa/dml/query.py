from sqlalchemy import *
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.orm import *
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql import operators

from sa.model.usercase import User, Order, GeoIp
from sa.session import s

# 在重复查询相同的对象时，直接先查询本地的缓存；需要expire或refresh，或expire_on_commit

# &, | and ~

# 字面字段, literal_column("0")
literal_column
# 字面值
literal
# 字段
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
