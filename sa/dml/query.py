from sqlalchemy import *
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.orm import *
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql import operators

from sa.model.example import Author, Order, GeoIp
from sa.session import Session

s = Session()

# &, | and ~

# 字面字段,
literal_column("0")
# 使xyz作为一个参数传递给数据库
literal("xyz")
# 字段
column("abc")

# raw sql
text("select 1")
text("default")

# 基本用法
obj = s.scalars(select(Author)).first()
s.query(Order).where(Order.id == 1).first()
select(Author).where(Author.id == 1).with_for_update()

# 列别名
select(Order.id, (Order.quantity * Order.price).label("total"))
# 表 (返回多列的结构)别名
aliased

# 在重复查询相同的对象时，直接先查询本地的缓存；需要expire或refresh，或expire_on_commit
s.refresh(obj)
select(Author).execution_options(populate_existing=True)

# 组装结果
st = select(Bundle("attr", Author.name, Order.quantity)).join_from(Author, Order).where(Order.price < 159)
for x in s.execute(st):
    (x.attr.name, x.attr.quantity)
# named tuple
st = select(Author.name, Order.quantity).join_from(Author, Order).where(Order.price < 159)
for x in s.execute(st):
    (x.name, x.quantity)

# 指定输出字段
select(Author).with_only_columns(Author.name)  # 不包含id
select(Author).options(load_only(Author.name))  # 延迟加载

# 统计
select(func.count()).select_from(Author)
select(func.sum(Order.quantity))
select(Author.org).group_by(Author.org)

# 排序
select(Author.name).order_by(Author.id.desc())
select(Author.name).order_by(asc("id"))

# distinct 非重复值， + order_by增强为distinct on，组内最...的记录
select(GeoIp).distinct(GeoIp.geoname_id)
select(GeoIp).distinct(GeoIp.geoname_id).order_by(GeoIp.geoname_id)

# any 如何使用
select(Order).where(Order.author_id == any_([1]))
select(Order).where(Order.author_id.in_([1]))
# not_in

# ARRAY 字段
# https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.ARRAY.Comparator
# https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.ARRAY
# 被另一个array包含
Author.books.contained_by([1, 2, 3, 4, 5])
# 包含另一个array
Author.books.contains([1])
# 包含一个值
Author.books.any(1)
# 7只要比字段数组中的一个值大就为True
Author.books.any(7, operator=operators.gt)
# 与另一个array存在交集 overlap

#  jsonb 对象方式修改实例的array或者jsonb属性，需要标记是否已修改
obj = s.scalar(select(Author).order_by(Author.id).limit(1))
obj.books.append(23)
flag_modified(obj, "books")
s.commit()
s.execute(update(Author).where(Author.id == 1).values({Author.books: Author.books + [5]}))

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
Author.org.ilike("bsc99%")  # 包含%
Author.org.istartswith("bsc")
Author.org.icontains("bsc")
Author.org.regexp_match("^bsc.*", "i")

# 数值
select(Order).where((Order.id <= 8) & (Order.id >= 4))
select(Order).where(Order.id.between(4, 8))  # 闭区间
# 浮点比较
select(func.round(cast(Order.price * 3, Numeric), 2)).where(cast(Order.price, Numeric(10, 2)) == 57.63)

# inet 字段, params可以是str，也可以是ipaddress对象
one = s.scalar(select(GeoIp).limit(1))
select(GeoIp.network).where(GeoIp.network.op(">>=")(one.network))
select(GeoIp.network).where(text("network >>= :ip")).params(ip=one.network)
# inet'192.168.31.1'; cidr 192.168.31.1/32
# inet'192.168.31.1/24'; cidr'192.168.31.1/24' error

s.commit()
s.close()
