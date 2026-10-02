from sqlalchemy import *
from sqlalchemy.dialects.postgresql import INTERVAL, distinct_on
from sqlalchemy.orm import *
from sqlalchemy.orm.attributes import flag_modified

from sa.model.example import Author, Order, GeoIp


def query_examples(session):
    # ── 1. 数据库环境 ──
    lc_collate = (
        session.execute(
            text(
                "SELECT datcollate AS lc_collate,datctype AS lc_ctype FROM pg_database WHERE datname = CURRENT_DATABASE();"
            ),
        )
        .mappings()
        .one()
    )

    # ── 2. SQL 表达式与字面量 ──
    # 逻辑运算符：& 表示且，| 表示或，~ 表示取反。

    # 字面字段,
    literal_column("0")
    # 使xyz作为一个参数传递给数据库
    literal("xyz")
    # 字段
    column("abc")

    # 原生 SQL
    text("select 1")
    text("default")

    # ── 3. ORM 对象、结果行与输出字段 ──
    # 推荐用法（查询 ORM 实体）：select() 配合 Session.scalars()；查询多列时使用 execute()。
    obj = session.scalars(select(Author)).first()
    # 旧式 Query 接口仍可使用；新笔记优先 select()，不将旧接口标成弃用。
    session.query(Order).where(Order.id == 1).first()
    select(Author).where(Author.id == 1).with_for_update()

    # 列别名
    select(Order.id, (Order.quantity * Order.price).label("total"))
    # 表 (返回多列的结构)别名
    aliased

    # 组装结果
    st = select(Bundle("attr", Author.name, Order.quantity)).join_from(Author, Order).where(Order.price < 159)
    for x in session.execute(st):
        (x.attr.name, x.attr.quantity)
    # 按列查询后，可通过结果行的字段名访问值。
    st = select(Author.name, Order.quantity).join_from(Author, Order).where(Order.price < 159)
    for x in session.execute(st):
        (x.name, x.quantity)

    # 指定输出字段
    select(Author).with_only_columns(Author.name)  # 不包含id
    select(Author).options(load_only(Author.name))  # 延迟加载

    # ── 4. 会话中的对象缓存与刷新 ──
    # 普通 SELECT 仍执行 SQL；标识映射复用已有对象，通常不覆盖已加载的属性。
    # 推荐用法（按主键取对象）：Session.get()；对象已在会话中且未过期时可避免查询。
    session.get(Author, obj.id)
    session.refresh(obj)
    select(Author).execution_options(populate_existing=True)

    # ── 5. 聚合统计 ──
    # 统计
    select(func.count()).select_from(Author)
    select(func.sum(Order.quantity))
    select(Author.org).group_by(Author.org)

    # ── 6. 排序与 DISTINCT ON ──
    # 排序
    select(Author.name).order_by(Author.id.desc())
    select(Author.name).order_by(asc("id"))

    # DISTINCT 去掉所选行的重复值；PostgreSQL 的 DISTINCT ON 则按指定键保留一条。
    # SQLAlchemy 2.1 已弃用传列给 distinct() 的写法：select(GeoIp).distinct(GeoIp.geoname_id)
    # 推荐用法（PostgreSQL 分组首条）：ext(distinct_on(...))；排序末尾加唯一列以消除并列的不确定性。
    select(GeoIp).ext(distinct_on(GeoIp.geoname_id)).order_by(GeoIp.geoname_id, GeoIp.network)

    # ── 7. 通用条件：列表成员、区间与数值 ──
    # any 如何使用
    select(Order).where(Order.author_id == any_([1]))
    # 推荐用法（普通值列表成员判断）：in_()，清楚地表达普通值列表的成员判断。
    select(Order).where(Order.author_id.in_([1]))
    # 排除指定值：not_in()

    # 数值
    select(Order).where((Order.id <= 8) & (Order.id >= 4))
    # 推荐用法（单字段闭区间）：between()；组合复杂条件时使用上面的比较表达式。
    select(Order).where(Order.id.between(4, 8))  # 闭区间
    # 浮点比较
    select(func.round(cast(Order.price * 3, Numeric), 2)).where(cast(Order.price, Numeric(10, 2)) == 57.63)

    # ── 8. 字符串查询 ──
    # 字符串
    Author.org.ilike("bsc99%")  # 包含%
    Author.org.istartswith("bsc")
    Author.org.icontains("bsc")
    Author.org.regexp_match("^bsc.*", "i")

    # ── 9. 布尔值与空值查询 ──
    # bool字段
    GeoIp.is_anonymous_proxy.is_(True)
    GeoIp.is_anonymous_proxy.is_not(None)

    # ── 10. 数组查询与修改 ──
    # ARRAY 字段
    # 参考资料：https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.ARRAY.Comparator
    # 参考资料：https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.ARRAY
    # 被另一个array包含
    Author.books.contained_by([1, 2, 3, 4, 5])
    # 包含另一个array
    Author.books.contains([1])
    # SQLAlchemy 2.1 已弃用数组的 ARRAY.Comparator.any()/all()；推荐使用 any_()/all_()。
    # 下面的旧式操作符示例还需导入：from sqlalchemy.sql import operators
    # Author.books.any(1)
    1 == any_(Author.books)
    # Author.books.any(7, operator=operators.gt)
    # 只要数组中至少一个元素小于 7，结果就为真。
    7 > any_(Author.books)
    # 与另一个array存在交集 overlap

    #  jsonb 对象方式修改实例的array或者jsonb属性，需要标记是否已修改
    obj = session.scalar(select(Author).order_by(Author.id).limit(1))
    obj.books.append(23)
    flag_modified(obj, "books")
    session.commit()
    session.execute(update(Author).where(Author.id == 1).values({Author.books: Author.books + [5]}))

    # ── 11. 日期与时间查询 ──
    # datetime 字段
    updated_at = func.current_timestamp() + cast("20s", INTERVAL)
    # 将 timestamptz 转为 date 时，使用数据库连接的 TimeZone 设置。
    select(Order).where(Order.updated_at.cast(Date) == "2023-7-23")
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

    # ── 12. 网络地址查询 ──
    # inet 字段, params可以是str，也可以是ipaddress对象
    one = session.scalar(select(GeoIp).limit(1))
    select(GeoIp.network).where(GeoIp.network.op(">>=")(one.network))
    select(GeoIp.network).where(text("network >>= :ip")).params(ip=one.network)
    # 主机地址示例：inet '192.168.31.1'；CIDR 对应 192.168.31.1/32。
    # inet 可表示 192.168.31.1/24；同值直接作为 CIDR 会因主机位非零而报错。

    session.commit()


if __name__ == "__main__":
    from sa.session import SessionMaker

    with SessionMaker() as session:
        query_examples(session)
