import time

from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import *
from sqlalchemy.orm.attributes import flag_modified


class Base(DeclarativeBase):
    pass


class Demo(Base):
    __tablename__ = "test_json"
    id = Column(BigInteger, primary_key=True)
    datab = Column(JSONB)
    history = Column(JSONB)


# 参考资料：https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.JSONB


def modify_json_in_place(session, obj):
    # 原地修改 JSON/ARRAY 后，标记属性已修改；重新赋值整个属性时不需要此标记。
    obj.datab["max"] = 1000
    flag_modified(obj, "datab")
    session.commit()
    print(obj.datab)


def query_json(session):
    # 每条查询独立执行；前面的修改可能使某些条件不再匹配。
    st = select(Demo).where(Demo.datab.has_key("max"))
    print(session.scalars(st).all())
    st = select(Demo).where(Demo.datab["max"].as_integer() == 4)
    print(session.scalars(st).all())
    st = select(Demo).where(Demo.datab["desc"].as_string() == "测试")
    print(session.scalars(st).all())
    st = select(Demo).where(Demo.datab["is_deleted"].as_boolean().is_(False))
    print(session.scalars(st).all())
    st = select(Demo).where(Demo.datab["max"].astext.cast(Integer) == 4)
    print(session.scalars(st).all())


def set_json_key(session, obj):
    # 添加或修改对象的键。
    st = (
        update(Demo)
        .where(Demo.id == obj.id)
        .values(datab=func.jsonb_set(Demo.datab, ["ccc"], func.to_jsonb(50)))
        .returning(Demo)
    )
    obj = session.scalars(st).one()
    session.commit()
    print(obj.datab)


def set_json_array_item(session, obj):
    # 按索引修改数组；这里的索引 1 不存在时，会将新值追加到数组末尾。
    st = (
        update(Demo)
        .where(Demo.id == obj.id)
        .values(datab=func.jsonb_set(Demo.datab, ["min", "1"], func.to_jsonb(60)))
        .returning(Demo)
    )
    obj = session.scalars(st).one()
    session.commit()
    print(obj.datab)


def delete_json_array_item(session, obj):
    # 推荐用法（按路径删除）：delete_path()。
    # 备选表达式：Demo.datab.op("#-")(array(["min", "0"]))
    st = update(Demo).where(Demo.id == obj.id).values(datab=Demo.datab.delete_path(["min", "0"])).returning(Demo)
    obj = session.scalars(st).one()
    session.commit()
    print(obj.datab)


def delete_json_key(session, obj):
    # SQLAlchemy 2.1 已弃用 JSONB 的 Python 减法；推荐用 .op("-") 显式表达删除操作符。
    # 旧写法：Demo.datab - "ccc"
    st = update(Demo).where(Demo.id == obj.id).values(datab=Demo.datab.op("-")("ccc")).returning(Demo)
    obj = session.scalars(st).one()
    session.commit()
    print(obj.datab)


def delete_json_keys(session, obj):
    # 同时删除多个键；与删除单个键的示例分别执行，避免覆盖语句。
    # 旧写法：Demo.datab - cast(["max", "min"], ARRAY(Text))
    # 旧写法：Demo.datab - array(["max", "min"], type_=ARRAY(Text))
    st = (
        update(Demo)
        .where(Demo.id == obj.id)
        .values(datab=Demo.datab.op("-")(array(["max", "min"], type_=Text)))
        .returning(Demo)
    )
    obj = session.scalars(st).one()
    session.commit()
    print(obj.datab, "删除 max、min")


def append_history_object(session, obj):
    # 将空值变成空数组再拼接对象；.op("||") 与 .concat() 对应。
    # 对应 SQL：COALESCE(history, '[]'::JSONB) || :new_value::JSONB
    st = (
        update(Demo)
        .where(Demo.id == obj.id)
        .values(history=func.coalesce(Demo.history, cast([], JSONB)).concat({"request_time": 1698246526}))
    )
    session.execute(st)
    session.commit()
    print(obj.history)


def append_history_array(session, obj):
    # jsonb_build_array() 生成空数组；这里拼接一个仅含一条记录的数组。
    st = (
        update(Demo)
        .where(Demo.id == obj.id)
        .values(history=func.coalesce(Demo.history, func.jsonb_build_array()) + [{"request_time": int(time.time())}])
    )
    session.execute(st)
    session.commit()
    print(obj.history)


def query_history(session, obj):
    st = select(func.jsonb_array_length(Demo.history)).where(Demo.id == obj.id)
    print(session.scalar(st))
    # 查询数组中的对象是否包含 request_time 键。
    # 对应 SQL：history @? '$[*].request_time'
    st = select(Demo).where(Demo.id == obj.id, Demo.history.path_exists(cast("$[*].request_time", JSONPATH)))
    print(session.scalars(st).all())
    # 若时间值存为 ISO 文本，可使用以下路径筛选；本例存的是 Unix 秒数。
    # history @? '$[*] ? (@.request_time < "2023-08-24T00:00:00+00:00")'


def delete_history_item(session, obj):
    """删除示例时间的历史项，保留剩余元素的顺序。

    原生 SQL 批量删除参考，时间值与本函数的单对象示例不同：
    UPDATE test_json t
    SET history = (SELECT JSONB_AGG(j.element ORDER BY j.idx)
                   FROM JSONB_ARRAY_ELEMENTS(t.history) WITH ORDINALITY AS j(element, idx)
                   WHERE NOT j.element @> '{"request_time": 1698224975}')
    WHERE t.history @> '[{"request_time": 1698224975}]';
    """
    # 参考资料：https://docs.sqlalchemy.org/en/21/dialects/postgresql.html#table-valued-functions
    # WITH ORDINALITY 记录原位置；聚合时保持剩余元素的顺序。
    # 已有 JSON 文本时，可用 cast(type_coerce('{"request_time": 1698246526}', Text), JSONB)。
    j = (
        func.jsonb_array_elements(Demo.history)
        .table_valued(column("element", JSONB), with_ordinality="idx")
        .render_derived("j")
    )
    # 方言辅助函数写法仍可用：func.jsonb_agg(aggregate_order_by(j.c.element, j.c.idx.asc()))。
    # 推荐用法（SQLAlchemy 2.1+）：通过聚合函数的 aggregate_order_by() 链式指定顺序。
    sub = (
        select(func.jsonb_agg(j.c.element).aggregate_order_by(j.c.idx.asc()))
        .select_from(j)
        .where(~j.c.element.contains(cast({"request_time": 1698246526}, JSONB)))
    )
    st = (
        update(Demo)
        .where(
            Demo.id == obj.id, Demo.history.contains(func.jsonb_build_array(cast({"request_time": 1698246526}, JSONB)))
        )
        .values(history=sub.scalar_subquery())
        .returning(Demo)
    )
    print(session.scalar(st))
    session.commit()


if __name__ == "__main__":
    from config import settings

    db = create_engine(settings.db_url)
    try:
        Base.metadata.drop_all(bind=db, tables=[Demo.__table__])
        Base.metadata.create_all(bind=db)
        with sessionmaker(bind=db)() as session:
            obj = Demo(datab={"max": 4, "min": [{"aa": 4}], "desc": "测试", "is_deleted": False})
            session.add(obj)
            session.commit()
            modify_json_in_place(session, obj)
            query_json(session)
            set_json_key(session, obj)
            set_json_array_item(session, obj)
            delete_json_array_item(session, obj)
            delete_json_key(session, obj)
            delete_json_keys(session, obj)
            append_history_object(session, obj)
            append_history_array(session, obj)
            query_history(session, obj)
            delete_history_item(session, obj)
    finally:
        db.dispose()
