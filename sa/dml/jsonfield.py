import time

from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import *
from sqlalchemy.orm.attributes import flag_modified

from conf import settings

db = create_engine(settings.engine)
session = sessionmaker(bind=db)


class Base(DeclarativeBase):
    pass


class Demo(Base):
    __tablename__ = "test_json"
    id = Column(BigInteger, primary_key=True)
    datab = Column(JSONB)
    history = Column(JSONB)


Base.metadata.drop_all(bind=db, tables=[Demo.__table__])
Base.metadata.create_all(bind=db)
obj = Demo(
    datab={"max": 4, "min": [{"aa": 4}], "desc": "测试", "is_deleted": False},
)

s = session()
s.add(obj)
s.commit()

# jsonb字段
# https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.JSONB

#  jsonb 对象方式修改实例的array或者jsonb属性，需要标记是否已修改
obj.datab["max"] = 1000
flag_modified(obj, "datab")
s.commit()
print(obj.datab)

# jsonb 查询
st = select(Demo).where(Demo.datab.has_key("max"))
# 查询value为某值 JSONB.Comparator:
st = select(Demo).where(Demo.datab["max"].as_integer() == 4)
st = select(Demo).where(Demo.datab["desc"].as_string() == "测试")
st = select(Demo).where(Demo.datab["is_deleted"].as_boolean().is_(False))
st = select(Demo).where(Demo.datab["max"].astext.cast(Integer) == 4)

# jsonb 添加、修改key
st = update(Demo).values(datab=func.jsonb_set(Demo.datab, ["ccc"], func.to_jsonb(50))).returning(Demo)
obj = s.scalars(st).first()
print(obj.datab)

# jsonb 按索引更新
st = update(Demo).values(datab=func.jsonb_set(Demo.datab, ["min", "1"], func.to_jsonb(60))).returning(Demo)
obj = s.scalars(st).first()
print(obj.datab)

# jsonb 按索引删除
# st = update(Demo).values(datab=Demo.datab.op("#-")(array(["min", "0"])))
st = update(Demo).values(datab=Demo.datab.delete_path(["min", "0"]))
# jsonb 按key删除
st = update(Demo).values(datab=Demo.datab - "ccc").returning(Demo)
# st = update(Demo).values(datab=Demo.datab - cast(["max", "min"], ARRAY(Text))).returning(Demo)
st = update(Demo).values(datab=Demo.datab - array(["max", "min"], type_=ARRAY(Text))).returning(Demo)

obj = s.scalars(st).first()
s.commit()
print(obj.datab, "del max min")

# jsonb_array 添加
# sql request_history=COALESCE(request_history,'[]'::JSONB)||$4::jsonb
# jsonb_build_array() 生成空jsonb_array; .op("||") <==> .concat
st = (
    update(Demo)
    .where(Demo.id == 1)
    .values(history=func.coalesce(Demo.history, cast([], JSONB)).concat({"request_time": 1698246526}))
)
s.execute(st)
st = (
    update(Demo)
    .where(Demo.id == 1)
    .values(history=func.coalesce(Demo.history, func.jsonb_build_array()) + [{"request_time": int(time.time())}])
)
s.execute(st)
s.commit()

# jsonb_array 查询长度
st = select(func.jsonb_array_length(Demo.history)).select_from(Demo).where(Demo.id == 1)
print(s.scalar(st))
# jsonb_array 查询包含key
# sql ="WHERE request_history @? '$[*].request_time'"
st = select(Demo).where(Demo.history.path_exists(cast("$[*].request_time", JSONPATH)))
# jsonb_array 查询查询value
sql = """... WHERE request_history @? '$[*] ? (@.request_time<"2023-08-24T00:00:00+00:00")'"""

# jsonb_array 修改、删除
# cast(type_coerce('{"request_time": 1698246526}', Text), JSONB)
sql = """UPDATE test_json t
SET    history = (SELECT JSONB_AGG(j.element ORDER BY j.idx)
             FROM   JSONB_ARRAY_ELEMENTS(t.history) WITH ORDINALITY AS j(element, idx)
             WHERE  NOT j.element @> '{"request_time": 1698224975}')
WHERE  t.history @> '[{"request_time": 1698224975}]';"""

# https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#table-valued-functions
j = (
    func.jsonb_array_elements(Demo.history)
    .table_valued(column("element", JSONB), with_ordinality="idx")
    .render_derived("j")
)
sub = (
    select(func.jsonb_agg(aggregate_order_by(j.c.element, j.c.idx.asc())))
    .select_from(j)
    .where(~j.c.element.contains(cast({"request_time": 1698246526}, JSONB)))
)
st = (
    update(Demo)
    .values(history=sub.scalar_subquery())
    .where(Demo.history.contains(func.jsonb_build_array(cast({"request_time": 1698246526}, JSONB))))
    .returning(Demo)
)
print(s.scalar(st))
