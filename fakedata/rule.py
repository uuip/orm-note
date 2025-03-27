import datetime
import random
from functools import lru_cache

import pandas as pd
from mimesis import Locale, Generic
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept
from sqlalchemy.orm.relationships import _RelationshipDeclared

from . import factory

g = Generic(locale=Locale.ZH)

today = datetime.date.today()
yesterday = today - datetime.timedelta(hours=24 * 7)
tomorrow = today + datetime.timedelta(hours=24 * 2)


@lru_cache()
def is_unique(field: Column):
    if field.unique:
        return field.unique
    for x in field.table.constraints:
        if (
            isinstance(x, UniqueConstraint)
            and len(x.columns) == 1
            and field.name in x.columns
        ):
            return True
    for x in field.table.indexes:
        if isinstance(x, Index) and len(x.columns) == 1 and field.name in x.columns:
            return x.unique


def general_rule(field: Column):
    if field.primary_key and (
        field.autoincrement is True
        or isinstance(field.type, Integer)
        or isinstance(field.identity, Identity)
    ):
        return text("default")
    if field.server_default:
        return text("default")
    if isinstance(field.type, Enum):
        return random.choice(list(field.type.enums))
    if isinstance(field.type, Integer):
        return g.numeric.integer_number(start=1)
    if isinstance(field.type, Float):
        return g.numeric.float_number()
    if isinstance(field.type, Boolean):
        return g.development.boolean()
    if isinstance(field.type, (VARCHAR, Text, String)):
        if field.primary_key or is_unique(field):
            return factory.unique_str()
        if "email" == field.name:
            return g.person.email(unique=True)
        if "username" == field.name:
            return g.person.username()
        if "phone" == field.name:
            return g.person.telephone()
        if "first_name" == field.name:
            return g.person.first_name()
        if "last_name" == field.name:
            return g.person.last_name()
        if "password" == field.name:
            return g.person.password()
        if "nick_name" == field.name:
            return g.person.last_name()
        if "号" in field.name:
            return str(g.numeric.integer_number(start=1000))
        if "geoname" == field.name or "country_name" == field.name:
            return g.address.country()
        if "name" in field.name:
            return g.person.name()
        if "time" in field.name:
            t = g.datetime.datetime(start=yesterday.year)
            return t.isoformat()
        return factory.cn_words(length=field.type.length or 5)
    if isinstance(field.type, (DateTime, TIMESTAMP)):
        if field.name == "end_at":
            return g.datetime.datetime(start=today.year, end=tomorrow.year)
        return g.datetime.datetime(start=yesterday.year)
    if isinstance(field.type, Date):
        if field.name == "end_at":
            return g.datetime.date(start=today.year, end=tomorrow.year)
        return g.datetime.date(start=today.year)
    if isinstance(field.type, (DECIMAL, Numeric, NUMERIC)):
        return g.numeric.decimal_number()
    if isinstance(field.type, (JSON, JSONB)):
        return {}
    if isinstance(field.type, ARRAY):
        return []
    if isinstance(field.type, (INET, CIDR)):
        return g.internet.ip_v4_object()
    if field.default:
        default = field.default
        if callable(default):
            return default()
        return default
    if field.nullable:
        return null()
    raise NotImplementedError(f"no rule for this type: {field.type}")


def get_related_model(field: _RelationshipDeclared):
    local_column = list(field.local_columns)[0]
    if list(field.remote_side)[0].table.name == local_column.table.name:
        if local_column.nullable:
            return
        raise NotImplementedError("关联自己")
    return field.mapper.class_


def get_db_column_mapper(model):
    # 字段定义时如果指定name属性，那么orm字段与数据库列不一致。
    # db表中实际字段(key)与orm属性列(value)映射
    return {v.name: k for k, v in model.__mapper__.c.items()}


def get_primary_key(model):
    db_column_map_orm_field = get_db_column_mapper(model)
    db_column = inspect(model).primary_key[0].name
    return db_column_map_orm_field[db_column]


def required_fields(model):
    # 找出需要哪些字段
    # model.__mapper__.c.items() 列出了必需的数据库字段，外键字段是author_id
    # model.__mapper__.attrs.items() 列出了模型定义的所有字段，外键字段author_id，author都列出
    # 对于外键字段，存在类似 author, author_id 两个字段，只能选一个
    # 如果以attrs.items()要判断多种情况,还要处理自引用这个情况。
    # 于是以c.items() 为base

    # 使用orm方式( session.add() ) 使用 author字段
    # 使用insert() 语句 使用 author_id字段

    related_actual_field_defines = get_related_actual_field_defines(model)

    for k, v in model.__mapper__.c.items():
        if v.foreign_keys:
            yield k, related_actual_field_defines[k]["define"]
        else:
            yield k, v


def get_related_actual_field_defines(model):
    # c.items() 中 author_id 只是 数据库中表的字段定义，不是orm的属性定义，没有模型信息。
    # 这些信息在attrs.items()反而有。

    db_column_map_orm_field = get_db_column_mapper(model)

    related_actual_field_defines = {}

    for k, v in model.__mapper__.attrs.items():
        if hasattr(v, "columns"):
            # k, v.key, v.columns[0].name
            continue
        if isinstance(v, _RelationshipDeclared):
            if v.direction.name == "MANYTOONE":
                # 这里返回的是orm列定义中的name，与orm字段不完全一致
                db_column = v.local_remote_pairs[0][0]
                orm_actual_field = db_column_map_orm_field[db_column.name]
                # author_id: {"mapper_field": author,...}
                related_actual_field_defines[orm_actual_field] = {
                    "mapper_field": k,
                    "define": v,
                    "unique": db_column.unique,  # onetoone
                }
            elif v.direction.name == "ONETOMANY":
                pass
                # print("ONETOMANY no need")
            else:  # MANYTOMANY
                pass
                # print("handle MANYTOMANY by yourself")
    return related_actual_field_defines


def generate_related_field_data(s, model, field_name, field: _RelationshipDeclared, size):
    related_2_mapped = get_related_actual_field_defines(model)

    target_model = get_related_model(field)

    if related_2_mapped[field_name]["unique"]:  # one to one
        related_size = size
    else:
        related_size = 1

    if target_model is None:
        objs = [None] * size
    else:
        objs = s.scalars(
            select(target_model).order_by(func.random()).limit(related_size)
        ).all()
        if len(objs) < related_size:
            make_fake_data(s, target_model, size=related_size - len(objs))
        objs = s.scalars(
            select(target_model).order_by(func.random()).limit(related_size)
        ).all() * int(size / related_size)

    if size == 1:
        #  session.add() 方式
        return related_2_mapped[field_name]["mapper_field"], objs
    # insert 方式
    return field_name, [getattr(obj, get_primary_key(target_model)) for obj in objs]


def make(session_maker: sessionmaker, model: DeclarativeAttributeIntercept, size=1):
    with session_maker() as s:
        make_fake_data(s, model, size)


def make_fake_data(s, model, size=1):
    # 填充字段数据
    df = pd.DataFrame()
    for field_name, field in required_fields(model):
        if isinstance(field, _RelationshipDeclared):
            related_data = generate_related_field_data(s, model, field_name, field, size)
            df[related_data[0]] = related_data[1]
        else:
            df[field_name] = [general_rule(field) for _ in range(size)]
    if size == 1:
        instance = model(**df.to_dict(orient="records")[0])
        s.add(instance)
        s.commit()
        s.refresh(instance)
        return instance
    else:
        st = insert(model).values(df.to_dict(orient="records"))
        s.execute(st)
        s.commit()
