import datetime
import pandas as pd
import random
import sys
from functools import lru_cache
from mimesis import Locale, Generic
from pathlib import Path
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm.relationships import _RelationshipDeclared

import factory

g = Generic(locale=Locale.ZH)

today = datetime.date.today()
yesterday = today - datetime.timedelta(hours=24 * 7)
tomorrow = today + datetime.timedelta(hours=24 * 2)


@lru_cache()
def is_unique(field):
    if field.unique:
        return field.unique
    for x in field.table.constraints:
        if isinstance(x, UniqueConstraint) and len(x.columns) == 1 and field.name in x.columns:
            return True
    for x in field.table.indexes:
        if isinstance(x, Index) and len(x.columns) == 1 and field.name in x.columns:
            return x.unique


def general_rule(field: Column):
    if field.autoincrement is True or field.name == "id":
        return text("default")
    if isinstance(field.type, Integer):
        return g.numeric.integer_number(start=1)
    if isinstance(field.type, Float):
        return g.numeric.float_number()
    if isinstance(field.type, Boolean):
        return g.development.boolean()
    if isinstance(field.type, (VARCHAR, Text, String)):
        if field.primary_key or is_unique(field):
            return faker.unique_str()
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
        return faker.cn_words(length=field.type.length or 5)
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
    if isinstance(field.type, Enum):
        return random.choice(list(field.type.enums))
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
    raise NotImplementedError(f"no rule for this type: {field.type}")


def relation_rule(field: _RelationshipDeclared):
    local_column = list(field.local_columns)[0]
    if list(field.remote_side)[0].table.name == local_column.table.name:
        if local_column.nullable:
            return
        raise NotImplementedError("关联自己")
    return field.mapper.class_manager.class_


def required_fields(model):
    # 一个外键包含2个字段，实际字段author_id,映射字段author

    #  orm 的列，不是db列，例如 from在python是关键字，orm里属性列是from_，而不是db的from
    db_map_model = {v.name: k for k, v in model.__mapper__.c.items()}

    fk_mapped_columns = {}

    # 找出被映射过的列author_id，并返回_RelationshipDeclared，而不是author_id的列类型; 后者是int导致不能区分是否外键
    # attrs: 同时包含实际字段author_id,映射字段author
    for model_field, v in model.__mapper__.attrs.items():
        if isinstance(v, _RelationshipDeclared):
            if v.direction.name == "MANYTOONE":
                db_column = list(v.local_columns)[0].name
                fk_model_field = db_map_model[db_column]
                fk_mapped_columns[fk_model_field] = v
                print(f"model field: {fk_model_field}, db field: {db_column} -> {model_field}")
            elif v.direction.name == "ONETOMANY":
                print("no need")
            else:  # MANYTOMANY
                print("handle MANYTOMANY by yourself")

    # c: 不包含外键的映射字段author
    for field_name, field in model.__mapper__.c.items():
        if field_name in fk_mapped_columns:
            yield field_name, fk_mapped_columns[field_name]
        else:
            yield field_name, field


def make_fake_data(model, size):
    df = pd.DataFrame()
    for field_name, field in required_fields(model):
        if isinstance(field, _RelationshipDeclared):
            obj = s.scalar(select(relation_rule(field)).order_by(func.random()).limit(1))
            df[field_name] = [obj.id] * size
        else:
            df[field_name] = [general_rule(field) for _ in range(size)]
    return df


if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent.absolute()))
    from sa.reflect import models
    from sa.session import Session

    with Session() as s:
        model = models["order"]
        df = make_fake_data(model, 10)
        st = insert(model).values(df.to_dict(orient="records"))
        s.execute(st)
        s.commit()
