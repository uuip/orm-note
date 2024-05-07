import datetime
import random
import sys
from functools import lru_cache
from pathlib import Path

import pandas as pd
from mimesis import Locale, Generic
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import *

import faker

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


def field_rule(field: Column):
    if isinstance(field, Relationship):
        raise ValueError("Relationship")
    if field.autoincrement is True or field.name == "id":
        return text("default")
    if isinstance(field.type, Enum):
        return random.choice(list(field.type.enums))
    if isinstance(field.type, (JSON, JSONB)):
        return {}
    if isinstance(field.type, ARRAY):
        return []

    if isinstance(field.type, (INET, CIDR)):
        return g.internet.ip_v4_object()
    if isinstance(field.type, Integer):
        return g.numeric.integer_number(start=1)
    if isinstance(field.type, Float):
        return g.numeric.float_number()
    if isinstance(field.type, Boolean):
        return g.development.boolean()
    if isinstance(field.type, (VARCHAR, Text, String)):
        if field.primary_key or is_unique(field):
            return faker.unique_username()
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
        return faker.cn_words(max_length=10)
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

    if field.default:
        default = field.default
        if callable(default):
            return default()
        return default
    raise TypeError(f"no rule for this type: {field.type}")


def make(model):
    return model(**{field_name: field_rule(field) for field_name, field in model.__mapper__.c.items()})


if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent.absolute()))
    from sa.reflect import models
    from sa.session import Session

    with Session() as s:
        for t in ["author"]:
            model = models[t]
            for x in range(1):
                df = pd.DataFrame()
                for field_name, field in model.__mapper__.c.items():
                    df[field_name] = [field_rule(field) for _ in range(100)]
                with s.begin():
                    st = insert(model).values(df.to_dict(orient="records"))
                    s.execute(st)
                t2 = datetime.datetime.now()
