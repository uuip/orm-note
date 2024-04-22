import datetime
import sys
import uuid
from pathlib import Path

import pandas as pd
from mimesis import Locale, Generic
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import *

sys.path.append(str(Path(__file__).parent.parent.absolute()))
from sa.reflect import models
from sa.session import Session

g = Generic(locale=Locale.ZH)

today = datetime.date.today()
yesterday = today - datetime.timedelta(hours=24 * 7)
tomorrow = today + datetime.timedelta(hours=24 * 2)


def is_unique(field):
    if field.unique:
        return field.unique
    for x in field.table.constraints:
        if isinstance(x, UniqueConstraint) and len(x.columns) == 1 and x.columns[0].name == field.name:
            return True


def field_rule(field: Column):
    if isinstance(field, Relationship):
        raise ValueError("Relationship")
    if field.autoincrement is True or field.name == "id":
        return text("default")
    if isinstance(field.type, Enum):
        return g.choice(items=list(field.type.enums))
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
            return str(uuid.uuid4())
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
            return str(g.numeric.integer_number(start=1))
        if "geoname" == field.name or "country_name" == field.name:
            return g.address.country_code()
        if "name" in field.name:
            return g.person.name()
        if "time" in field.name:
            t = g.datetime.datetime(start=yesterday.year)
            return t.isoformat()
        return g.text.word()
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


if __name__ == "__main__":
    with Session() as s:
        for t in ["geoip2_network"]:
            model = models[t]
            for x in range(1):
                df = pd.DataFrame()
                for field_name, field in model.__mapper__.c.items():
                    df[field_name] = [field_rule(field) for _ in range(100)]
                with s.begin():
                    st = insert(model).values(df.to_dict(orient="records"))
                    s.execute(st)
                t2 = datetime.datetime.now()
