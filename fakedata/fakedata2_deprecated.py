import datetime
import decimal
import random
import time
import traceback
import uuid
from ipaddress import IPv4Network

import faker
import pandas as pd
from joblib import Parallel, delayed
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import *

from sa.reflect import models
from sa.session import db

local = "zh_CN"
faker.Faker.seed(int(time.time()))
fake = faker.Faker(locale=local)
unique_fake = faker.Faker(locale=local).unique

today = datetime.date.today()
yesterday = today - datetime.timedelta(hours=24 * 7)
tomorrow = today + datetime.timedelta(hours=24 * 2)


def field_rule(field: Column):
    if isinstance(field, Relationship):
        raise ValueError("Relationship")
    if field.autoincrement is True or field.name == "id":
        return
    if isinstance(field.type, Enum):
        return random.choice(list(field.type.enum_class))
    if isinstance(field.type, (JSON, JSONB)):
        return {}
    if isinstance(field.type, ARRAY):
        return []

    if field.unique:
        mode = unique_fake
    else:
        mode = fake
    if isinstance(field.type, (INET, CIDR)):
        return IPv4Network(mode.ipv4(network=True))
    if isinstance(field.type, Integer):
        return mode.pyint()
    if isinstance(field.type, Float):
        return mode.pyfloat()
    if isinstance(field.type, Boolean):
        return mode.pybool()
    if isinstance(field.type, (VARCHAR, Text, String)):
        if field.primary_key or field.unique:
            return str(uuid.uuid4())
        if "email" == field.name:
            return mode.email()
        if "username" == field.name:
            return mode.user_name()
        if "phone" == field.name:
            return mode.phone_number()
        if "first_name" == field.name:
            return mode.first_name()
        if "last_name" == field.name:
            return mode.last_name()
        if "password" == field.name:
            return mode.password()
        if "nick_name" == field.name:
            return mode.name()
        if "号" in field.name:
            return str(mode.pyint())
        if "geoname" == field.name or "country_name" == field.name:
            return mode.country()
        if "name" in field.name:
            return mode.name()
        if "desc" in field.name:
            return mode.sentence()
        if "time" in field.name:
            t = mode.date_time_between_dates(datetime_start=yesterday)
            return t.isoformat()
        return faker.Faker(local).text(max_nb_chars=5)[:-1]
    if isinstance(field.type, (DateTime, TIMESTAMP)):
        if field.name == "end_at":
            return mode.date_time_between_dates(datetime_start=today, datetime_end=tomorrow)
        return mode.date_time_between_dates(datetime_start=yesterday)
    if isinstance(field.type, Date):
        if field.name == "end_at":
            return mode.date_between_dates(datetime_start=today, date_end=tomorrow)
        return mode.date_between_dates(date_start=yesterday)
    if isinstance(field.type, (DECIMAL, Numeric, NUMERIC)):
        return decimal.Decimal(str(mode.pyfloat()))

    if field.default:
        default = field.default
        if callable(default):
            return default()
        return default
    raise TypeError(f"no rule for this type: {field.type}")


if __name__ == "__main__":
    tablename = "transactions_pool"
    model = models[tablename]
    try:
        for x in range(1):
            t1 = datetime.datetime.now()
            df = pd.DataFrame()
            for field_name, field in model.__mapper__.c.items():
                if isinstance(field.type, (VARCHAR, Text, String)):
                    df[field_name] = Parallel(n_jobs=-1)(delayed(field_rule)(field) for _ in range(2000))
                else:
                    df[field_name] = [field_rule(field) for _ in range(2000)]
            t2 = datetime.datetime.now()
            t = (t2 - t1).total_seconds()
            print(f"generate time: {field_name:<15} {t}")
            df.to_sql(name=tablename, con=db, if_exists="append", index=False, chunksize=2000, method="multi")
    except:
        traceback.print_exc()
