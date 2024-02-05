import datetime
import time
from ipaddress import IPv4Network
from typing import Type, TypeVar

import factory
import factory.fuzzy
import faker
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import JSONB, INET, CIDR
from sqlalchemy.orm import *

model_factorys = {}
s = None
driver = "psycopg2"
Base = declarative_base()

local = "zh_CN"
faker.Faker.seed(int(time.time()))
fake_class = faker.Faker(locale=local)
unique_fake_class = faker.Faker(locale=local).unique

M = TypeVar("M", bound=DeclarativeBase)
F = Type[factory.alchemy.SQLAlchemyModelFactory]


def setup(session):
    global s, driver
    s = session
    driver = session.bind.driver


def fake(dtype, *args, **kwargs):
    return factory.LazyFunction(lambda: getattr(fake_class, dtype)(*args, **kwargs))


def fake_inet():
    return factory.LazyFunction(lambda: IPv4Network(fake_class.ipv4(network=True)))


def unique_fake(dtype, *args, **kwargs):
    return factory.LazyFunction(lambda: getattr(unique_fake_class, dtype)(*args, **kwargs))


def get_factory(model: Type[M]) -> F:
    if f := model_factorys.get(model):
        return f
    f = make_factory(model)
    model_factorys[model] = f
    return f


def make(model: Type[M], size: int = 1, **kwargs) -> M:
    if size == 1:
        return get_factory(model).create(**kwargs)
    return get_factory(model).create_batch(size, **kwargs)


class GetOrCreateSQLAlchemyFactory(factory.alchemy.SQLAlchemyModelFactory):
    # 外键要定义完整的引用
    @classmethod
    def _get_or_create(cls, model: Type[M], session: Session, args, kwargs) -> M:
        db_orm_column_map = {v.name: k for k, v in model.__mapper__.c.items()}

        unique_key = []
        for k, v in model.__mapper__.c.items():
            if v.unique or v.primary_key:
                unique_key.append(k)  # key: orm里的列，不是db列
        for k, v in kwargs.items():
            if k in unique_key and v is not None:
                if obj := session.scalars(select(model).filter_by(**{k: v})).first():
                    return obj

        unique_together = []
        for x in model.__table__.constraints:
            if isinstance(x, UniqueConstraint) and len(x.columns) > 1:
                # kwargs 都是orm里的列，这里也转换为orm里的列
                unique_together.append([db_orm_column_map[c.name] for c in x.columns])
        for x in unique_together:
            if set(kwargs) & set(x) == set(x):
                qs_kwargs = {k: v for k, v in kwargs.items() if k in set(x)}
                if obj := session.scalars(select(model).filter_by(**qs_kwargs)).first():
                    return obj

        if obj := session.scalars(select(model).filter_by(**kwargs)).first():
            return obj
        return cls._save(model, session, args, kwargs)


def make_factory(model_class: Type[M], **kwargs) -> F:
    factory_name = "%sFactory" % model_class.__name__
    base_class = GetOrCreateSQLAlchemyFactory
    db_orm_column_map = {v.name: k for k, v in model_class.__mapper__.c.items()}

    class Meta:
        model = model_class
        sqlalchemy_get_or_create = True
        sqlalchemy_session = s
        sqlalchemy_session_persistence = "commit"

    attrs = {}
    fk_mapped_columns = set()  # orm中已经被外键映射过的列
    for k, v in model_class.__mapper__.attrs.items():
        if isinstance(v, Relationship):
            if v.direction.name == "MANYTOONE":
                fk_mapped_columns.add(db_orm_column_map[list(v.local_columns)[0].name])
                attrs[k] = field_rule(v, Meta.sqlalchemy_session)
            elif v.direction.name == "ONETOMANY":
                # no need
                continue
            else:  # MANYTOMANY
                print("handle MANYTOMANY by yourself")
                continue
    # field_name: orm 的列，不是db列，例如 from在python是关键字，orm里属性列是from_，而不是db的from
    for field_name, field in model_class.__mapper__.c.items():
        if field_name in fk_mapped_columns:
            continue
        attrs[field_name] = field_rule(field, Meta.sqlalchemy_session)
    attrs["Meta"] = Meta
    attrs.update(specially_rule(model_class, Meta.sqlalchemy_session))
    attrs.update(kwargs)

    factory_class = type(factory.Factory).__new__(type(factory.Factory), factory_name, (base_class,), attrs)
    factory_class.__name__ = "%sFactory" % model_class.__name__
    factory_class.__doc__ = "Auto-generated factory for class %s" % model_class
    return factory_class


def field_rule(field: Column, session):
    if isinstance(field, Relationship):
        if list(field.remote_side)[0].table.name == list(field.local_columns)[0].table.name:
            if field.nullable:
                return
            raise ValueError("关联自己")
        target = field.mapper.class_manager.class_
        if obj := factory.LazyFunction(lambda: session.scalars(select(target).order_by(func.random())).first()):
            return obj
        return factory.SubFactory(get_factory(target))
    if field.autoincrement is True or field.name == "id":
        return
    if isinstance(field.type, (INET, CIDR)):
        if driver == "psycopg":
            return fake_inet()
        return factory.Faker("ipv4", network=True)
    if isinstance(field.type, Enum):
        return factory.fuzzy.FuzzyChoice(list(field.type.enum_class))
    if isinstance(field.type, Integer):
        if field.unique:
            return unique_fake("pyint")
        return factory.fuzzy.FuzzyInteger(1, high=10000)
    if isinstance(field.type, Float):
        return factory.fuzzy.FuzzyFloat(0.1, high=10)
    if isinstance(field.type, (DECIMAL, Numeric, NUMERIC)):
        return factory.fuzzy.FuzzyDecimal(1, high=1000, precision=6)
    if isinstance(field.type, Boolean):
        return factory.Faker("pybool")
    if isinstance(field.type, (JSON, JSONB)):
        return {}
    if isinstance(field.type, ARRAY):
        return []
    if isinstance(field.type, (VARCHAR, Text, String)):
        fake_fun = unique_fake if field.unique is True else fake
        if "email" == field.name:
            return unique_fake("email")
        if "username" == field.name:
            return unique_fake("user_name")
        if "phone" == field.name:
            return unique_fake("phone_number")
        if "first_name" == field.name:
            return fake("first_name")
        if "last_name" == field.name:
            return fake("last_name")
        if "password" == field.name:
            return fake("password")
        if "nick_name" == field.name:
            return fake("name")
        if "号" in field.name:
            return unique_fake("pyint")
        if "geoname" == field.name or "country_name" == field.name:
            return fake("country")
        if "name" in field.name:
            return fake_fun("name")
        if "desc" in field.name:
            return fake("sentence")
        return factory.LazyFunction(lambda: faker.Faker(local).text(max_nb_chars=10)[:-1])
    if isinstance(field.type, (DateTime, TIMESTAMP)):
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(hours=24 * 7)
        tomorrow = today + datetime.timedelta(hours=24 * 2)
        if field.name == "end_at":
            return fake("date_time_between_dates", datetime_start=today, datetime_end=tomorrow)
        return fake("date_time_between_dates", datetime_start=yesterday)
    if isinstance(field.type, Date):
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(hours=24 * 7)
        tomorrow = today + datetime.timedelta(hours=24 * 2)
        if field.name == "end_at":
            return fake("date_between_dates", datetime_start=today, date_end=tomorrow)
        return fake("date_between_dates", date_start=yesterday)
    if field.default:
        default = field.default
        if callable(default):
            return factory.LazyFunction(default)
        return default


def specially_rule(model, session):
    # 反射的表没有在orm里指定外键
    return {}
    fields_map = {}
    if model.__table__.name == "org":
        fields_map["name"] = fake("company")
    elif model.__table__.name == "task":
        fields_map["org"] = factory.SelfAttribute("dt_project.org")
    elif model.__table__.name == "dt_project":
        # 需要先在orm模型里加上many2many的定义
        # fields_map["controls"] = manytomany(target_table="dt_user", throught="dt_project_controls")
        ...
    return fields_map


def manytomany(table_name, field: str, session):
    @factory.post_generation
    def fill(self, create, extracted, **kwargs):
        if not create:
            return
        if extracted:
            qs = extracted
        else:
            # todo:
            qs = session.query()[:2]
        getattr(self, field).extend(qs)

    return fill
