from sqlalchemy.ext.automap import automap_base

from sa.session import engine

AutoBase = automap_base()

# class Tx(AutoBase):
#     __tablename__ = "transactions"

AutoBase.prepare(autoload_with=engine)
models = AutoBase.classes


def justtable():
    from sqlalchemy import MetaData

    metadata = MetaData()
    metadata.reflect(bind=engine)
    model = metadata.tables["users"]


def astable():
    from sqlalchemy import MetaData, Table
    from sqlalchemy.orm import DeclarativeBase

    class Base(DeclarativeBase):
        pass

    metadata = MetaData()

    class User(Base):
        __table__ = Table("users", metadata, autoload_with=engine)
