from sqlalchemy import MetaData, Table
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import DeclarativeBase


# 推荐用法（自动生成 ORM 类与关系）：automap；只需少数已知表时优先定点反射。
def automap_models(engine):
    auto_base = automap_base()
    # 如需手动声明自动映射模型，应放在 prepare() 之前。
    # class Tx(auto_base):
    #     __tablename__ = "transactions"
    auto_base.prepare(autoload_with=engine)
    return auto_base.classes


# 推荐用法（读取多张表的 Core 元数据）：MetaData.reflect()；只需部分表时用 only 限定范围。
def justtable(engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return metadata.tables["users"]


# 推荐用法（已知单表并自行定义 ORM 类）：Table(..., autoload_with=engine)，避免反射整个数据库。
def astable(engine):
    class Base(DeclarativeBase):
        pass

    class User(Base):
        __table__ = Table("users", Base.metadata, autoload_with=engine)

    return User


if __name__ == "__main__":
    from sa.session import engine

    print(automap_models(engine).keys())
    print(justtable(engine))
    print(astable(engine))
