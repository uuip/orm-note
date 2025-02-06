import pkgutil
from importlib import import_module
from pathlib import Path

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from sa.model import Base
from sa.session import engine

if __name__ == "__main__":
    path = str(Path(__file__).parent / "model")
    for module_finder, name, ispkg in pkgutil.walk_packages([path], "sa.model."):
        module = import_module(name)
    allmodels = [x for x in Base.__subclasses__() if getattr(x, "__abstract__", False) is not True]
    # Base.metadata.drop_all(bind=db)
    Base.metadata.create_all(bind=engine)

    print(CreateTable(allmodels[0].__table__).compile(dialect=postgresql.dialect()))
