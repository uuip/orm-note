"""演示 ORM 属性名与数据库列名的区别。

当 ORM 属性名与数据库列名不同（例如 name 对应 username）时，
可通过映射信息分别取得两类名称，以及对应的实例值。
"""

from sqlalchemy import BigInteger, Identity, Text, create_engine, inspect, select
from sqlalchemy.orm import mapped_column, sessionmaker

from config import settings
from sa.model import Base


def get_field_values(obj):
    """按 ORM 属性名取得实例值。

    返回示例：{'name': 'John', 'id': 1}
    """
    return {field: getattr(obj, field) for field, _ in obj.__mapper__.c.items()}


def get_column_values(obj):
    """按数据库列名取得实例值。

    返回示例：{'username': 'John', 'id': 1}
    """
    return {col.name: getattr(obj, field) for field, col in obj.__mapper__.c.items()}


class Author(Base):
    __tablename__ = "user"

    id = mapped_column(BigInteger, Identity(), primary_key=True)
    name = mapped_column("username", Text, unique=True, nullable=False)  # ORM 属性名为 name，数据库列名为 username。


def inspect_class_mapping():
    """从模型类检查 ORM 属性名与数据库列名。"""
    mapper = inspect(Author)

    # ORM 属性名，即 Python 对象的属性。
    fields = list(mapper.c.keys())
    print(f"ORM fields: {fields}")  # ['id', 'name']

    # 数据库列名。
    db_columns = [col.name for col in mapper.c]
    print(f"DB columns: {db_columns}")  # ['id', 'username']

    # ORM 属性名到数据库列名的映射。
    field_to_column = {key: col.name for key, col in mapper.c.items()}
    print(f"Field to column mapping: {field_to_column}")  # {'id': 'id', 'name': 'username'}


def inspect_instance_values():
    """分别按 ORM 属性名与数据库列名取得实例值。"""
    engine = create_engine(settings.db_url, echo=False)
    SessionMaker = sessionmaker(bind=engine)

    with SessionMaker() as session:
        stmt = select(Author).where(Author.id == 1)
        author = session.scalar(stmt)

        if author:
            # 按 ORM 属性名组织实例值。
            field_values = get_field_values(author)
            print(f"Field values: {field_values}")  # {'id': 1, 'name': 'John'}

            # 按数据库列名组织实例值。
            column_values = get_column_values(author)
            print(f"Column values: {column_values}")  # {'id': 1, 'username': 'John'}


if __name__ == "__main__":
    print("=== Class-level inspection ===")
    inspect_class_mapping()

    print("\n=== Instance-level inspection ===")
    inspect_instance_values()
