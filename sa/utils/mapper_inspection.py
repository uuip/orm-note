"""Demonstrate the difference between ORM field names and database column names.

When ORM field name differs from database column name (e.g., name -> username),
SQLAlchemy provides ways to access both at class level and instance level.
"""

from sqlalchemy import BigInteger, Identity, Text, create_engine, inspect, select
from sqlalchemy.orm import mapped_column, sessionmaker

from conf import settings
from sa.model import Base


def get_field_values(obj):
    """Get model instance values mapped by ORM field names.

    Returns dict like: {'name': 'John', 'id': 1}
    """
    return {field: getattr(obj, field) for field, _ in obj.__mapper__.c.items()}


def get_column_values(obj):
    """Get model instance values mapped by database column names.

    Returns dict like: {'username': 'John', 'id': 1}
    """
    return {col.name: getattr(obj, field) for field, col in obj.__mapper__.c.items()}


class Author(Base):
    __tablename__ = "user"

    id = mapped_column(BigInteger, Identity(), primary_key=True)
    name = mapped_column("username", Text, unique=True, nullable=False)  # field: name, column: username


def inspect_class_mapping():
    """Inspect ORM field names and database column names from the class."""
    mapper = inspect(Author)

    # ORM field names (Python attributes)
    fields = list(mapper.c.keys())
    print(f"ORM fields: {fields}")  # ['id', 'name']

    # Database column names
    db_columns = [col.name for col in mapper.c]
    print(f"DB columns: {db_columns}")  # ['id', 'username']

    # Field -> Column mapping
    field_to_column = {key: col.name for key, col in mapper.c.items()}
    print(f"Field to column mapping: {field_to_column}")  # {'id': 'id', 'name': 'username'}


def inspect_instance_values():
    """Get instance values mapped by field names or column names."""
    engine = create_engine(settings.db_url, echo=False)
    SessionMaker = sessionmaker(bind=engine)

    with SessionMaker() as session:
        stmt = select(Author).where(Author.id == 1)
        author = session.scalar(stmt)

        if author:
            # Values mapped by ORM field names
            field_values = get_field_values(author)
            print(f"Field values: {field_values}")  # {'id': 1, 'name': 'John'}

            # Values mapped by database column names
            column_values = get_column_values(author)
            print(f"Column values: {column_values}")  # {'id': 1, 'username': 'John'}


if __name__ == "__main__":
    print("=== Class-level inspection ===")
    inspect_class_mapping()

    print("\n=== Instance-level inspection ===")
    inspect_instance_values()
