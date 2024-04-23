from sqlalchemy import *
from sqlalchemy.orm import *

from sa.types.custom_types import UniversalJSON, LongText, StringUUID


class Base(DeclarativeBase):
    pass


class Compatibility(Base):
    """MySQL vs PostgreSQL type compatibility reference"""

    __tablename__ = "db_compare"
    # mysql index requires prefix length，For indexed text fields, use String(N) instead
    __table_args__ = (Index("idx_db_compare_f6", "f6", mysql_length=768),)

    id = mapped_column(BigInteger, Identity(), primary_key=True)

    # server_default: use text('false') or text('0'), both work
    f1 = mapped_column(Boolean, server_default=text("false"))

    # DateTime: use timezone=False to avoid TIMESTAMP, stores as DATETIME in both
    created_at = Column(DateTime(timezone=False))

    # Numeric: always specify precision for consistency
    f2 = mapped_column(Numeric(5, 2))

    # String: VARCHAR needs length limit, required by mysql max 16339 utf8mb4
    f6 = mapped_column(String(1000))

    # JSON: pg uses JSONB (binary, faster), mysql uses JSON (text-based)
    f3 = mapped_column(UniversalJSON)

    # Use LongText (pg: TEXT, mysql: LONGTEXT) for large text
    f4 = mapped_column(LongText)

    # UUID: pg has native UUID type, mysql stores as CHAR(36)
    f5 = mapped_column(StringUUID)
