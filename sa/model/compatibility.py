import uuid

from sqlalchemy import *
from sqlalchemy.orm import *

from sa.types.custom_types import UniversalJSON, LongText


class Base(DeclarativeBase):
    pass


class Compatibility(Base):
    """MySQL vs PostgreSQL type compatibility reference"""

    __tablename__ = "db_compare"
    # mysql index requires prefix length，For indexed text fields, use String(N) instead
    __table_args__ = (Index("idx_db_compare_f7", "f7", mysql_length=768),)

    id = mapped_column(BigInteger, Identity(), primary_key=True)

    # # DateTime: PG→TIMESTAMP WITHOUT TIME ZONE, MySQL→DATETIME. timezone only affects PG (WITH/WITHOUT TIME ZONE)
    created_at = Column(DateTime(timezone=False))

    # server_default: use text('false') or "0", both work but text("false") is better compatible with alembic
    f1 = mapped_column(Boolean, server_default=text("false"))

    # Numeric: always specify precision for consistency
    f2 = mapped_column(Numeric(5, 2))

    # JSON: pg uses JSONB (binary, faster), mysql uses JSON (text-based)
    f3 = mapped_column(UniversalJSON)

    # Use LongText (pg: TEXT, mysql: LONGTEXT) for large text
    f4 = mapped_column(LongText)

    # UUID: pg has native UUID type, mysql stores as CHAR(32)
    f5: Mapped[uuid.UUID] = mapped_column(Uuid, default=uuid.uuid7)
    f6: Mapped[str] = mapped_column(Uuid(as_uuid=False), default=lambda: str(uuid.uuid7()))

    # String: VARCHAR needs length limit, required by mysql max 16339 utf8mb4
    f7 = mapped_column(String(1000))
