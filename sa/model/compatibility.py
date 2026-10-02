import uuid

from sqlalchemy import *
from sqlalchemy.orm import *

from sa.types.custom_types import UniversalJSON, LongText


class Base(DeclarativeBase):
    pass


class Compatibility(Base):
    """MySQL 与 PostgreSQL 的类型兼容性参考。"""

    __tablename__ = "db_compare"
    # MySQL 的 TEXT 索引需要前缀；这里的 VARCHAR 前缀用于满足 utf8mb4 的索引长度限制。
    # 需要建索引的文本字段优先使用 String(N)。
    __table_args__ = (Index("idx_db_compare_f7", "f7", mysql_length=768),)

    id = mapped_column(BigInteger, Identity(), primary_key=True)

    # DateTime：PostgreSQL 对应 TIMESTAMP WITHOUT TIME ZONE，MySQL 对应 DATETIME；timezone 参数在 PostgreSQL 下区分是否带时区。
    created_at = Column(DateTime(timezone=False))

    # 布尔服务端默认值可用 text("false") 或字符串 "0"；text("false") 与 Alembic 的兼容性更好。
    f1 = mapped_column(Boolean, server_default=text("false"))

    # Numeric 显式指定精度与小数位，便于保持两种数据库的字段定义一致。
    f2 = mapped_column(Numeric(5, 2))

    # UniversalJSON 在 PostgreSQL 下使用 JSONB，在 MySQL 下使用 JSON；两者均采用二进制存储格式。
    f3 = mapped_column(UniversalJSON)

    # 大文本使用 LongText：PostgreSQL 对应 TEXT，MySQL 对应 LONGTEXT。
    f4 = mapped_column(LongText)

    # 推荐用法：使用内置 Uuid；PostgreSQL 对应原生 UUID，MySQL 对应 CHAR(32)。
    f5: Mapped[uuid.UUID] = mapped_column(Uuid, default=uuid.uuid7)
    f6: Mapped[str] = mapped_column(Uuid(as_uuid=False), default=lambda: str(uuid.uuid7()))

    # MySQL 的 VARCHAR 必须指定长度；允许的最大长度取决于字符集和整行大小。
    # 原笔记记录的 utf8mb4 上限为 16339，保留该值供原场景参考。
    f7 = mapped_column(String(1000))
