import uuid
from typing import Any

import sqlalchemy as sa
from sqlalchemy import CHAR, TypeDecorator, Text
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql.type_api import TypeEngine


# 推荐用法（新模型的跨数据库 UUID）：优先内置 sqlalchemy.Uuid，参考 model/compatibility.py。
# 只有需要保留既有 CHAR(36) 存储格式等定制行为时，才使用这里的自定义类型。
class StringUUID(TypeDecorator[uuid.UUID | str | None]):
    impl = CHAR
    cache_ok = True

    def process_bind_param(self, value: uuid.UUID | str | None, dialect: Dialect) -> uuid.UUID | str | None:
        if value is None:
            return value
        if dialect.name == "postgresql":
            return value
        elif dialect.name == "mysql":
            return str(value)
        else:
            if isinstance(value, uuid.UUID):
                return value.hex
            return value

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_result_value(self, value: uuid.UUID | str | None, dialect: Dialect) -> uuid.UUID | str | None:
        if value is None:
            return value
        if dialect.name == "postgresql":
            return value
        else:
            if isinstance(value, str):
                return uuid.UUID(value)
            return value


# 推荐用法：只切换方言类型、无需转换值时，使用 with_variant() 即可。
LongText = Text().with_variant(LONGTEXT(), "mysql")


class UniversalJSON(TypeDecorator[dict | list | None]):
    # 保留外层类型：字段有默认值时，ORM 忽略 None 并采用默认值。
    # 无默认值或 Core 显式绑定 None 时仍写 JSON null；SQL NULL 使用 sqlalchemy.null()。
    impl = sa.JSON().with_variant(JSONB(), "postgresql")
    cache_ok = True
