import uuid
from typing import Any

import sqlalchemy as sa
from sqlalchemy import CHAR, TEXT, TypeDecorator
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql.type_api import TypeEngine


class StringUUID(TypeDecorator[uuid.UUID | str | None]):
    impl = CHAR
    cache_ok = True

    def process_bind_param(
        self, value: uuid.UUID | str | None, dialect: Dialect
    ) -> uuid.UUID | str | None:
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

    def process_result_value(
        self, value: uuid.UUID | str | None, dialect: Dialect
    ) -> uuid.UUID | str | None:
        if value is None:
            return value
        if dialect.name == "postgresql":
            return value
        else:
            if isinstance(value, str):
                return uuid.UUID(value)
            return value


class LongText(TypeDecorator[str | None]):
    impl = TEXT
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name == "mysql":
            return dialect.type_descriptor(LONGTEXT())
        else:
            return dialect.type_descriptor(TEXT())


class UniversalJSON(TypeDecorator[dict | list | None]):
    impl = sa.JSON
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        else:
            return dialect.type_descriptor(sa.JSON())
