from alembic import context
from alembic.ddl import impl
from alembic.ddl.base import ColumnComment, IdentityColumnDefault, RenameTable
from alembic.ddl.postgresql import (
    PostgresqlColumnType,
    visit_column_type,
    visit_rename_table,
    visit_column_comment,
    visit_identity_column,
    )
from alembic.util.sqla_compat import compiles
from sqlalchemy.dialects import registry


def register_kingbase_dialect():
    """Register KingBase as a PostgreSQL-compatible dialect for alembic.

    KingBase inherits PGDialect but uses dialect name "kingbase",
    while alembic dispatches by name string, not class hierarchy.
    """
    impl._impls["kingbase"] = impl._impls["postgresql"]
    registry.register("kingbase", "app.utils.sqlalchemy_kingbase_dialect", "KingBaseDialect")
    compiles(PostgresqlColumnType, "kingbase")(visit_column_type)
    compiles(RenameTable, "kingbase")(visit_rename_table)
    compiles(ColumnComment, "kingbase")(visit_column_comment)
    compiles(IdentityColumnDefault, "kingbase")(visit_identity_column)

register_kingbase_dialect()

