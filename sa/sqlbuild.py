from sqlalchemy import select, column, table, text
from sqlalchemy.engine.default import DefaultDialect

dialect = DefaultDialect(paramstyle="qmark")

t = table("user")
stmt = select(text("*")).select_from(t).where(column("address") == "55").limit(10)
compiler = stmt.compile(dialect=dialect)
print(compiler.string, [compiler.params[x] for x in compiler.positiontup])
