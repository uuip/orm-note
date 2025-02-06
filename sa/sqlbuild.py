from sqlalchemy import select, column, table, text
from sqlalchemy.engine.default import DefaultDialect
# from sqlalchemy.dialects.mysql import mysqlconnector
# dialect = mysqlconnector.dialect


dialect = DefaultDialect(paramstyle="qmark")

t = table("user")
stmt = select(text("*")).select_from(t).where(column("address") == "55").limit(10)
compiler = stmt.compile(dialect=dialect)
print(compiler.string, [compiler.params[x] for x in compiler.positiontup])


from pypika import PostgreSQLQuery, Query, Table, Field
from pypika.terms import ValueWrapper


def generate_sql(table_name, out_cols, clause_col, clause_val):
    t = Table(table_name)
    if clause_col:
        q = PostgreSQLQuery.from_(t).select(*out_cols).where(Field(clause_col) == ValueWrapper(clause_val))
    else:
        q = Query.from_(t).select(*out_cols)
    return q.limit(100).get_sql()
