from sqlalchemy import select, column, table, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.ddl import CreateTable
from sa.model.example import Author


def compile_examples():
    # from sqlalchemy.dialects.mysql import mysqlconnector
    # dialect = mysqlconnector.dialect
    # dialect = psycopg.dialect(paramstyle="format") #%s

    # 推荐用法（已有 SQLAlchemy 模型或语句）：使用 compile(dialect=目标方言)，避免另写一套 SQL。
    # 下面的通用方言仅演示问号占位符；实际 PostgreSQL/MySQL SQL 应使用对应的方言。
    dialect = DefaultDialect(paramstyle="qmark")

    t = table("user")
    stmt = select(text("*")).select_from(t).where(column("address") == "55").limit(10)
    compiler = stmt.compile(dialect=dialect)
    print(compiler.string, [compiler.params[x] for x in compiler.positiontup])

    # psycopg.dialect(paramstyle="format")
    print(CreateTable(Author.__table__).compile(dialect=postgresql.dialect()))


# 备选用法：不使用 SQLAlchemy 模型、只需要独立 SQL 构造器时可选择 PyPika。
from pypika import PostgreSQLQuery, Query, Table, Field
from pypika.terms import ValueWrapper


def generate_sql(table_name, out_cols, clause_col, clause_val):
    t = Table(table_name)
    if clause_col:
        q = PostgreSQLQuery.from_(t).select(*out_cols).where(Field(clause_col) == ValueWrapper(clause_val))
    else:
        q = Query.from_(t).select(*out_cols)
    return q.limit(100).get_sql()


if __name__ == "__main__":
    compile_examples()
