# https://docs.sqlalchemy.org/en/20/orm/mapped_sql_expr.html#sql-expressions-as-mapped-attributes
# https://docs.sqlalchemy.org/en/20/orm/mapped_attributes.html#synonyms

from sqlalchemy import *
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import query_expression, with_expression

from sa.model import Base
from sa.session import engine, SessionMaker

if __name__ == "__main__":

    class Author(Base):
        __tablename__ = "author"

        id = Column(BigInteger, Identity(), primary_key=True)
        name = Column(Text, unique=True)
        wage = Column(Integer)
        total_income2 = query_expression()

        # hybrid_property: 实例访问时返回 Python 计算值，类访问时返回 SQL 表达式（可用于查询）
        @hybrid_property
        def total_income(self):
            return self.wage * 12

        @total_income.inplace.expression
        @classmethod
        def _total_income_expr(cls):
            return type_coerce(cls.wage * 12, Integer)

    Base.metadata.create_all(bind=engine)
    with SessionMaker() as s:
        # ok
        obj: Author = s.scalars(select(Author).where(Author.total_income > 1500)).first()
        if obj:
            print(obj.name)

        # 不正确用法：https://docs.sqlalchemy.org/en/20/orm/queryguide/columns.html#orm-queryguide-with-expression
        # 不能在 where() 中直接使用 query_expression 属性，因为此时该属性还未被 with_expression 绑定到表达式
        obj: Author = s.scalars(
            select(Author)
            .options(with_expression(Author.total_income2, Author.wage * 12))
            .where(Author.total_income2 > 1500)
        ).first()
        if obj:
            print(obj.name)

        # ok
        # 正确用法：将表达式提取到变量中，同时用于 with_expression 和 where
        total_in_expr = Author.wage * 12
        obj: Author = s.scalars(
            select(Author).options(with_expression(Author.total_income2, total_in_expr)).where(total_in_expr > 1500)
        ).first()
        if obj:
            print(obj.name)
