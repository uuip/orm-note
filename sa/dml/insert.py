from sqlalchemy import *
from sqlalchemy.dialects.postgresql import insert

from fakedata.rule import general_rule
from sa.model.example import Author
from sa.session import Session


def make_author():
    return {"name": general_rule(Author.name), "org": general_rule(Author.org), "books": []}


def insert_single():
    user_data = make_author()
    user = Author(**user_data)
    s.add(user)
    s.commit()
    print(user.id)


def bulk_insert():
    user1 = Author(**make_author())
    user2 = Author(**make_author())
    # insertmanyvalues
    s.add_all([user1, user2])
    s.commit()

    # 下述2种insert，插入关联对象xxx时，其key应当使用 xxx_id,而不是映射的xxx

    # returning 触发 insertmanyvalues 优化, key只能是字符串；否则executemany; psycopg2的executemany很慢, psycopg的正常
    # https://docs.sqlalchemy.org/en/20/core/connections.html#insert-many-values-behavior-for-insert-statements
    st = insert(Author).returning(Author.id)
    s.execute(
        st,
        [
            make_author(),
            make_author(),
        ],
    )
    s.commit()

    # 不需 returning，一条语句 INSERT ... VALUES; key可以是Author.xxx
    # https://docs.sqlalchemy.org/en/20/core/dml.html#sqlalchemy.sql.expression.Insert.values
    st = insert(Author).values(
        [
            make_author(),
            make_author(),
        ]
    )
    s.execute(st)
    s.commit()

    # 不丢弃None字段
    # s.execute(insert(Author).execution_options(render_nulls=True), [user_data, user_data2])

    # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#updating-using-the-excluded-insert-values


if __name__ == "__main__":
    with Session() as s:
        bulk_insert()
