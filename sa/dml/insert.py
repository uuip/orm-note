from sqlalchemy import *
from sqlalchemy.dialects.postgresql import insert

from sa.model.example import Author
from sa.session import Session


def insert_single():
    user_data = {"name": "myname1", "org": "some", "books": []}
    user = Author(**user_data)
    s.add(user)
    s.commit()
    print(user.id)


def bulk_insert():
    user_data = {"name": "111aaa", "org": "some", "books": []}
    user_data2 = {"name": "111bbb", "org": "some", "books": []}
    user1 = Author(**user_data)
    user2 = Author(**user_data2)
    # insertmanyvalues
    s.add_all([user1, user2])
    s.commit()

    # returning 触发 insertmanyvalues 优化；否则executemany; psycopg的executemany很慢
    # https://docs.sqlalchemy.org/en/20/core/connections.html#insert-many-values-behavior-for-insert-statements
    s.execute(insert(Author).returning(Author.id), [user_data, user_data2])
    s.commit()

    # 不需 returning，一条语句 INSERT ... VALUES
    # https://docs.sqlalchemy.org/en/20/core/dml.html#sqlalchemy.sql.expression.Insert.values
    st = (
        insert(Author)
        .values(
            [
                {Author.name: "111ccc", "org": "2022-1-1", "books": []},
                {Author.name: "111ddd", "org": "2022-1-1", "books": []},
            ]
        )
        .on_conflict_do_nothing()
    )
    s.execute(st)
    s.commit()

    # 不丢弃None字段
    # s.execute(insert(Author).execution_options(render_nulls=True), [user_data, user_data2])

    # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#updating-using-the-excluded-insert-values


if __name__ == "__main__":
    with Session() as s:
        bulk_insert()
