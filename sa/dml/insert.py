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
    user = Author(**user_data)
    s.add_all([user])
    s.commit()

    # # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#updating-using-the-excluded-insert-values
    # returning 触发 use_insertmanyvalues 优化；否则会将params拆开，每个执行insert
    s.execute(insert(Author).on_conflict_do_nothing().returning(Author.id), [user_data, user_data2])
    s.commit()

    # 不需 returning，values是一个整体
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


if __name__ == "__main__":
    with Session() as s:
        insert_single()
        bulk_insert()
