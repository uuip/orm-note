from sqlalchemy import *
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from sa.model.usercase import User

s: Session
user_data = {"name": "myname", "org": "some", "books": []}


def insert_single():
    user = User(**user_data)
    s.add(user)
    s.commit()
    print(user.id)


def bulk_insert():
    user = User(**user_data)
    s.add_all([user])
    s.commit()

    # # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#updating-using-the-excluded-insert-values
    # returning 触发 use_insertmanyvalues 优化；否则会将params拆开，每个执行insert
    s.execute(insert(User).on_conflict_do_nothing().returning(User.id), [user_data])
    s.commit()

    # 不需 returning，values是一个整体
    st = (
        insert(User)
        .values(
            [
                {User.name: "cccc", "org": "2022-1-1", "books": []},
            ]
        )
        .on_conflict_do_nothing()
    )
    s.execute(st)
    s.commit()
