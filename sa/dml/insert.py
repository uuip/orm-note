import uuid

from sqlalchemy import *
from sqlalchemy.dialects.postgresql import insert

from sa.model.example import Author


def make_author():
    return {"name": uuid.uuid7(), "org": uuid.uuid7(), "books": []}


def insert_single(session):
    user_data = make_author()
    user = Author(**user_data)
    # 推荐用法（单个实体或包含关系对象）：add()，由 ORM 工作单元管理写入。
    session.add(user)
    session.commit()
    print(user.id)

    # PostgreSQL UPSERT：复用唯一的 name，更新前面插入的记录。
    st = insert(Author).values({**user_data, "org": "updated org"})
    st = st.on_conflict_do_update(
        index_elements=[Author.name],
        set_={"org": st.excluded.org},
    )
    session.execute(st)
    session.commit()


def bulk_insert(session):
    user1 = Author(**make_author())
    user2 = Author(**make_author())
    # 推荐用法（多个实体或关系对象）：add_all()；符合方言条件时可使用 insertmanyvalues 优化。
    session.add_all([user1, user2])
    session.commit()

    # 下述2种insert，插入关联对象xxx时，其key应当使用 xxx_id,而不是映射的xxx

    # 推荐用法（批量字典写入）：用 returning 触发 insertmanyvalues 优化。
    # returning 触发 insertmanyvalues 优化, key只能是字符串；否则executemany; psycopg2的executemany很慢, psycopg的正常
    # 参考资料：https://docs.sqlalchemy.org/en/20/core/connections.html#insert-many-values-behavior-for-insert-statements
    st = insert(Author).returning(Author.id)
    session.execute(
        st,
        [
            make_author(),
            make_author(),
        ],
    )
    session.commit()

    # 条件用法（显式多行 VALUES 或逐行 SQL 表达式）：values([...])；应控制批次大小。
    # 不需要 returning 也可生成一条多行 INSERT；键可使用 Author.xxx。
    # 参考资料：https://docs.sqlalchemy.org/en/20/core/dml.html#sqlalchemy.sql.expression.Insert.values
    st = insert(Author).values(
        [
            make_author(),
            make_author(),
        ]
    )
    session.execute(st)
    session.commit()

    # 不丢弃None字段
    # session.execute(insert(Author).execution_options(render_nulls=True), [user_data, user_data2])

    # 参考资料：https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#updating-using-the-excluded-insert-values


if __name__ == "__main__":
    from sa.session import SessionMaker

    with SessionMaker() as session:
        bulk_insert(session)
