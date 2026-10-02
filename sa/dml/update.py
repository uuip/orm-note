from sqlalchemy import *

from sa.model.example import Author, Order, ShipTransfer, ShipTransfer2


def update_single(session):
    obj = session.scalar(select(Author).limit(1))
    # 推荐用法（修改已加载的单个实体）：直接赋值，由 ORM 跟踪修改。
    obj.org = "other"
    session.commit()

    # 推荐用法（按条件统一更新）：execute(update(...).where(...).values(...))，无需逐个加载对象。
    st = update(Author).where(Author.org == "other").values({Author.org: "other org"})
    st = update(Author).where(Author.org == "other").values(org="other org")
    session.execute(st)
    session.commit()
    print(obj.org)

    obj = session.scalar(select(Author).limit(1))
    # 旧式 Query 写法仍可使用；新笔记优先使用上面的 execute(update(...))。
    session.query(Author).filter(Author.id == obj.id).update({Author.org: "dskdkdkd"})
    session.query(Author).filter(Author.id == obj.id).update({"org": "dskdkdkd"})
    session.commit()


def bulk_update(session, engine):
    for obj in session.scalars(select(Author)):
        obj.nickname = "sometext"
    # executemany执行
    session.commit()

    # 推荐用法（已知各行主键且值不同）：批量按主键更新；字典需包含完整主键，键只能是字符串。
    # 由 executemany 执行，不支持 returning。
    # 参考资料：https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#orm-bulk-update-by-primary-key
    obj = session.scalar(select(Author).limit(1))
    session.execute(update(Author), [{"id": obj.id, "org": "bbbb"}, {"id": 2, "org": "cccc"}])
    session.commit()

    # 推荐用法（按自定义条件批量更新）：通过 Core 连接执行，绕开 ORM 自动按主键更新。
    # WHERE 与更新值绑定到字典的键，由 executemany 执行。
    # 参考资料：https://docs.sqlalchemy.org/en/20/tutorial/data_update.html#the-update-sql-expression-construct
    # where条件bindparam, 其余字段与orm相同
    # 参考资料：https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#disabling-bulk-orm-update-by-primary-key-for-an-update-statement-with-multiple-parameter-sets
    to_update = [
        {"v_name": "aaaa", "nickname": "bindparambbbb"},
        {"v_name": "aaaa1", "nickname": "bindparambbbb"},
    ]
    st = update(Author).where(Author.nickname == bindparam("v_name"))
    with engine.begin() as conn:
        conn.execute(st, to_update)
    # 连接与事务由 Session 管理，不在这里单独关闭连接。
    conn = session.connection()
    conn.execute(st, to_update)
    session.commit()


def update_from(session):
    # 参考资料：https://docs.sqlalchemy.org/en/20/tutorial/data_update.html#update-from
    st = (
        update(ShipTransfer)
        .where(ShipTransfer.token_id == ShipTransfer2.token_id)
        .values({ShipTransfer.from_: ShipTransfer2.to})
    )
    session.execute(st)
    session.commit()

    # UPDATE..FROM (VALUES ...), 所有参数一次构造为VALUES
    vst = values(
        column("org", Text),
        column("nickname", Text),
        name="vst",
    ).data([["dskdkdkd", "aaaa"], ["dskdkdkd22", "aaaa"]])
    st = update(Author).where(Author.org == vst.c.org).values({Author.nickname: vst.c.nickname})
    session.execute(st)
    session.commit()


def update_with_case_value(session):
    st = (
        update(Order)
        .where(Order.price == 191)
        .values(
            quantity=case(
                (Order.price == 191, 550),
                else_=Order.quantity,
            )
        )
    )
    session.execute(st)
    session.commit()


def correlated_updates(session):
    # 参考资料：https://docs.sqlalchemy.org/en/20/tutorial/data_update.html#correlated-updates
    query_owner = select(ShipTransfer2.from_).where(ShipTransfer.token_id == ShipTransfer2.token_id).scalar_subquery()
    st = update(ShipTransfer).values(to=query_owner)
    session.execute(st)
    session.commit()


if __name__ == "__main__":
    from sa.session import SessionMaker, engine

    with SessionMaker() as session:
        bulk_update(session, engine)
