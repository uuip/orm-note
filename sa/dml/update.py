from sqlalchemy import *

from sa.model.example import Author, Order, ShipTransfer, ShipTransfer2
from sa.session import SessionMaker, engine


def update_single():
    obj = s.scalar(select(Author).limit(1))
    obj.org = "other"
    s.commit()

    st = update(Author).where(Author.org == "other").values({Author.org: "other org"})
    st = update(Author).where(Author.org == "other").values(org="other org")
    s.execute(st)
    s.commit()
    print(obj.org)

    obj = s.scalar(select(Author).limit(1))
    s.query(Author).filter(Author.id == obj.id).update({Author.org: "dskdkdkd"})
    s.query(Author).filter(Author.id == obj.id).update({"org": "dskdkdkd"})
    s.commit()


def bulk_update():
    for obj in s.scalars(select(Author)):
        obj.nickname = "sometext"
    # executemany执行
    s.commit()

    # 指定pk, params的key只能是字符串; executemany执行
    # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#orm-bulk-update-by-primary-key
    obj = s.scalar(select(Author).limit(1))
    s.execute(update(Author), [{"id": obj.id, "org": "bbbb"}, {"id": 2, "org": "cccc"}])
    s.commit()

    # 批量更新：where与value绑定到字典的key; executemany执行
    # https://docs.sqlalchemy.org/en/20/tutorial/data_update.html#the-update-sql-expression-construct
    # where条件bindparam, 其余字段与orm相同
    # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#disabling-bulk-orm-update-by-primary-key-for-an-update-statement-with-multiple-parameter-sets
    to_update = [
        {"v_name": "aaaa", "nickname": "bindparambbbb"},
        {"v_name": "aaaa1", "nickname": "bindparambbbb"},
    ]
    st = update(Author).where(Author.nickname == bindparam("v_name"))
    with db.begin() as conn:
        conn.execute(st, to_update)
    with s.connection() as conn:
        conn.execute(st, to_update)
        conn.commit()


def update_from():
    # https://docs.sqlalchemy.org/en/20/tutorial/data_update.html#update-from
    st = (
        update(ShipTransfer)
        .where(ShipTransfer.token_id == ShipTransfer2.token_id)
        .values({ShipTransfer.from_: ShipTransfer2.to})
    )
    s.execute(st)
    s.commit()

    # UPDATE..FROM (VALUES ...), 所有参数一次构造为VALUES
    vst = values(
        column("org", Text),
        column("nickname", Text),
        name="vst",
    ).data([["dskdkdkd", "aaaa"], ["dskdkdkd22", "aaaa"]])
    st = update(Author).where(Author.org == vst.c.org).values({Author.nickname: vst.c.nickname})
    s.execute(st)
    s.commit()


def update_with_case_value():
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
    s.execute(st)
    s.commit()


def correlated_updates():
    # https://docs.sqlalchemy.org/en/20/tutorial/data_update.html#correlated-updates
    query_owner = select(ShipTransfer2.from_).where(ShipTransfer.token_id == ShipTransfer2.token_id).scalar_subquery()
    st = update(ShipTransfer).values(to=query_owner)
    s.execute(st)
    s.commit()


if __name__ == "__main__":
    with SessionMaker() as s:
        bulk_update()
