from sqlalchemy import *

from sa.model.example import Author, Order, ShipTransfer, ShipTransfer2
from sa.session import db, Session


def update_single():
    obj = s.scalar(select(Author).limit(1))
    obj.org = "other"
    s.commit()

    st = update(Author).where(Author.org == "other").values(org="other org")
    s.execute(st)
    s.commit()
    print(obj.org)


def bulk_update():
    for obj in s.scalars(select(Author).execution_options(yield_per=500)):
        obj.nickname += "sometext"
    # 拆分成多条语句执行
    s.commit()

    obj = s.scalar(select(Author).limit(1))
    s.query(Author).filter(Author.id == obj.id).update({"org": "dskdkdkd"})
    s.commit()

    # 批量更新 UPDATE..FROM (VALUES ...) RETURNING
    vst = values(
        column("org", Text),
        column("nickname", Text),
        name="vst",
    ).data(
        [
            ["dskdkdkd", "aaaa"],
        ]
    )
    st = update(Author).where(Author.org == vst.c.org).values({Author.nickname: vst.c.nickname})
    s.execute(st)
    s.commit()

    # 指定pk, params的key只能是字符串; 拆分成多条语句执行
    s.execute(update(Author), [{"id": obj.id, "org": "bbbb"}, {"id": 104, "org": "cccc"}])
    s.commit()

    # https://docs.sqlalchemy.org/en/20/tutorial/data_update.html
    # 批量更新: 子查询
    query_owner = select(ShipTransfer2.from_).where(ShipTransfer.token_id == ShipTransfer2.token_id).scalar_subquery()
    st = update(ShipTransfer).values(to=query_owner)
    s.execute(st)
    s.commit()

    # 批量更新-关联查询 UPDATE..FROM...RETURNING
    st = (
        update(ShipTransfer)
        .where(ShipTransfer.token_id == ShipTransfer2.token_id)
        .values({ShipTransfer.from_: ShipTransfer2.to})
    )
    s.execute(st)
    s.commit()

    # 批量更新：where与value绑定到字典的key; 拆分成多条语句执行
    to_update = [{"v_name": "aaaa", "v_nickname": "bindparambbbb"}, {"v_name": "aaaa1", "v_nickname": "bindparambbbb"}]
    st = update(Author).where(Author.nickname == bindparam("v_name")).values(nickname=bindparam("v_nickname"))
    with db.begin() as conn:
        conn.execute(st, to_update)


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


if __name__ == "__main__":
    with Session() as s:
        update_single()
        bulk_update()
        update_with_case_value()
