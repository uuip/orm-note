from sqlalchemy import *

from sa.model.usercase import User, Order, ShipTransfer, ShipTransfer2
from sa.session import db, s


def update_single():
    obj = s.scalar(select(User).limit(1))
    obj.name = "other"
    s.commit()

    st = update(User).where(User.name == "other").values(org="other org")
    s.execute(st)
    s.commit()
    print(obj.org)


def bulk_update(model, other):
    # 批量更新 1.x 风格
    for obj in s.scalars(select(User).execution_options(yield_per=500)):
        obj.nickname += "sometext"
    s.commit()

    obj = s.scalar(select(User).limit(1))
    s.query(User).filter(User.id == obj.id).update({"org": "dskdkdkd"})

    # 批量更新 UPDATE..FROM values
    vst = values(
        column("name", Text),
        column("nickname", Text),
        name="vst",
    ).data(
        [
            ["a", "aaa1"],
            ["e", "bbb1"],
        ]
    )
    st = update(User).where(User.name == vst.c.name).values({User.nickname: vst.c.nickname})
    s.execute(st)
    s.commit()

    # 指定pk
    s.execute(update(model), [{"id": ..., "something": "somevalue"}])

    # https://docs.sqlalchemy.org/en/20/tutorial/data_update.html
    # 批量更新: 子查询
    query_owner = select(User).where(other.token_id == model.token_id).scalar_subquery()
    update(User).values(owner=query_owner)

    # 批量更新-关联查询 UPDATE..FROM
    update(User).where(other.token_id == model.token_id).values({model.from_: other.from_})

    # 批量更新：where与value绑定到字典的key; 拆分成多条语句执行
    to_update = [{"v_name": ..., "v_nickname": ...}]
    st = update(User).where(User.name == bindparam("v_name")).values(nickname=bindparam("v_nickname")).returning()
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
    update_single()
    bulk_update(ShipTransfer, ShipTransfer2)
    update_with_case_value()
