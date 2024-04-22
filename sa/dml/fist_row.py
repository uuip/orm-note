from sqlalchemy import case, select, desc, func
from sqlalchemy.orm import aliased

from sa.model.example import ShipTransfer
from sa.session import Session


def get_first_row_every_group_distinct_on():
    # 获取分组第一条：distinct on,性能好
    t = aliased(ShipTransfer)
    owner_case = case(
        (t.to.in_(["aa", "bb"]) & t.from_.not_in(["cc", "dd"]), t.from_),
        else_=t.to,
    ).label("owner")
    owner_table = (
        select(t.token_id, owner_case).distinct(t.token_id).order_by(t.token_id, desc(t.blockNumber), desc(t.logIndex))
    )
    return owner_table


def get_first_row_every_group_window_func():
    # 获取分组第一条：窗口函数
    t = aliased(ShipTransfer)
    win = select(
        t.token_id,
        t.from_,
        t.to,
        func.row_number()
        .over(partition_by=t.token_id, order_by=[desc(t.blockNumber), desc(t.logIndex)])
        .label("new_index"),
    ).cte("win")
    owner_case = case(
        (win.c.to.in_(["aa", "bb"]) & win.c.from_.not_in(["cc", "dd"]), win.c.from_),
        else_=win.c.to,
    ).label("owner")
    owner_table = select(win.c.token_id, owner_case).where(win.c.new_index == 1)
    return owner_table


if __name__ == "__main__":
    with Session() as s:
        st = get_first_row_every_group_distinct_on()
        print(s.scalars(st).all())
        st = get_first_row_every_group_window_func()
        print(s.scalars(st).all())
