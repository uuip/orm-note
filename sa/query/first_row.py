from sqlalchemy import case, select, desc, func
from sqlalchemy.orm import aliased
from sqlalchemy.dialects.postgresql import distinct_on

from sa.model.example import ShipTransfer


def get_first_row_every_group_distinct_on():
    # 获取分组第一条：distinct on,性能好
    # 推荐用法（PostgreSQL）：使用 DISTINCT ON 获取每组首条。
    t = aliased(ShipTransfer)
    owner_case = case(
        (t.to.in_(["aa", "bb"]) & t.from_.not_in(["cc", "dd"]), t.from_),
        else_=t.to,
    ).label("owner")
    # SQLAlchemy 2.1 已弃用传列给 distinct() 的写法：select(t.token_id, owner_case).distinct(t.token_id)
    owner_table = (
        select(t.token_id, owner_case)
        .ext(distinct_on(t.token_id))
        .order_by(t.token_id, desc(t.blockNumber), desc(t.logIndex), desc(t.id))
    )
    return owner_table


def get_first_row_every_group_window_func():
    # 推荐用法（需要排名）：使用 row_number() 窗口函数获取分组首条。
    t = aliased(ShipTransfer)
    win = select(
        t.token_id,
        t.from_,
        t.to,
        func.row_number()
        .over(partition_by=t.token_id, order_by=[desc(t.blockNumber), desc(t.logIndex), desc(t.id)])
        .label("new_index"),
    ).cte("win")
    owner_case = case(
        (win.c.to.in_(["aa", "bb"]) & win.c.from_.not_in(["cc", "dd"]), win.c.from_),
        else_=win.c.to,
    ).label("owner")
    owner_table = select(win.c.token_id, owner_case).where(win.c.new_index == 1)
    return owner_table


if __name__ == "__main__":
    from sa.session import SessionMaker

    with SessionMaker() as s:
        st = get_first_row_every_group_distinct_on()
        print(s.execute(st).all())
        st = get_first_row_every_group_window_func()
        print(s.execute(st).all())
