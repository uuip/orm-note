from sqlalchemy import select, exists


def element_not_in_another_not_exists(model1, model2):
    # 在当前测试场景中与leftjoin相当
    return select(model1.token_id).where(~exists(select(model2.token_id).where(model2.token_id == model1.token_id)))


def element_not_in_another_leftjoin(model1, model2):
    # join字段与查询字段都有索引
    return (
        select(model1.token_id)
        .join(model2, model1.token_id == model2.token_id, isouter=True)
        .where(model2.token_id.is_(None))
    )


def element_not_in_another_except(model1, model2):
    # 返回去重的token_id，若返回其他字段需要where token_id in ...
    return select(model1.token_id).except_(select(model2.token_id))


def element_not_in_another_not_in(model1, model2):
    # 性能最差
    subq = select(model2.token_id).scalar_subquery()
    return select(model1).where(model1.token_id.not_in(subq))
