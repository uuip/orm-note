from sqlalchemy import select, exists


# 推荐用法：查询左表中没有匹配右表的记录时，优先 NOT EXISTS；可保留左表 NULL 与重复值。
def element_not_in_another_not_exists(model1, model2):
    # 左表的 NULL 键会保留；重复的 token_id 不会被去重。
    # 在当前测试场景中与leftjoin相当
    return select(model1.token_id).where(~exists(select(model2.token_id).where(model2.token_id == model1.token_id)))


# 备选用法：LEFT JOIN 配合右表键 IS NULL，适合已有连接结构或执行计划更优的场景。
def element_not_in_another_leftjoin(model1, model2):
    # join字段与查询字段都有索引
    return (
        select(model1.token_id)
        .join(model2, model1.token_id == model2.token_id, isouter=True)
        .where(model2.token_id.is_(None))
    )


# 推荐用法（去重集合差）：EXCEPT。
def element_not_in_another_except(model1, model2):
    # 返回去重的token_id，若返回其他字段需要where token_id in ...
    # EXCEPT 会去重，并将 NULL 视为相同值；与前面的反连接示例语义不同。
    return select(model1.token_id).except_(select(model2.token_id))


# 条件用法：比较键两侧均非空时可用 NOT IN；不要当作可空键的通用差集写法。
def element_not_in_another_not_in(model1, model2):
    # 子查询含 NULL 时，未匹配的比较结果为 UNKNOWN，随后被 WHERE 排除。
    # 子查询非空时，左表 NULL 键也会被排除；键可空时优先使用 NOT EXISTS。
    # 性能最差
    subq = select(model2.token_id).scalar_subquery()
    return select(model1.token_id).where(model1.token_id.not_in(subq))
