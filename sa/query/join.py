from sqlalchemy import *
from sqlalchemy.orm import *

from sa.model.example import Author, Order


def join_examples(session):
    # 关联查询
    select(Author).join(Order, Author.id == Order.author_id).where(Order.price < 159)

    st = select(Author.name, Order.quantity).join_from(Author, Order).where(Order.price < 159)
    for x in session.execute(st):
        (x.name, x.quantity)

    # 推荐用法（一对多或多对多集合预加载）：selectinload；这里用显式 JOIN 过滤主对象。
    # 集合仍会完整加载，不只包含 quantity == 2 的订单；只筛选关联对象也可使用 .any()/.has()。
    select(Author).join(Order).options(selectinload(Author.order_collection)).where(Order.quantity == 2)

    # 避免 N+1 查询的几种关系加载方式

    # 推荐用法（已有用于过滤的显式 JOIN）：contains_eager 复用该连接填充关系，一次 SELECT 完成。
    select(Order).join(Author).options(contains_eager(Order.author)).limit(5)

    # 显式内连接会影响结果：没有作者的订单会被排除。

    # selectinload 在消费查询结果时加载作者，不是在首次访问属性时才发起查询。
    # 这里的小批量非空结果使用两次 SELECT；更大的批次可能需要更多查询。
    select(Order).join(Author).options(selectinload(Order.author)).limit(5)
    # 备选用法：subqueryload 在加载结果时，基于原查询构造第二次 SELECT；通常优先 selectinload。
    # 配合 LIMIT/OFFSET 时按唯一列排序，保证两次 SELECT 选择同一批记录。
    select(Order).join(Author).options(subqueryload(Order.author)).order_by(Order.id).limit(5)
    # 备选用法：immediateload 适合作者种类少且多已在会话中；总次数为主查询加未缓存的不同作者查询。
    select(Order).join(Author).options(immediateload(Order.author)).limit(5)
    # 推荐用法（多对一预加载）：joinedload；不需要过滤作者时可省略显式 join(Author)。
    # 当前示例保留显式连接作对照，因此作者表连接两次，但只有一次 SELECT。
    select(Order).join(Author).options(joinedload(Order.author)).limit(5)

    session.commit()


if __name__ == "__main__":
    from sa.session import SessionMaker

    with SessionMaker() as session:
        join_examples(session)
