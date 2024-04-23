from sqlalchemy import *
from sqlalchemy.orm import *

from sa.model.example import Author, Order
from sa.session import SessionMaker

s = SessionMaker()
# 关联查询
select(Author).join(Order, Author.id == Order.author_id).where(Order.price < 159)

st = select(Author.name, Order.quantity).join_from(Author, Order).where(Order.price < 159)
for x in s.execute(st):
    (x.name, x.quantity)

# where过滤关联表时，需要显式join
select(Author).join(Order).options(selectinload(Author.order_collection)).where(Order.quantity == 2)

# n+1

# joinedload()的优化版，需要显式join， Author表join一次，总1次查询
select(Order).join(Author).options(contains_eager(Order.author)).limit(5)

# 下面方式，是否指定join(Author)无影响

# 先查出limit数量的子表，然后访问属性时发起第二条查询，父表id in 集合，总2次查询
select(Order).join(Author).options(selectinload(Order.author)).limit(5)
# 先查出limit数量的子表，然后访问属性时发起第二条查询，父表join前面查子表语句为子查询，总2次查询
select(Order).join(Author).options(subqueryload(Order.author)).limit(5)
# immediateload 若user已查询，则无操作，否则单独sql查询user，总查询 "不同user" 数量
select(Order).join(Author).options(immediateload(Order.author)).limit(5)
# joinedload Author表join两次, 总1次查询
select(Order).join(Author).options(joinedload(Order.author)).limit(5)

s.commit()
s.close()
