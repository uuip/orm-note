from sqlalchemy import *
from sqlalchemy.orm import *

from sa.model.usercase import User, Order
from sa.session import s

# 关联查询
select(User).join(Order, User.id == Order.user_id).where(Order.price < 159)
st = select(User.name, Order.quantity).join_from(User, Order).where(Order.price < 159)
for x in s.execute(st):
    (x.name, x.quantity)

# n+1

# select(...).join(...).options(joinedload())的优化版，需要显式join， 总1次查询
select(Order).join(User).options(contains_eager(Order.user)).limit(5)

# 显式join(User)，joinedload User表join两次
# st = select(Order).join(User).options(joinedload(Order.user)).limit(5)

# 不指定join(User)，正常
# st = select(Order).options(joinedload(Order.user)).limit(5)

# 下面方式，是否指定join(User)无影响
# 先查出limit数量的子表，然后访问属性时发起第二条查询，父表id in 集合，总2次查询
select(Order).join(User).options(selectinload(Order.user)).limit(5)
# 先查出limit数量的子表，然后访问属性时发起第二条查询，父表join(前面查子表语句为子查询)，总2次查询
select(Order).join(User).options(subqueryload(Order.user)).limit(5)

# immediateload 若user已查询，则无操作，否则单独sql查询user，总查询 "不同user" 数量
# st = select(Order).join(User).options(immediateload(Order.user)).limit(5)

# where过滤关联表时，需要显式join
select(User).join(Order).options(selectinload(User.order_collection)).where(Order.quantity == 2)
