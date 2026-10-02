from sqlalchemy import *
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import *

from config import settings

# 其他笔记：
# - URL.create()
# - 常见 QueuePool 的默认 pool_size 为 5；其他连接池类型可能不同。
# - expire_on_commit=False 不会在提交后刷新属性；赋入的时间字符串可能仍保留为字符串。
engine = create_engine(settings.db_url, echo=False)
SessionMaker = sessionmaker(bind=engine)

# https://docs.sqlalchemy.org/en/21/dialects/postgresql.html#using-psycopg-connection-pooling
async_engine = create_async_engine(settings.db_url, echo=False)
AsyncSessionMaker = async_sessionmaker(bind=async_engine)


# 条件用法：需要手动安排提交时点或在一个会话中进行多个事务时使用。
def usage_a():
    with SessionMaker() as session:  # 会话对象类型为 Session。
        session.execute(text("select 1"))
        session.commit()
    # 外层上下文退出时调用 session.close()。


def usage_c():
    # 推荐用法（一个独立事务）：自动处理 begin()/commit()/rollback()，并在退出时关闭会话。
    with SessionMaker.begin() as session:
        session.execute(text("select 1"))
    # 正常退出时提交事务并关闭会话；出现异常时回滚。


# 与 usage_c 等价；需要分别控制会话与事务的作用域时，使用这层嵌套。
def usage_b():
    with SessionMaker() as session:
        with session.begin():
            session.execute(text("select 1"))
        # 内层正常退出时提交事务，异常时回滚。
    # 外层上下文退出时调用 session.close()。
