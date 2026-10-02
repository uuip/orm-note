from sqlalchemy import *

from sa.model.example import Author


def delete_obj(session):
    obj = session.scalar(select(Author).limit(1))
    # 推荐用法（删除已加载实体并执行 ORM 关系规则）：Session.delete()。
    session.delete(obj)
    session.commit()


def delete_with_query(session):
    # 旧式 Query 写法仍可使用；新笔记优先使用下面的 execute(delete(...))。
    session.query(Author).filter(Author.id == 104).delete()
    session.commit()
    # 推荐用法（按条件批量删除）：execute(delete(...))；绕过 ORM 逐对象级联，关联清理由数据库外键保证。
    session.execute(delete(Author).where(Author.id == 105))
    session.commit()


if __name__ == "__main__":
    from sa.session import SessionMaker

    with SessionMaker() as session:
        delete_obj(session)
        delete_with_query(session)
