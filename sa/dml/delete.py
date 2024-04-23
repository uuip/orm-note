from sqlalchemy import *

from sa.model.example import Author
from sa.session import SessionMaker


def delete_obj():
    obj = s.scalar(select(Author).limit(1))
    s.delete(obj)
    s.commit()


def delete_with_query():
    s.query(Author).filter(Author.id == 104).delete()
    s.commit()
    s.execute(delete(Author).where(Author.id == 105))
    s.commit()


if __name__ == "__main__":
    with SessionMaker() as s:
        delete_obj()
        delete_with_query()
