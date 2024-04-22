from sqlalchemy import *

from sa.model.example import Author
from sa.session import Session


def delete_single():
    obj = s.scalar(select(Author).limit(1))
    s.delete(obj)
    s.commit()


def bulk_delete():
    s.query(Author).filter(Author.id == 104).delete()
    s.commit()
    s.execute(delete(Author).where(Author.id == 105))
    s.commit()


if __name__ == "__main__":
    with Session() as s:
        delete_single()
        bulk_delete()
