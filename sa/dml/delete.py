from sqlalchemy import *
from sqlalchemy.orm import Session

s: Session


def delete_single():
    s.delete(...)
    s.commit()


def bulk_delete():
    s.query(...).filter(...).delete()
    s.commit()
    s.execute(delete(...).where(...))
    s.commit()
