from enum import StrEnum

from sqlalchemy.orm import *


class Base(DeclarativeBase):
    pass


class OnDelete(StrEnum):
    cascade = "CASCADE"
