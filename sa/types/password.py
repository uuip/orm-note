"""
PassWord: auto-hashing SQLAlchemy column type using Argon2.

用法：
    class User(Base):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True)
        password = Column(PassWord)
"""

from typing import Any, Optional

from pwdlib import PasswordHash
from pwdlib.hashers.argon2 import Argon2Hasher
from sqlalchemy import TypeDecorator, Text

password_hash = PasswordHash((Argon2Hasher(),))


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return password_hash.verify(plain_password, hashed_password)


def make_password(password: str) -> str:
    return password_hash.hash(password)


class PassWord(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value: Optional[str], dialect: Any) -> Optional[str]:
        if value is None:
            return None
        return make_password(value)

    def process_result_value(self, value: Optional[str], dialect: Any) -> Optional[str]:
        return value
