"""
EncryptedString：自动加密和解密的 SQLAlchemy 列类型。

用法：
    class MyModel(Base):
        __tablename__ = 'my_model'
        id = Column(Integer, primary_key=True)
        password = Column(EncryptedString)
"""

from sqlalchemy import TypeDecorator
from sqlalchemy.dialects.postgresql import TEXT

from .crypto import decrypt, encrypt, is_encrypted


class EncryptedString(TypeDecorator):
    impl = TEXT
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None or is_encrypted(value):
            return value
        return encrypt(value)

    def process_result_value(self, value, dialect):
        if value is None or not is_encrypted(value):
            return value
        return decrypt(value)
