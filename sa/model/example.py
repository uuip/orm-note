# https://github.com/agronholm/sqlacodegen
# https://github.com/sqlalchemy/alembic
# https://docs.sqlalchemy.org/en/20/orm/nonstandard_mappings.html#mapping-a-class-against-arbitrary-subqueries

# 都有Enum
# from sqlalchemy import Enum
# from enum import Enum

import enum
from typing import Optional

from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import *

from . import Base


class Status(enum.StrEnum):
    open = enum.auto()
    close = enum.auto()


class Author(Base):
    __tablename__ = "author"

    id = Column(BigInteger, Identity(), primary_key=True)
    name = Column(Text, unique=True, nullable=False)
    org = Column(Text)
    books = Column(ARRAY(item_type=Integer))
    nickname: Mapped[Optional[str]]

    order_collection = relationship("Order", back_populates="author")

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name}>"


class Order(Base):
    __tablename__ = "order"

    id = Column(BigInteger, Identity(), primary_key=True)
    updated_at = Column(
        TIMESTAMP(timezone=True, precision=0),
        server_default=func.current_timestamp(0),
        onupdate=func.current_timestamp(),
    )
    created_at = Column(
        TIMESTAMP(timezone=True, precision=0),
        server_default=func.current_timestamp(),
    )
    quantity = Column(BigInteger)
    price_num = Column(Numeric(precision=10, scale=2))  # 总10位，小数2位
    price = Column(Float)  # cast(...,Numeric(10,2)后再比较相等
    status = Column(Enum(Status, native_enum=False))  # Enum 参数默认native_enum=True，等价pg ENUM type
    # status = Column(Enum(Status, values_callable=lambda obj: [str(x.value) for x in obj]))
    block_time = Column(BigInteger)

    author_id = Column(ForeignKey(Author.id, ondelete="CASCADE"))
    author = relationship(Author, back_populates="order_collection")

    # https://docs.sqlalchemy.org/en/20/orm/mapped_sql_expr.html
    @hybrid_property
    def charge(self):
        return self.price * self.quantity

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.id}>"


class GeoIp(Base):
    __tablename__ = "geoip2_network"
    __table_args__ = (
        Index(
            "network_gist",
            "network",
            postgresql_using="gist",
            postgresql_ops={"network": "inet_ops"},
        ),
    )

    network = Column(CIDR, nullable=False, primary_key=True)
    geoname_id = Column(Integer)
    registered_country_geoname_id = Column(Integer)
    represented_country_geoname_id = Column(Integer)
    is_anonymous_proxy = Column(Boolean)
    is_satellite_provider = Column(Boolean)
    postal_code = Column(Text)
    latitude = Column(Numeric)
    longitude = Column(Numeric)
    accuracy_radius = Column(Integer)


class EventBase(Base):
    __abstract__ = True

    id = Column(BigInteger, Identity(), primary_key=True)
    transactionHash = Column(VARCHAR(66))
    logIndex = Column(Integer)

    event = Column(Text, nullable=False)
    transactionIndex = Column(Integer)
    blockNumber = Column(Integer)

    from_ = Column(VARCHAR(42), name="from")
    to = Column(VARCHAR(42))
    token_id = Column(Integer, index=True)


class ShipTransfer(EventBase):
    __tablename__ = "ship_transfer"
    __table_args__ = (
        # 下面两行等价
        # UniqueConstraint("transactionHash", "logIndex", name="unique_ship_transfer"),
        Index("uniqueidx", "transactionHash", "logIndex", unique=True),
    )


class ShipTransfer2(EventBase):
    __tablename__ = "ship_transfer2"
    __table_args__ = (Index("uniqueidx2", "transactionHash", "logIndex", unique=True),)
