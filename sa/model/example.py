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

    id = mapped_column(BigInteger, Identity(), primary_key=True)
    name = mapped_column(Text, unique=True, nullable=False)
    org = mapped_column(Text)
    books = mapped_column(ARRAY(item_type=Integer))
    nickname: Mapped[Optional[str]]

    order_collection = relationship("Order", back_populates="author")

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name}>"


class Order(Base):
    __tablename__ = "order"

    id = mapped_column(BigInteger, Identity(), primary_key=True)
    updated_at = mapped_column(
        TIMESTAMP(timezone=True, precision=0),
        server_default=func.current_timestamp(0),
        onupdate=func.current_timestamp(),
    )
    created_at = mapped_column(
        TIMESTAMP(timezone=True, precision=0),
        server_default=func.current_timestamp(),
    )
    quantity = mapped_column(BigInteger)
    price_num = mapped_column(Numeric(precision=10, scale=2))  # 总10位，小数2位
    price = mapped_column(Float)  # cast(...,Numeric(10,2)后再比较相等
    status = mapped_column(Enum(Status, native_enum=False))  # Enum 参数默认native_enum=True，等价pg ENUM type
    # status = mapped_column(Enum(Status, values_callable=lambda obj: [str(x.value) for x in obj]))
    block_time = mapped_column(BigInteger)

    author_id = mapped_column(ForeignKey(Author.id, ondelete="CASCADE"))
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

    network = mapped_column(CIDR, nullable=False, primary_key=True)
    geoname_id = mapped_column(Integer)
    registered_country_geoname_id = mapped_column(Integer)
    represented_country_geoname_id = mapped_column(Integer)
    is_anonymous_proxy = mapped_column(Boolean)
    is_satellite_provider = mapped_column(Boolean)
    postal_code = mapped_column(Text)
    latitude = mapped_column(Numeric)
    longitude = mapped_column(Numeric)
    accuracy_radius = mapped_column(Integer)


class EventBase(Base):
    __abstract__ = True

    id = mapped_column(BigInteger, Identity(), primary_key=True)
    transactionHash = mapped_column(VARCHAR(66))
    logIndex = mapped_column(Integer)

    event = mapped_column(Text, nullable=False)
    transactionIndex = mapped_column(Integer)
    blockNumber = mapped_column(Integer)

    from_ = mapped_column(VARCHAR(42), name="from")
    to = mapped_column(VARCHAR(42))
    token_id = mapped_column(Integer, index=True)


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
