from sqlalchemy import *
from sqlalchemy.orm import *

from . import Base, OnDelete


class MiddleTable(Base):
    __tablename__ = "association_table"
    # 数据库级联只清理关联表记录，不删除另一端的父对象。
    left_id = Column(ForeignKey("user.id", ondelete=OnDelete.cascade), primary_key=True)
    right_id = Column(ForeignKey("project.id", ondelete=OnDelete.cascade), primary_key=True)


class User(Base):
    __tablename__ = "user"

    id = Column(BigInteger, primary_key=True)
    username = Column(String)

    # cascade 常见写法：
    # 不级联删除：省略 cascade，保留默认的 "save-update, merge"。
    # 禁用所有级联：cascade=None（或 ""）；与省略 cascade 不同。
    # 删除级联：cascade="all, delete"，Session.delete() 删除父对象时也删除子对象。
    emails = relationship("Email", back_populates="user", cascade="all, delete", passive_deletes=True)

    profile = relationship("Profile", uselist=False, back_populates="user", cascade="all, delete", passive_deletes=True)

    country_id = Column(ForeignKey("country.id", ondelete=OnDelete.cascade))
    country = relationship("Country", back_populates="users")

    projects = relationship("Project", secondary=MiddleTable.__table__, back_populates="join_users")

    allsendmsg = relationship(
        "Message",
        foreign_keys="Message.sender_id",
        back_populates="sender",
        cascade="all, delete",
        passive_deletes=True,
    )
    allreceivedmsg = relationship(
        "Message",
        foreign_keys="Message.receiver_id",
        back_populates="receiver",
        cascade="all, delete",
        passive_deletes=True,
    )


# 一对多
class Email(Base):
    __tablename__ = "email"

    id = Column(BigInteger, primary_key=True)
    address = Column(String)
    user_id = Column(ForeignKey(User.id, ondelete=OnDelete.cascade))
    user = relationship(User, back_populates="emails")


# 一对一
class Profile(Base):
    __tablename__ = "user_profile"

    id = Column(BigInteger, primary_key=True)
    display_name = Column(String)
    user_id = Column(ForeignKey(User.id, ondelete=OnDelete.cascade), unique=True)
    user = relationship(User, back_populates="profile")


# 多对一
class Country(Base):
    __tablename__ = "country"

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    users = relationship(User, back_populates="country", cascade="all, delete", passive_deletes=True)


# 多对多
class Project(Base):
    __tablename__ = "project"

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    join_users = relationship(User, secondary=MiddleTable.__table__, back_populates="projects")


# 多列引用一个表的同一字段
class Message(Base):
    __tablename__ = "message"

    id = Column(BigInteger, primary_key=True)
    msg = Column(String)
    sender_id = Column(ForeignKey(User.id, ondelete=OnDelete.cascade))
    receiver_id = Column(ForeignKey(User.id, ondelete=OnDelete.cascade))
    sender = relationship(User, foreign_keys=sender_id, back_populates="allsendmsg")
    receiver = relationship(User, foreign_keys=receiver_id, back_populates="allreceivedmsg")
