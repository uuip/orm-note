from sqlalchemy import *
from sqlalchemy.orm import *

from . import Base, OnDelete


class MiddleTable(Base):
    __tablename__ = "association_table"
    left_id = Column(ForeignKey("user.id"), primary_key=True)
    right_id = Column(ForeignKey("project.id"), primary_key=True)


class User(Base):
    __tablename__ = "user"

    id = Column(BigInteger, primary_key=True)
    username = Column(String)

    emails = relationship("Email", back_populates="user")

    wife = relationship("Wife", uselist=False, back_populates="user")

    country_id = Column(ForeignKey("country.id", ondelete=OnDelete.cascade))
    country = relationship("Country", back_populates="users")

    projects = relationship("Project", secondary=MiddleTable.__table__, back_populates="join_users")

    allsendmsg = relationship("Message", foreign_keys="Message.sender_id", back_populates="sender")
    allreceivedmsg = relationship("Message", foreign_keys="Message.receiver_id", back_populates="receiver")


# onetomany
class Email(Base):
    __tablename__ = "email"

    id = Column(BigInteger, primary_key=True)
    address = Column(String)
    user_id = Column(ForeignKey(User.id, ondelete=OnDelete.cascade))
    user = relationship(User, back_populates="emails")


# onetoone
class Wife(Base):
    __tablename__ = "wife"

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    user_id = Column(ForeignKey(User.id, ondelete=OnDelete.cascade), unique=True)
    user = relationship(User, back_populates="wife")


# manytoone
class Country(Base):
    __tablename__ = "country"

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    users = relationship(User, back_populates="country")


# manytomany
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
