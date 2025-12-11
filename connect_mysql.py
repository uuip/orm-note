import json

import mysql.connector
from mysql.connector import CMySQLConnection
from mysql.connector.cursor_cext import CMySQLCursorDict
from sqlalchemy import *
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import *

from conf import settings
from sa.session import SessionMaker, engine


class Base(DeclarativeBase):
    pass


class Workflow(Base):
    __tablename__ = "workflows_test"

    id = mapped_column(BigInteger, primary_key=True)
    graph_txt = mapped_column(LONGTEXT)
    graph = mapped_column(JSON)
    method = mapped_column(String(256), server_default="api-key")


Base.metadata.drop_all(bind=engine, tables=[Workflow.__table__])
Base.metadata.create_all(bind=engine, tables=[Workflow.__table__])

value = {
    "split": "\n\n",
}

with mysql.connector.connect(**settings.db_dict) as conn:  # type: CMySQLConnection
    with conn.cursor() as cursor:  # type: CMySQLCursorDict
        cursor.execute(
            f"insert into {Workflow.__tablename__} (graph_txt, graph) values (%s,%s)",
            [json.dumps(value), json.dumps(value)],
        )
        conn.commit()

with SessionMaker() as session:
    for row in session.scalars(select(Workflow)):
        print(type(row.graph_txt), row.graph_txt)
        print(type(row.graph), row.graph)
