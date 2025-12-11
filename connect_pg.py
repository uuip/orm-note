import json

import psycopg
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import *

from conf import settings
from sa.session import SessionMaker, engine


class Base(DeclarativeBase):
    pass


class Workflow(Base):
    __tablename__ = "workflows_test"

    id = mapped_column(Uuid, primary_key=True, server_default=func.gen_random_uuid())
    graph_txt = mapped_column(Text)
    graph = mapped_column(JSONB)


Base.metadata.drop_all(bind=engine, tables=[Workflow.__table__])
Base.metadata.create_all(bind=engine, tables=[Workflow.__table__])

value = {
    "split": "\n\n",
}

with psycopg.connect(settings.db) as conn:  # type: psycopg.Connection
    conn.execute(
        f"insert into {Workflow.__tablename__} (graph_txt, graph) values (%s,%s)",
        [json.dumps(value), json.dumps(value)],
    )
    conn.commit()


with SessionMaker() as session:
    with session.begin() as begin:
        for row in session.scalars(select(Workflow)):
            print(type(row.graph), row.graph)
