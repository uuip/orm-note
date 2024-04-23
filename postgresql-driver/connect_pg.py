from sqlalchemy import *
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import *

from sa.session import SessionMaker


class Base(DeclarativeBase):
    pass


class Workflow(Base):
    __tablename__ = "workflows_test"

    id = mapped_column(Uuid, primary_key=True, server_default=func.gen_random_uuid())
    graph_txt = mapped_column(Text)
    graph = mapped_column(JSONB)


# Base.metadata.drop_all(bind=engine, tables=[Workflow.__table__])
# Base.metadata.create_all(bind=engine, tables=[Workflow.__table__])

value = {
    "split": "\n\n",
}

# with psycopg.connect(settings.db) as conn:  # type: psycopg.Connection
#     conn.execute("select 1")
    # conn.execute(
    #     f"insert into {Workflow.__tablename__} (graph_txt, graph) values (%s,%s)",
    #     [json.dumps(value), json.dumps(value)],
    # )
    # conn.commit()


with SessionMaker() as session:
    session.execute(text("select 1"))

# Example: query remote database using settings.db
# dsn = settings.db
# with psycopg.connect(dsn, row_factory=dict_row) as conn:
#     cursor = conn.execute("select * from plugin_declarations limit 1;")
#     row = cursor.fetchone()
#
# row["declaration"] = json.loads(row["declaration"])
# with open("declaration.json", "w+") as f:
#     json.dump([row], f, ensure_ascii=False, indent=4)
