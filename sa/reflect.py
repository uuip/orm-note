from sqlalchemy.ext.automap import automap_base

from sa.session import db

AutoBase = automap_base()

# class Tx(AutoBase):
#     __tablename__ = "transactions"

AutoBase.prepare(autoload_with=db)
models = AutoBase.classes

# # ==================
# metadata = MetaData()
# metadata.reflect(bind=db)
# model = metadata.tables["users"]
# # ==================
# metadata = MetaData()
# original = Table(from_table, metadata, autoload_with=db)
# new_table = Table(to_table, metadata)
#
# for col in original.columns:
#     if col.name == "id":
#         continue
#     new_table.append_column(col.copy())
# new_table.append_column(Column("id", BigInteger, primary_key=True))
# metadata.drop_all(bind=db, tables=[new_table])
# metadata.create_all(bind=db, tables=[new_table])
#
#
# class NewTableModel(declarative_base(metadata=metadata)):
#     __table__ = new_table
