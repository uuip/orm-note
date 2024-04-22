from sqlalchemy.ext.automap import automap_base

from sa.session import db

AutoBase = automap_base()

# class Tx(AutoBase):
#     __tablename__ = "transactions"

AutoBase.prepare(autoload_with=db)
models = AutoBase.classes
