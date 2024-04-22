from playhouse.reflection import generate_models

from pw.model import db

models = generate_models(db)
