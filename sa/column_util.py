def model_colunm_map(obj):
    return {k: getattr(obj, k) for k, v in obj.__mapper__.c.items()}


def db_column_map(obj):
    # db column  -> model column, example: from -> from_
    db_to_model_column_map = {v.name: k for k, v in obj.__mapper__.c.items()}
    return {c.name: getattr(obj, db_to_model_column_map[c.name]) for c in obj.__table__.columns}
