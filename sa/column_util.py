def to_model_colunm(obj):
    return {k: getattr(obj, k) for k, v in obj.__mapper__.c.items()}


def to_db_column(obj):
    # db column  -> model column, example: from -> from_
    db_orm_column_map = {v.name: k for k, v in obj.__mapper__.c.items()}
    return {c.name: getattr(obj, db_orm_column_map[c.name]) for c in obj.__table__.columns}
