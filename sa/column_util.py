def model_colunm_map(obj):
    return {k: getattr(obj, k) for k, v in obj.__mapper__.c.items()}


def db_column_map(obj):
    return {c.name: getattr(obj, model_field) for model_field, c in obj.__mapper__.c.items()}
