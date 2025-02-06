def model_colunm_map(obj):
    return {model_field: getattr(obj, model_field) for model_field, c in obj.__mapper__.c.items()}


def db_column_map(obj):
    return {c.name: getattr(obj, model_field) for model_field, c in obj.__mapper__.c.items()}
