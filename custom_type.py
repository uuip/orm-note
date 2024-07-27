from typing import Annotated

from pydantic import *


def validate_phone(v: int) -> str:
    if len(str(v)) != 11:
        raise ValueError
    return str(v)


PhoneNumber = Annotated[
    int,
    PlainValidator(validate_phone),
    PlainSerializer(lambda x: str(x), return_type=str),
    WithJsonSchema({"type": "integer"}, mode="validation"),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]


class User(BaseModel):
    phone: PhoneNumber


u = User(phone=12312345678)
