from datetime import datetime
from zoneinfo import ZoneInfo

from pydantic import *
from sqlalchemy import *

from sa.reflect import Model
from sa.session import s

obj = s.scalar(select(Model).where(Model.id == 1))
print(obj.id)

sh = ZoneInfo("Asia/Shanghai")


def strftime(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class BaseSerializer(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at", mode="before")
    def transform_time(cls, v):
        if isinstance(v, int):
            v = datetime.fromtimestamp(v)
        return v

    @field_serializer("created_at", "updated_at")
    def serializes_time(self, v):
        return strftime(v)


class ModelSerializer(BaseSerializer):
    name: str
    desc: str
    energy: int

    @computed_field(return_type=int)
    @property
    def someattr(self):
        return self.created_at.year


# a = ModelSerializer.model_validate(obj)
# a = ModelSerializer(**{"created_at": 100, "updated_at": 200})

s.close()
