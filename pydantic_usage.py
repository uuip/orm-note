from datetime import datetime
from typing import ClassVar, Any, Self
from zoneinfo import ZoneInfo

from pydantic import *

sh = ZoneInfo("Asia/Shanghai")


def strftime(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S%z")


class BaseSerializer(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime

    # https://docs.pydantic.dev/latest/concepts/models/#automatically-excluded-attributes
    all: ClassVar[dict] = dict()
    _processed_at: datetime = PrivateAttr(default_factory=datetime.now)

    def __init__(self, **data):
        super().__init__(**data)

    # 验证字段时实例还没有构造，不使用self
    # mode="before" 验证原始输入
    # mode="after" 默认值, 验证转换为定义字段后的值
    # 有些字段是可选的，在入参没输入，必需使用model_validator
    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def transform_time(cls, v) -> datetime:
        print("transform_time", type(v), v)
        if isinstance(v, int):
            v = datetime.fromtimestamp(v)
        return v

    @model_validator(mode="before")
    @classmethod
    def check(cls, data: Any) -> Any:
        return data

    @model_validator(mode="after")
    def check2(self) -> Self:
        return self

    @field_serializer("created_at", "updated_at")
    def serializes_time(self, v) -> str:
        return strftime(v)

    @computed_field
    @property
    def someattr(self) -> int:
        return self.created_at.year


class ModelSerializer(BaseSerializer):
    energy: int


a = ModelSerializer(**{"id": 30, "energy": 200, "created_at": 100, "updated_at": 200})
print(a.model_dump_json())
