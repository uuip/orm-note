from functools import cached_property
from pathlib import Path
from urllib.parse import urlparse

from pydantic import Field, model_validator, computed_field  # noqa
from pydantic_settings import SettingsConfigDict, BaseSettings

_env_file = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=_env_file, extra="ignore")

    db: str = Field(alias="db_url")

    @computed_field
    @cached_property
    def db_standard(self) -> str:
        return urlparse(self.db)._replace(scheme="postgresql").geturl()

    @computed_field
    @cached_property
    def db_asyncpg(self) -> str:
        return urlparse(self.db)._replace(scheme="postgresql+asyncpg").geturl()

    # @model_validator(mode="before")
    # def variable(cls, value):
    #     value["db_standard"] = urlparse(value["db_url"])._replace(scheme="postgresql").geturl()
    #     return value


settings = Settings()
