from functools import cached_property
from pathlib import Path
from urllib.parse import urlparse

from pydantic import Field, computed_field
from pydantic_settings import *

env_file = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_file, extra="ignore")

    db: str = Field(alias="db_url")

    @computed_field
    @cached_property
    def db_standard(self) -> str:
        return urlparse(self.db)._replace(scheme="postgresql").geturl()

    @computed_field
    @cached_property
    def db_asyncpg(self) -> str:
        return urlparse(self.db)._replace(scheme="postgresql+asyncpg").geturl()


settings = Settings()
