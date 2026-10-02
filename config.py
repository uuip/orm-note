from functools import cached_property
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pydantic import Field, computed_field
from pydantic_settings import *

env_file = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_file, extra="ignore")

    db_url: str = Field()
    encryption_key: str | None = Field(default=None, repr=False)

    @computed_field
    @cached_property
    def db_dict(self) -> dict[str, Any]:
        u = urlparse(self.db_url)
        if u.scheme.startswith("postgres"):
            default_port = 5432
        else:
            default_port = 3306
        return {
                "host"    : u.hostname,
                "port"    : int(u.port or default_port),
                "user"    : u.username,
                "password": u.password,
                "database": u.path.lstrip("/"),
                }

    @computed_field
    @cached_property
    def db_asyncpg(self) -> str:
        return urlparse(self.db_url)._replace(scheme="postgresql+asyncpg").geturl()


settings = Settings()
