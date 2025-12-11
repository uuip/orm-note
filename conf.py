from functools import cached_property
from pathlib import Path
from urllib.parse import urlparse

from pydantic import Field, computed_field
from pydantic_settings import *

env_file = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_file, extra="ignore")

    db_url: str = Field()

    @computed_field
    @cached_property
    def db_dict(self) -> str:
        u = urlparse(self.db_url)
        if u.scheme.startswith("postgres"):
            default_port = 5432
        else:
            default_port = 3306
        return {
            "host": u.hostname,
            "port": int(u.port or default_port),
            "user": u.username,
            "password": u.password,
            "database": u.path.lstrip("/"),
        }

    @computed_field
    @cached_property
    def db(self) -> str:
        u = urlparse(self.db_url)
        return u._replace(scheme=u.scheme.split("+")[0]).geturl()

    @computed_field
    @cached_property
    def db_asyncpg(self) -> str:
        return urlparse(self.db_url)._replace(scheme="postgresql+asyncpg").geturl()


settings = Settings()
