from functools import cached_property
from pathlib import Path
from urllib.parse import urlparse

from pydantic import Field, computed_field
from pydantic_settings import SettingsConfigDict, BaseSettings, PydanticBaseSettingsSource, YamlConfigSettingsSource

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


settings = Settings()


class YamlSettings(BaseSettings):
    model_config = SettingsConfigDict(yaml_file=..., extra="ignore")

    db: str = Field(alias="db_url")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return init_settings, env_settings, YamlConfigSettingsSource(settings_cls), file_secret_settings
