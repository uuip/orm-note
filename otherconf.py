"""
配置优先级：环境变量 > yaml 配置
嵌套配置的环境变量使用2个下划线分割
"""

from functools import cached_property
from pathlib import Path
from pydantic import computed_field, BaseModel, Field
from pydantic_settings import *
from urllib.parse import urlparse


class CustomSources(BaseSettings):
    def __init_subclass__(cls, source):
        super().__init_subclass__()

        def func(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (
                init_settings,
                env_settings,
                source(settings_cls),
                file_secret_settings,
            )

        cls.settings_customise_sources = classmethod(func)


class BscConfig(BaseModel):
    """
    环境变量设置：
    BSC__NODE
    BSC__INIT_BLOCK
    """

    node: str = Field()
    init_block: int


class PGConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int
    database: str
    user: str | None
    password: str | None


yaml_file = Path(__file__).parent / "config.yaml"


class YamlSettings(CustomSources, source=YamlConfigSettingsSource):
    model_config = SettingsConfigDict(
        yaml_file=yaml_file, extra="ignore", env_nested_delimiter="__"
    )

    @computed_field
    @cached_property
    def db(self) -> PGConfig:
        c = urlparse(self.db_url)
        return PGConfig(
            host=c.hostname,
            port=c.port or 5432,
            database=c.path.lstrip("/"),
            user=c.username,
            password=c.password,
        )

    db_url: str
    bsc: BscConfig


settings = YamlSettings()


class TomlSettings(CustomSources, source=TomlConfigSettingsSource):
    model_config = SettingsConfigDict(toml_file="conf.toml", extra="ignore")

    broker: str


# [app]
# from_address = ["0xd17aDA449e0A441df3Fd6170F48C0871a7dD7C4a", "0x0529b0c14c5cf934868b5135a510df65fdf8a619"]
# broker = "redis://:octa8lab@106.3.151.9:6379/4"
#
# [app.database]
# host = "localhost/somedb"
# port = 5432
#
# [[app.node]]
# host = "1.2.3.4"
# port = 80
#
# [[app.node]]
# host = "1.2.3.4"
# port = 80
class PyprojectTomlSettings(CustomSources, source=PyprojectTomlConfigSettingsSource):
    model_config = SettingsConfigDict(
        pyproject_toml_table_header=["app"], extra="ignore"
    )

    broker: str
