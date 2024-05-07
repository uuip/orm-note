from pydantic_settings import *


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
            print(source)
            return init_settings, env_settings, source(settings_cls), file_secret_settings

        cls.settings_customise_sources = classmethod(func)


class TomlSettings(CustomSources, source=TomlConfigSettingsSource):
    model_config = SettingsConfigDict(toml_file="conf.toml", extra="ignore")

    broker: str


class YamlSettings(CustomSources, source=YamlConfigSettingsSource):
    """
    可以先写好yamle，使用 datamodel-code-generator 生产模型
    """

    model_config = SettingsConfigDict(yaml_file="conf.yaml", extra="ignore")

    broker: str


class YamlSettings1(BaseSettings):
    model_config = SettingsConfigDict(yaml_file="conf.yaml", extra="ignore")

    broker: str

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
    model_config = SettingsConfigDict(pyproject_toml_table_header=["app"], extra="ignore")

    broker: str
