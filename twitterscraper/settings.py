import os
from typing import Optional, List

import yaml
import pydantic

SETTINGS_FILE_PATH_ENV_VAR_KEY = "SETTINGS_FILE"
DEFAULT_SETTINGS_FILE_PATH = "settings.yaml"


class PostgresSettings(pydantic.BaseModel):
    host: str
    port: int
    username: str
    password: pydantic.SecretStr
    database: Optional[str] = None


class JobmanagerSettings(pydantic.BaseModel):
    postgres: PostgresSettings


class RepositorySettings(pydantic.BaseSettings):
    postgres: PostgresSettings


class NitterSettings(pydantic.BaseSettings):
    instances: List[pydantic.HttpUrl]
    request_timeout_seconds: float = 10.0

    @property
    def instances_str(self):
        return [str(inst) for inst in self.instances]


class Settings(pydantic.BaseModel):
    jobmanager: JobmanagerSettings
    repository: RepositorySettings
    nitter: NitterSettings


def load_settings():
    settings_file_path = os.getenv(SETTINGS_FILE_PATH_ENV_VAR_KEY) or DEFAULT_SETTINGS_FILE_PATH
    with open(settings_file_path, "r") as f:
        data = yaml.safe_load(f)

    return Settings.parse_obj(data)
