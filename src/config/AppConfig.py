from dataclasses import dataclass
from mashumaro.mixins.json import DataClassJSONMixin


@dataclass
class AppConfig(DataClassJSONMixin):
    ssh_host: str
    ssh_username: str
    ssh_password: str
    database_username: str
    database_password: str
    localhost: str
    localport: int

