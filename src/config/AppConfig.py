from dataclasses import dataclass
from mashumaro.mixins.json import DataClassJSONMixin


@dataclass
class AppConfig(DataClassJSONMixin):
    ssh_host: str
    ssh_username: str
    ssh_password: str
    mysql_username: str
    mysql_password: str
    localhost: str
    localport: int
    db_instances: list[str] = None

    def add_database(self, db_name: str):
        if self.db_instances is None:
            self.db_instances = [db_name]
        else:
            self.db_instances.append(db_name)
