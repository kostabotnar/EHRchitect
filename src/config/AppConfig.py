from dataclasses import dataclass
from mashumaro.mixins.json import DataClassJSONMixin


@dataclass
class DbInstance(DataClassJSONMixin):
    instance_id: str
    db_name: str


@dataclass
class AppConfig(DataClassJSONMixin):
    ssh_host: str
    ssh_username: str
    ssh_password: str
    mysql_username: str
    mysql_password: str
    localhost: str
    localport: int
    db_instances: list[DbInstance] = None

    def add_database(self, db_name: str):
        if self.db_instances is None:
            self.db_instances = []
        self.db_instances.append(DbInstance(instance_id=db_name, db_name=db_name))

