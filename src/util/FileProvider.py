from pathlib import Path


class FileProvider(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FileProvider, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.project_dir = Path(__file__).resolve().parents[2]

    @property
    def result_path(self):
        return self.project_dir / "build"

    @property
    def config_path(self):
        return self.project_dir / "config"

    @property
    def data_path(self):
        return self.project_dir / "data"

    @property
    def data_archive_path(self):
        return self.project_dir / "data_arch"

    @property
    def log_config_file(self):
        return self.config_path / "logging.conf"

    @property
    def app_config_file(self):
        return self.config_path / "app_config.json"

    @property
    def data_dictionary_file(self):
        return self.data_path / "datadictionary.csv"

    @property
    def tnx_data_map_file(self):
        return self.data_path / "tnx_data_model_map.csv"

    @property
    def code_map_table_file(self):
        return str(self.data_path / "icd9_map_icd10.csv")
