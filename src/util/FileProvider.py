from pathlib import Path

import pandas as pd


class FileProvider(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FileProvider, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.project_dir = Path(__file__).resolve().parents[2]
        self.result_path = None

    @property
    def config_path(self) -> Path:
        return self.project_dir / "config"

    @property
    def data_path(self) -> Path:
        return self.project_dir / "data"

    @property
    def log_config_file(self) -> Path:
        return self.config_path / "logging.conf"

    @property
    def app_config_file(self) -> Path:
        return self.config_path / "app_config.json"

    @property
    def data_dictionary_file(self) -> Path:
        return self.data_path / "datadictionary.csv"

    @property
    def tnx_data_map_file(self) -> Path:
        return self.data_path / "tnx_data_model_map.csv"

    @property
    def code_map_table_file(self) -> str:
        return str(self.data_path / "icd9_map_icd10.csv")

    def get_result_file_path(self, filename: str) -> Path:
        return self.result_path / filename

    def patients_metadata_file_location(self, dir_name: str) -> tuple:
        return self.result_path / dir_name, 'patients.parquet'

    def get_transition_group_location(self, outcome_dir: str, end_level_number, group_number: int) -> tuple:
        root = self.get_transitions_path(outcome_dir, end_level_number)
        return root / f'group={group_number}', f'transition_{end_level_number - 1}_{end_level_number}.parquet'

    def get_event_group_location(self, outcome_dir: str, level_number, group_number: int) -> tuple:
        root = self.get_events_path(outcome_dir, level_number)
        return root / f'group={group_number}', f'event_{level_number}.parquet'

    def get_events_path(self, outcome_dir: str, level_number: int) -> Path:
        return self.result_path / outcome_dir / f'events/event_{level_number}.parquet'

    def get_transitions_path(self, outcome_dir: str, end_level_number: int) -> Path:
        return (self.result_path / outcome_dir / 'transitions' /
                f'transition_{end_level_number - 1}_{end_level_number}.parquet')

    def save_dataframe_file(self, df: pd.DataFrame, file_dir: Path, filename: str, file_format: str = 'parquet'):
        file_dir.mkdir(parents=True, exist_ok=True)
        if file_format == 'csv':
            df.to_csv(file_dir / filename)
        elif file_format == 'parquet':
            df.to_parquet(file_dir / filename, engine='pyarrow')

    def events_metadata_file_location(self, dir_name) -> tuple:
        return self.result_path / dir_name, 'events.parquet'

