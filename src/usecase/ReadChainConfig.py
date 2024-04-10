import logging

from src.datamodel.ExperimentConfig import ExperimentConfig
from src.util.FileProvider import FileProvider


class ReadChainConfig:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__file_provider = FileProvider()

    def execute(self, config_file_name: str) -> ExperimentConfig:
        self.logger.debug(f'execute for {config_file_name}')
        fname = self.__file_provider.get_study_config_file_path(config_file_name)
        with open(fname) as f:
            config_txt = f.readlines()
            config_txt = ''.join(config_txt)
        config = ExperimentConfig.from_json(config_txt)
        if config.name is None:
            config.name = config_file_name[:-5].replace('/', '_')
            config.outcome_dir = config.name
        return config
