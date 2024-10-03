import logging
from datetime import datetime

from src.datamodel.ExperimentConfig import ExperimentConfig
from src.util.FileProvider import FileProvider


class StudyConfigReader:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__file_provider = FileProvider()

    def read(self, config_file_name: str) -> ExperimentConfig:
        self.logger.debug(f'Read {config_file_name}')
        with open(config_file_name) as f:
            config_txt = f.readlines()
            config_txt = ''.join(config_txt)
        config = ExperimentConfig.from_json(config_txt)
        if config.name is None:
            config.name = config_file_name[:-5].replace('/', '_')
            config.outcome_dir = config.name
        return config

    def validate(self, config_file_name: str) -> bool:
        self.logger.debug(f'Validate {config_file_name}')
        config = self.read(config_file_name)
        if config is None:
            self.logger.error(f'Failed to read config file: {config_file_name}')
            return False
        if len(config.levels) == 0:
            self.logger.error(f'No levels defined in config file: {config_file_name}')
            return False
        if config.time_frame is not None:
            try:
                if config.time_frame.min_date is not None:
                    datetime.strptime(str(config.time_frame.min_date), '%Y-%m-%d')
                if config.time_frame.max_date is not None:
                    datetime.strptime(str(config.time_frame.max_date), '%Y-%m-%d')
            except ValueError:
                self.logger.error(f'Invalid time frame format in config file: {config_file_name}. '
                                  f'Study time frame should be in format YYYY-MM-DD')
                return False
        for l in config.levels:
            if l.level < 0:
                self.logger.error(f'Invalid level number: {l.level} in config file: {config_file_name}')
                return False
            if not l.events:
                self.logger.error(f'Level {l.level if l.name is None else l.name} in config file: {config_file_name} '
                                  f'should have at least one event')
                return False
            if any([e.id is None or e.category is None for e in l.events]):
                self.logger.error(f'Invalid event in level {l.level} in config file: {config_file_name}. '
                                  f'Event should have id and category')
                return False
        return True
