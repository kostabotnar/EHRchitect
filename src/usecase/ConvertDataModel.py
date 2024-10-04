import logging
from pathlib import Path

import pandas as pd

from src.adapter.TnxAdapter import TnxAdapter
from src.util.FileProvider import FileProvider


class ConvertDataModel:
    __source_types = {'tnx'}

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fp = FileProvider()
        self.datadictionary = pd.read_csv(self.fp.data_dictionary_file)

    def execute(self, data_path: str, source_type: str):
        """
        Executes the data conversion process based on the provided input parameters.
        Save results in a new CSV file.
        :param data_path: the path to the TNX data
        :param source_type: the source type (tnx)
        :return: none
        """
        self.logger.debug(f'Execute for data in {data_path} from source {source_type}')
        if source_type not in self.__source_types:
            raise ValueError(f'Source type {source_type} is not supported')

        if source_type == 'tnx':
            adapter = TnxAdapter()
        else:
            raise ValueError(f'Unsupported source type {source_type}')
        res_dir = adapter.convert(Path(data_path))
        return res_dir
