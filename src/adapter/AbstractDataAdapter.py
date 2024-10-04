import abc
import logging
from pathlib import Path

import pandas as pd

from src.util.FileProvider import FileProvider


class AbstractDataAdapter(metaclass=abc.ABCMeta):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fp = FileProvider()
        self.datadictionary = pd.read_csv(self.fp.data_dictionary_file)

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'convert') and
                callable(subclass.convert) or
                NotImplemented)

    @abc.abstractmethod
    def convert(self, src_path: Path) -> Path:
        """Load in the data set"""
        raise NotImplementedError
