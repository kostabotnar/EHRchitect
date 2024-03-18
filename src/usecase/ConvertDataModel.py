import logging
import os
import shutil
import zipfile

import pandas as pd

from src.datamodel.DataColumns import DataDictionaryColumns as dd_cols, CommonColumns as cc_cols, \
    TnxMapColumns as map_cols
from src.util.ConcurrentUtil import ConcurrentUtil
from src.util.FileProvider import FileProvider


class ConvertDataModel:
    __source_types = {'tnx'}

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fp = FileProvider()
        self.datadictionary = pd.read_csv(self.fp.data_dictionary_file)
        self.conversion_map = None

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
            return self.convert_tnx_to_common_async(data_path)

        return None

    def convert_tnx_to_common_async(self, src_path: str) -> str:
        self.logger.debug(f'Async Converting TNX data to common format')
        conversion_map = pd.read_csv(self.fp.tnx_data_map_file)
        tnx_files = conversion_map[map_cols.tnx_file].unique().tolist()

        src_is_zip = src_path[-4:] == '.zip'
        if src_is_zip:
            src_path = self.extract_files_from_zip_async(src_path, tnx_files)

        self.logger.debug(f'Converting TNX files to common format')
        dest_path = f'{src_path}_ehr_cm'
        params = [(f, src_path, dest_path, conversion_map[conversion_map[map_cols.tnx_file] == f])
                  for f in tnx_files]
        ConcurrentUtil.run_in_separate_processes(self.convert_tnx_to_common, params)
        if src_is_zip is not None:
            self.logger.debug(f'Remove unzipped archive directory {src_path}')
            shutil.rmtree(src_path)
        return dest_path

    def convert_tnx_to_common(self, file_name: str, src_path: str, dest_path: str, table_map: pd. DataFrame):
        self.logger.debug(f'Converting file {file_name} to from {src_path} to {dest_path}')
        if not os.path.exists(src_path) or not os.path.exists(f'{src_path}/{file_name}'):
            raise FileNotFoundError(f'File {src_path}/{file_name} not found')
        selected_columns = table_map[map_cols.tnx_column].tolist()
        columns_map = {tnx_c: cm_c
                       for tnx_c, cm_c in zip(table_map[map_cols.tnx_column], table_map[map_cols.column])}

        df = pd.read_csv(f'{src_path}/{file_name}', usecols=selected_columns)
        # rename columns
        df = df.rename(columns=columns_map)
        # Drop duplicates
        df = df.drop_duplicates()
        # Convert column to datetime
        if file_name == 'patient.csv':
            # convert years of birth and death to datetime format
            df[cc_cols.date_of_birth] = pd.to_datetime(df[cc_cols.date_of_birth], format='%Y')
            df[cc_cols.date_of_death] = pd.to_datetime(df[cc_cols.date_of_death], format='%Y%m')
        else:
            table_name = table_map[map_cols.table].tolist()[0]
            date_columns = [c for c in self.get_tnx_table_date_columns(table_name)]
            for c in date_columns:
                df[c] = pd.to_datetime(df[c], format='%Y%m%d')
        # Save to a new CSV file
        out_file_name = table_map[map_cols.file].tolist()[0]
        if not os.path.exists(dest_path):
            os.mkdir(dest_path)
        self.logger.debug(f'Saving file {dest_path}/{out_file_name}')
        df.to_csv(f'{dest_path}/{out_file_name}', index=False)

    def get_tnx_table_date_columns(self, table_name: str) -> list:
        selection = ((self.datadictionary[dd_cols.table_name] == table_name)
                     & ((self.datadictionary[dd_cols.data_type] == "DATETIME")
                        | (self.datadictionary[dd_cols.data_type] == "DATE")))
        return self.datadictionary.loc[selection, dd_cols.column_name].unique().tolist()

    def extract_files_from_zip_async(self, src_path: str, tnx_files: list) -> str:
        extraction_path = src_path[:-4]
        self.logger.debug(f'Create a directory {extraction_path} for unzipping data')
        os.makedirs(extraction_path, exist_ok=True)
        # extract files from zip archive
        params = [(f, src_path, extraction_path) for f in tnx_files]
        ConcurrentUtil.run_in_separate_processes(self.extract_files_from_zip, params)
        return extraction_path

    def extract_files_from_zip(self, filename: str, archive_name: str, extraction_path: str):
        self.logger.debug(f'Extract {filename} from {archive_name} to {extraction_path}')
        with zipfile.ZipFile(archive_name, 'r') as zip_ref:
            if filename not in zip_ref.namelist():
                print(f'Process {os.getpid()}: archive {archive_name} does not contain {filename}')
                return
            zip_ref.extract(filename, extraction_path)
        self.logger.debug(f'{filename} extracted')
