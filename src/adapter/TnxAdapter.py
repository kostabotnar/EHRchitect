import zipfile
from pathlib import Path

import pandas as pd

from src.adapter.AbstractDataAdapter import AbstractDataAdapter
from src.util.ConcurrentUtil import ConcurrentUtil
from src.datamodel.DataColumns import DataDictionaryColumns as dd_cols, CommonColumns as cc_cols


class MapColumns:
    file = "File"
    table = "Table"
    column = "Column"
    tnx_file = "TNX File"
    tnx_column = "TNX Column"


class TnxAdapter(AbstractDataAdapter):
    data_map_file = "tnx_data_model_map.csv"

    def convert(self, src_path: Path) -> Path:
        conversion_map = pd.read_csv(self.fp.data_path / self.data_map_file)
        tnx_files = conversion_map[MapColumns.tnx_file].unique().tolist()

        src_is_zip = src_path.name.endswith('.zip')
        if src_is_zip:
            src_path = self.extract_files_from_zip_async(src_path, tnx_files)

        self.logger.debug(f'Converting TNX files to common format')
        dest_path = Path(f'{src_path}_ehr_cm')
        params = [(f, src_path, dest_path, conversion_map[conversion_map[MapColumns.tnx_file] == f])
                  for f in tnx_files]
        ConcurrentUtil.run_in_separate_processes(self.convert_tnx_to_common, params)
        return dest_path

    def convert_tnx_to_common(self, file_name: str, src_path: Path, dest_path: Path, table_map: pd.DataFrame):
        self.logger.debug(f'Converting file {file_name} to from {src_path} to {dest_path}')
        if not src_path.exists() or not (src_path / file_name).exists():
            raise FileNotFoundError(f'File {src_path / file_name} not found')
        selected_columns = table_map[MapColumns.tnx_column].tolist()
        columns_map = {tnx_c: cm_c
                       for tnx_c, cm_c in zip(table_map[MapColumns.tnx_column], table_map[MapColumns.column])}

        df = pd.read_csv(src_path / file_name, usecols=selected_columns)
        # rename columns
        df = df.rename(columns=columns_map)
        # Drop duplicates
        df = df.drop_duplicates()
        # Convert column to datetime
        if file_name == 'patient.csv':
            # convert years of birth and death to datetime format
            df[cc_cols.date_of_birth] = pd.to_datetime(df[cc_cols.date_of_birth], format='%Y')
            df[cc_cols.date_of_death] = pd.to_datetime(df[cc_cols.date_of_death], format='%Y%m') + pd.offsets.MonthEnd()
            # drop NA in date of birth
            df = df.dropna(subset=[cc_cols.date_of_birth])
        else:
            table_name = table_map[MapColumns.table].tolist()[0]
            date_columns = [c for c in self.get_tnx_table_date_columns(table_name)]
            for c in date_columns:
                df[c] = pd.to_datetime(df[c], format='%Y%m%d')
        # save to a new CSV file
        out_file_name = table_map[MapColumns.file].tolist()[0]
        if not dest_path.exists():
            dest_path.mkdir(parents=True, exist_ok=True)
        self.logger.debug(f'Saving file {dest_path / out_file_name}')
        df.to_csv(dest_path / out_file_name, index=False)

    def get_tnx_table_date_columns(self, table_name: str) -> list:
        selection = ((self.datadictionary[dd_cols.table_name] == table_name)
                     & ((self.datadictionary[dd_cols.data_type] == "DATETIME")
                        | (self.datadictionary[dd_cols.data_type] == "DATE")))
        return self.datadictionary.loc[selection, dd_cols.column_name].unique().tolist()

    def extract_files_from_zip_async(self, src_path: Path, tnx_files: list) -> {Path}:
        dest_path = Path(str(src_path).replace('.zip', ''))
        self.logger.debug(f'Create a directory {dest_path} for unzipping data')
        dest_path.mkdir(parents=True, exist_ok=True)
        # extract files from zip archive
        params = [(f, src_path, dest_path) for f in tnx_files]
        ConcurrentUtil.run_in_separate_processes(self.extract_files_from_zip, params)
        return dest_path

    def extract_files_from_zip(self, filename: str, archive: Path, extraction_path: Path):
        self.logger.debug(f'Extract {filename} from {archive} to {extraction_path}')
        with zipfile.ZipFile(archive, 'r') as zip_ref:
            if filename not in zip_ref.namelist():
                print(f'Archive {archive} does not contain {filename}')
                return
            zip_ref.extract(filename, extraction_path)
        self.logger.debug(f'{filename} extracted')

