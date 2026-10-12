import logging
from typing import List, Tuple

import pandas as pd

from src.datamodel.DataColumns import DataDictionaryColumns as dd
from src.db.SqlDataElement import SqlTable, SqlColumn
from src.util.FileProvider import FileProvider


class ParseDataDictionary:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fp = FileProvider()
        self.datadictionary = pd.read_csv(self.fp.data_dictionary_file)

    def execute(self, files_path: str) -> List[SqlTable]:
        self.logger.debug("Execute")
        df = self.datadictionary.copy()
        df[dd.foreign_key] = df[dd.foreign_key].fillna("")
        tables = []
        for t in df[dd.table_name].unique():
            src_file_name = df[df[dd.table_name] == t][dd.file_name].values[0]
            cols = [
                SqlColumn(
                    name=r[dd.column_name],
                    data_type=r[dd.data_type],
                    length=r[dd.length],
                    is_nullable=r[dd.nullable] == dd.val_yes,
                    is_primary_key=r[dd.primary_key] == dd.val_yes,
                    is_index=r[dd.index] == dd.val_yes,
                    foreign_key=r[dd.foreign_key],
                )
                for i, r in df[df[dd.table_name] == t].iterrows()
            ]
            table = SqlTable(name=t, src=f"{files_path}/{src_file_name}", columns=cols)
            if table.name is not None:
                tables.append(table)
        self.logger.debug(f"Parsed Tables: {[t.name for t in tables]}")
        return tables

    def get_read_options(
        self, table_name: str = None, file_name: str = None
    ) -> Tuple[dict, list]:
        """
        Collects pandas read_csv options for a common data model table from the data dictionary.
        Without them pandas infers types per chunk, which mixes str and int in the same column
        and breaks both the parquet conversion and codes with leading zeros.
        :param table_name: common data model table name
        :param file_name: common data model file name, used when table_name is not set
        :return: tuple of the column dtype map and the list of date columns
        """
        if table_name is not None:
            rows = self.datadictionary[self.datadictionary[dd.table_name] == table_name]
        elif file_name is not None:
            rows = self.datadictionary[self.datadictionary[dd.file_name] == file_name]
        else:
            raise ValueError("Either table_name or file_name has to be set")

        if rows.empty:
            self.logger.warning(
                f"Data dictionary has no entry for table {table_name} / file {file_name}. "
                f"Column types will be inferred by pandas"
            )

        dtypes = {
            r[dd.column_name]: dd.pandas_dtypes[r[dd.data_type]]
            for _, r in rows.iterrows()
            if r[dd.data_type] in dd.pandas_dtypes
        }
        date_columns = (
            rows.loc[rows[dd.data_type].isin(dd.date_data_types), dd.column_name]
            .unique()
            .tolist()
        )
        return dtypes, date_columns
