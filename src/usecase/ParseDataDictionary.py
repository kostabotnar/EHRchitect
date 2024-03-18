import logging
from typing import List

import pandas as pd

from src.datamodel.DataColumns import DataDictionaryColumns as dd
from src.db.SqlDataElement import SqlTable, SqlColumn
from src.util.FileProvider import FileProvider


class ParseDataDictionary:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fp = FileProvider()

    def execute(self, files_path: str) -> List[SqlTable]:
        self.logger.debug('Execute')
        df = pd.read_csv(self.fp.data_dictionary_file)
        df[dd.foreign_key] = df[dd.foreign_key].fillna('')
        tables = []
        for t in df[dd.table_name].unique():
            src_file_name = df[df[dd.table_name] == t][dd.file_name].values[0]
            cols = [SqlColumn(
                name=r[dd.column_name], data_type=r[dd.data_type], length=r[dd.length],
                is_nullable=r[dd.nullable] == dd.val_yes,
                is_primary_key=r[dd.primary_key] == dd.val_yes,
                is_index=r[dd.index] == dd.val_yes,
                foreign_key=r[dd.foreign_key]
            )
                for i, r in df[df[dd.table_name] == t].iterrows()]
            table = SqlTable(name=t, src=f'{files_path}/{src_file_name}', columns=cols)
            if table.name is not None:
                tables.append(table)
        self.logger.debug(f'Parsed Tables: {[t.name for t in tables]}')
        return tables
