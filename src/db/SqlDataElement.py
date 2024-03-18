from dataclasses import dataclass
from typing import Optional

from src.datamodel.DataColumns import DataDictionaryColumns as dd

import pandas as pd


@dataclass
class ForeignKey:
    column_name: str
    ref_table_name: str
    ref_column_name: str

    def __str__(self):
        return f'FOREIGN KEY ({self.column_name}) REFERENCES {self.ref_table_name} ({self.ref_column_name}) ' \
               f'ON DELETE SET NULL'


class SqlColumn:
    int16_cols = {
        "line",
        "cohort_number",
        "total_number_HCOs",
        "column_count",
        "year_of_birth",
        "month_year_death",
        "cohort_number"
    }
    int64_cols = {
        "total_patient_records",
        "total_number_unique_patients",
        "row_count",
        # datetime columns
        "date",
        "start_date",
        "end_date",
        "date_created",
        "test_date",
        "diagnosis_date",
        "oncology_treatment_start_date",
        "observation_date"
    }
    float_cols = {
        "lab_result_num_val",
        "value"
    }

    def __init__(self, name: str, data_type: str, length: int, is_nullable: bool, is_primary_key: bool,
                 is_index: bool, foreign_key: str = None):
        self.name = name.strip()
        self.type = data_type
        self.dtype = self.__get_dtype()
        self.length = None if str(length) == 'nan' else str(length)
        self.is_nullable = is_nullable
        self.is_pk = is_primary_key
        self.is_index = is_index
        self.foreign_key = self.parse_foreign_key(foreign_key)

    def __get_dtype(self):
        if self.name in self.int64_cols:
            return pd.Int64Dtype()
        if self.name in self.int16_cols:
            return pd.UInt16Dtype()
        if self.name in self.float_cols:
            return float
        return str

    def parse_foreign_key(self, foreign_key: Optional[str]) -> Optional[ForeignKey]:
        if (foreign_key is None) or (dd.fk_separator not in foreign_key):
            return None
        column, table = foreign_key.split(dd.fk_separator)
        column, table = column.strip(), table.strip()
        if len(column) == 0 or len(table) == 0:
            return None
        return ForeignKey(self.name, table, column)


class SqlTable:
    def __init__(self, name: str, src: Optional[str], columns: list[SqlColumn]):
        self.name = name
        self.src_file = src
        self.columns = columns

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column_types(self) -> list[str]:
        return [f'{c.type}({c.length})' if c.length is not None else c.type
                for c in self.columns]

    def primary_keys(self) -> list[str]:
        return [c.name for c in self.columns if c.is_pk]

    def foreign_keys(self):
        return [c.foreign_key for c in self.columns if c.foreign_key is not None]

    def foreign_key_names(self):
        return [c.foreign_key.column_name for c in self.columns if c.foreign_key is not None]

    def indexes(self):
        return [c.name for c in self.columns if c.is_index]

    def get_dtypes(self):
        # due to values inconsistency all columns read as string
        return {c.name: c.dtype for c in self.columns}
