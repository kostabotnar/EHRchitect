import logging

from src.db.DatabaseManager import DatabaseManager
from src.db.SqlDataElement import SqlTable, SqlColumn


class CreateSqlTablesStructure:
    def __init__(self, db_manager: DatabaseManager):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_manager = db_manager

    def execute(self, db_name: str, tables: list[SqlTable]) -> list[SqlTable]:
        self.logger.debug(f'Execute. DB: {db_name}, tables: {[t.name for t in tables]}')
        self.db_manager.open_ssh_tunnel()
        self.db_manager.create_db(db_name)
        tables = self.__order_tables(tables)
        for t in tables:
            self.db_manager.create_table(t)
        self.db_manager.close_ssh_tunnel()
        return tables

    def __order_tables(self, tables: list[SqlTable]) -> list[SqlTable]:
        self.logger.debug(f'__order_tables. tables: {[t.name for t in tables]}')
        if len(tables) == 0:
            return []
        fk_tables_names = {c.foreign_key.ref_table_name for t in tables for c in t.columns if c.foreign_key is not None}
        fk_tables = [t for t in tables if t.name in fk_tables_names]
        fk_tables = self.__order_tables(fk_tables)
        no_fk_tables = [t for t in tables if t.name not in fk_tables_names]
        res_tables = fk_tables + no_fk_tables
        self.logger.debug(f'result order: {[t.name for t in res_tables]}')
        return res_tables

    def create_code_map_table(self, db_name: str, file_path: str) -> SqlTable:
        self.logger.debug(f'create_code_map_table {db_name} from {file_path}')
        cols = [SqlColumn('icd9_code', "VARCHAR", 100, False, False, True),
                SqlColumn('icd10_code', "VARCHAR", 100, False, False, True),
                SqlColumn('code_description', "VARCHAR", 1200, False, False, False)]
        map_table = SqlTable(name='icd9_map_icd10', src=file_path, columns=cols)

        self.db_manager.open_ssh_tunnel()
        self.db_manager.create_db(db_name)
        self.db_manager.create_table(map_table)
        self.db_manager.close_ssh_tunnel()
        return map_table
