import os

from src.config.AppConfig import AppConfig
from src.db.DatabaseManager import DatabaseManager
from src.db.SqlDataElement import SqlTable
from src.util.ConcurrentUtil import ConcurrentUtil


def execute(app_config: AppConfig, local_access: bool, database: str, tables: list[SqlTable], archive_name: str,
            set_index: bool):
    print(f'Process {os.getpid()} Main: Execute for {len(tables)} tables')

    if not os.path.exists(archive_name):
        print(f'Process {os.getpid()} Main: File {archive_name} was not found')
        return

    # upload patients table first, because all others depends on it
    pt = [t for t in tables if t.name == 'patient']
    if len(pt) == 1:
        tables.remove(pt[0])
        upload_table_process(app_config, local_access, database, pt[0], archive_name, set_index)

    # upload all other tables in separate processes
    params = [(app_config, local_access, database, t, archive_name, set_index) for t in tables]
    ConcurrentUtil.run_in_separate_processes(upload_table_process, params, max_processes=len(params))


def upload_table_process(app_config: AppConfig, local_access: bool, database: str, table: SqlTable,
                         archive_name: str, set_index: bool):
    print(f'Process {os.getpid()} table {table.name}: Run thread for table {table.name}')
    if table.src_file[-4:] != '.csv':
        return
    print(f'Process {os.getpid()} table {table.name}: Read data file {table.src_file} from {archive_name}')

    db_manager = DatabaseManager(app_config, local_access=local_access, db_name=database)
    db_manager.open_ssh_tunnel()
    db_manager.upload_file_to_sql(table.src_file, table.name)
    # create indexes for the table
    if set_index:
        db_manager.create_indexes(table)
    db_manager.close_ssh_tunnel()


def import_code_mapping_data(db_manager: DatabaseManager, table: SqlTable):
    print(f'import code mapping data')
    if table.src_file[-4:] != '.csv':
        return
    print(f'Process {os.getpid()} table {table.name}: Read data file {table.src_file}')
    db_manager.open_ssh_tunnel()
    db_manager.upload_file_to_sql(table.src_file, table.name)
    db_manager.create_indexes(table)
    db_manager.close_ssh_tunnel()

