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


def upload_med_drug_chunk(md_codes: list, app_config: AppConfig, local_access: bool, database: str,
                          res_table_name: str):
    print(f'Process {os.getpid()} : Upload med drug data chunk size {len(md_codes)}')

    db_manager = DatabaseManager(app_config, local_access=local_access, db_name=database)
    db_manager.open_ssh_tunnel()

    columns = ['code', 'code_system', 'unique_id']
    df_md = db_manager.read_med_drug_data('medication_drug', columns, md_codes)
    unique_ids = df_md['unique_id'].unique().tolist()

    columns = ['code', 'code_system', 'unique_id']
    df_mi = db_manager.get_med_ingredients_by_unique_id('medication_ingredient', columns, unique_ids)
    mi_codes_list = [(r[1]['code'], r[1]['code_system'])
                     for r in df_mi[['code', 'code_system']].drop_duplicates().iterrows()]

    columns = ['code', 'code_system', 'code_description']
    df_st = db_manager.get_codes_description(columns, mi_codes_list)

    df_mi = df_mi.merge(df_st, on=['code', 'code_system'])
    df_md = df_md.merge(df_mi, on='unique_id', suffixes=('_md', '_mi'))
    df_md = df_md[['code_md', 'code_mi', 'code_system_md', 'code_description']].drop_duplicates()
    df_md = df_md.rename(columns={'code_md': 'md_code', 'code_mi': 'mi_code', 'code_system_md': 'md_code_system'})

    res = db_manager.upload_df_to_sql(df_md, res_table_name)
    print(f'Process {os.getpid()} : upload med drug ingredient successful: {res}')

    db_manager.close_ssh_tunnel()
    return res


def fill_med_drug_description_table(app_config, local_access, database, table, set_index: bool):
    print(f'fill in medication drug description table')
    chunksize = 1000

    db_manager = DatabaseManager(app_config, local_access=local_access, db_name=database)
    db_manager.open_ssh_tunnel()
    md_codes = db_manager.get_med_drug_codes()
    db_manager.close_ssh_tunnel()

    params = [(md_codes[i:i + chunksize], app_config, local_access, database, table.name)
              for i in range(0, len(md_codes), chunksize)]
    res = ConcurrentUtil.run_in_separate_processes(upload_med_drug_chunk, params)

    print(f'med drug description successfully uploaded {sum(res)} out of {len(res)}')
    if set_index:
        db_manager.create_indexes(table)
    db_manager.close_ssh_tunnel()
    return None
