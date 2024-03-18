import datetime
import logging.config
import shutil

from src.config.AppConfig import AppConfig
from src.db.DatabaseManager import DatabaseManager
from src.usecase import ImportDataToDB
from src.usecase.ConvertDataModel import ConvertDataModel
from src.usecase.CreateSqlTablesStructure import CreateSqlTablesStructure
from src.usecase.DownloadTnxDataset import DownloadTnxDataset
from src.usecase.ParseDataDictionary import ParseDataDictionary
from src.util.FileProvider import FileProvider

fp = FileProvider()
logging.config.fileConfig(fp.log_config_file)
logger = logging.getLogger('Main')


def create_db(database: str, url: str, archive: str, local_access: bool, new_db: bool, set_index: bool, drop_csv: bool):
    logger.debug('======Start======')
    download_dataset(url, archive)
    data_path = ConvertDataModel().execute(archive, 'tnx')
    tables_data = ParseDataDictionary().execute(data_path)

    app_config = init_app_config()
    db_manager = DatabaseManager(app_config, db_name=database, local_access=local_access)

    # crete MySQL DB
    code_map_table = None
    if new_db:
        table_creator = CreateSqlTablesStructure(db_manager)
        tables_data = table_creator.execute(database, tables_data)
        code_map_table = table_creator.create_code_map_table(database, fp.code_map_table_file)

    # upload data to the DB
    if code_map_table is not None:
        ImportDataToDB.import_code_mapping_data(db_manager, code_map_table)
    ImportDataToDB.execute(app_config, local_access, database, tables_data, archive, set_index)
    # remove temp data files
    if drop_csv:
        logger.debug(f'Delete data model csv files {data_path}')
        shutil.rmtree(data_path)

    logger.debug('======Finish======')


def run_study():
    pass


def init_app_config():
    logger.debug('Init app config')
    with open(fp.app_config_file) as f:
        lines = f.readlines()
    data = ''.join(lines)
    return AppConfig.from_json(data)


def download_dataset(url: str, archive: str, default_name: str = None):
    logger.debug(f'Download dataset {url} to {archive}')
    if url is not None:
        # download dataset archive
        if archive is None:
            # compose archive name
            archive = f'./{fp.data_archive_path}/{default_name}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
        result = DownloadTnxDataset().execute(url, archive)
        if result is not None:
            logger.error(f'Download error: {result}.')
            return
