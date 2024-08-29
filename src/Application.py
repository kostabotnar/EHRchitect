import logging.config
import shutil

from src.config.AppConfig import AppConfig
from src.datamodel.ExperimentConfig import ExperimentConfig
from src.db.DatabaseManager import DatabaseManager
from src.repository.CodeDescriptionRepository import CodeDescriptionRepository
from src.repository.EventRepository import EventRepository
from src.repository.PatientRepository import PatientRepository
from src.usecase import ImportDataToDB
from src.usecase.BuildEventsMetadata import BuildEventsMetadata
from src.usecase.ConvertDataModel import ConvertDataModel
from src.usecase.CreateSqlTablesStructure import CreateSqlTablesStructure
from src.usecase.DownloadTnxDataset import DownloadTnxDataset
from src.usecase.FindEventsChain import FindEventsChain
from src.usecase.FindPatients import FindPatients
from src.usecase.ParseDataDictionary import ParseDataDictionary
from src.usecase.ReadChainConfig import ReadChainConfig
from src.util.ConcurrentUtil import ConcurrentUtil
from src.util.FileProvider import FileProvider

fp = FileProvider()
logging.config.fileConfig(fp.log_config_file)
logger = logging.getLogger('Main')


def create_db(db_name: str, url: str, archive: str, local_access: bool, new_db: bool, set_index: bool, drop_csv: bool):
    logger.debug('======Start======')
    if (url is not None) and (not download_dataset(url, archive)):
        return
    data_path = ConvertDataModel().execute(archive, 'tnx')
    tables_data = ParseDataDictionary().execute(data_path)

    app_config = init_app_config()
    db_manager = DatabaseManager(app_config, db_name=db_name, local_access=local_access)

    # crete MySQL DB
    code_map_table = None
    if new_db:
        table_creator = CreateSqlTablesStructure(db_manager)
        tables_data = table_creator.execute(db_name, tables_data)
        code_map_table = table_creator.create_code_map_table(db_name, fp.code_map_table_file)

    # upload data to the DB
    if code_map_table is not None:
        ImportDataToDB.import_code_mapping_data(db_manager, code_map_table)
    ImportDataToDB.execute(app_config, local_access, db_name, tables_data, archive, set_index)
    # remove temp data files
    if drop_csv:
        logger.debug(f'Delete data model csv files {data_path}')
        shutil.rmtree(data_path)
    logger.debug('======Finish======')


def run_study(db_name: str, out_dir: str, study_list: list, local_db: bool = True):
    logger.debug('======Run Study Data Selection======')
    logger.debug(f'DB: {db_name}, out dir: {out_dir}, study list: {study_list}')
    app_config = init_app_config()
    db_manager = DatabaseManager(app_config)
    dbs = db_manager.list_databases()
    if db_name not in dbs:
        logger.error(f'Database {db_name} not found in the config.\n'
                     f'Available databases: {dbs}')
        return
    db_manager.database_name = db_name
    db_manager.local_access = local_db
    # db_manager = DatabaseManager(app_config, db_name=db_name, local_access=local_db)

    fp.set_result_path(out_dir)
    event_repo = EventRepository(db_manager)
    patient_repo = PatientRepository(db_manager)
    cd_repo = CodeDescriptionRepository(db_manager)
    include_icd9 = True  # todo: make this flag an event parameter

    for config_file_name in study_list:
        logger.debug(f'RUN CONFIG {config_file_name}')
        study_config = ReadChainConfig().execute(config_file_name)
        create_study_outcome_file_structure(study_config)

        patient_groups = FindPatients(patient_repo, event_repo).execute(study_config, include_icd9)
        if not patient_groups:
            logger.warning(f'No patients found for study {study_config.name}')
            continue

        BuildEventsMetadata(cd_repo).execute(study_config)

        params = [(app_config, pg, study_config, db_name, include_icd9, local_db) for pg in enumerate(patient_groups)]
        ConcurrentUtil.run_in_separate_processes(find_event_chain_async, params)

        logger.debug(f'FINISH {config_file_name}')

    logger.debug('======Finish Study Data Selection======')


def download_dataset(url: str, archive: str):
    logger.debug(f'Download dataset {url} to {archive}')
    if url is None or archive is None:
        logger.error(f'Download error: url or archive is not defined.')
        return False
    result = DownloadTnxDataset().execute(url, archive)
    if result is not None:
        logger.error(f'Download error: {result}.')
        return False

    return True


def init_app_config():
    logger.debug('Init app config')
    with open(fp.app_config_file) as f:
        lines = f.readlines()
    data = ''.join(lines)
    return AppConfig.from_json(data)


def create_study_outcome_file_structure(study_config: ExperimentConfig):
    logger.debug('Create Outcome File Structure')
    study_res_full_path = fp.get_result_file_path(study_config.outcome_dir)
    study_res_full_path.mkdir(parents=True, exist_ok=True)
    for i in range(len(study_config.levels)):
        e_path = fp.get_events_path(outcome_dir=study_config.outcome_dir, level_number=i)
        e_path.mkdir(parents=True, exist_ok=True)
        if i == 0:
            continue
        t_path = fp.get_transitions_path(outcome_dir=study_config.outcome_dir, end_level_number=i)
        t_path.mkdir(parents=True, exist_ok=True)


def find_event_chain_async(app_config: AppConfig, patient_group: tuple, experiment_config: ExperimentConfig,
                           db_name: str, include_icd9: bool, local_db: bool):
    db_manager = DatabaseManager(app_config, db_name=db_name, local_access=local_db)
    event_repo = EventRepository(db_manager)
    patients_repo = PatientRepository(db_manager)
    cd_repo = CodeDescriptionRepository(db_manager)

    FindEventsChain(patients_repo, event_repo, cd_repo).execute(
        patient_group=patient_group,
        experiment_config=experiment_config,
        include_icd9=include_icd9
    )
