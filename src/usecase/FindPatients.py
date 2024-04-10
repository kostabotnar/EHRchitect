import logging
from typing import Optional

import pandas as pd

from src.datamodel.ExperimentConfig import ExperimentConfig, ExperimentLevel
from src.datamodel.DataColumns import CommonColumns as cc
from src.repository.EventRepository import EventRepository
from src.repository.PatientRepository import PatientRepository
from src.usecase.GetEventData import GetEventData
from src.util.FileProvider import FileProvider


class FindPatients:

    def __init__(self, patient_repo: PatientRepository, event_repo: EventRepository):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__patient_repo = patient_repo
        self.__event_repo = event_repo
        self.__file_provider = FileProvider()
        self.logger.debug('Created')

    def execute(self, study_config: Optional[ExperimentConfig] = None,
                include_icd9: bool = True) -> Optional[list]:
        """
        Find all patients for index level and split them on groups
        """
        self.logger.debug(f'execute: experiment_config = {study_config} include_icd9 = {include_icd9}')

        if None in study_config.levels:
            return None

        index_patients = self.__get_index_patients(study_config.levels[0], include_icd9)
        patients_df = self.__get_patients_metadata(index_patients)
        patients_df = patients_df.sort_values(by=[cc.patient_id], ascending=[True]).set_index(cc.patient_id)

        file_dir, file_name = self.__file_provider.patients_metadata_file_location(study_config.outcome_dir)
        self.__file_provider.save_dataframe_file(df=patients_df, file_dir=file_dir, filename=file_name)

        patient_groups = self.__split_patients_on_groups(index_patients)
        return patient_groups

    def __split_patients_on_groups(self, index_patients: list) -> list:
        # split patients by the first character of their id. Maximum there will be 16 groups [0..f]
        res = {}
        for p in index_patients:
            if p[0] in res:
                res[p[0]].append(p)
            else:
                res[p[0]] = [p]
        return list(res.values())

    def __get_index_patients(self, index_level: ExperimentLevel, include_icd9: bool = True) -> Optional[list]:
        self.logger.debug(f'get index event patients for {len(index_level.events)} events')
        columns = [cc.patient_id, cc.code, cc.date]
        res_data = GetEventData(self.__patient_repo, self.__event_repo) \
            .execute(index_level, columns, None, None, include_icd9)
        if res_data is None or res_data.empty:
            self.logger.debug('No patients were found')
            return None
        patients = res_data[cc.patient_id].unique().tolist()
        self.logger.debug(f'{len(patients)} patients with index event were found')
        return patients

    def __get_patients_metadata(self, index_patients: list) -> pd.DataFrame:
        self.logger.debug(' grt patients metadata')
        columns = [cc.patient_id, cc.date_of_birth, cc.sex, cc.race, cc.ethnicity, cc.date_of_death]
        df = self.__patient_repo.get_patients_info(index_patients, columns)
        return df
