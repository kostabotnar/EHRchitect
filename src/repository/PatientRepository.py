from typing import Optional

import pandas as pd

from src.datamodel.DataColumns import CommonColumns as cc
from src.repository.BaseDbRepository import BaseDbRepository
from src.util.ConcurrentUtil import ConcurrentUtil


class PatientRepository(BaseDbRepository):

    def get_patients_info(self, patients, columns):
        self.logger.debug(f'get_patients_info: columns={columns}')

        df = self.db_manager.request_patient_info(patients, columns)
        if df.empty:
            return None

        if cc.date_of_death in columns:
            df[cc.date_of_death] = pd.to_datetime(df[cc.date_of_death], format='%Y%m%d')
        if cc.date_of_birth in columns:
            df[cc.date_of_birth] = pd.to_datetime(df[cc.date_of_birth], format='%Y%m%d')
        df = df.drop_duplicates()
        return df

    def get_dead_patients(
            self, columns: Optional[list] = None, date_patient_map: Optional[dict] = None
    ) -> Optional[pd.DataFrame]:
        self.logger.debug(f'get_dead_patients: columns={columns}, '
                          f'date_patient_map length = {len(date_patient_map)}')
        patient_groups = self._group_patient_params(date_patient_map) if date_patient_map else[None]
        params = [(patient_info, columns) for patient_info in patient_groups]

        res_dfs = ConcurrentUtil.do_async_job(self.db_manager.request_dead_patients, params)

        if all([r is None for r in res_dfs]):
            return None
        df = pd.concat(res_dfs)

        if df.empty:
            return None
        if cc.date_of_death in columns:
            df[cc.date_of_death] = pd.to_datetime(df[cc.date_of_death], format='%Y%m%d')
        if cc.date_of_birth in columns:
            df[cc.date_of_birth] = pd.to_datetime(df[cc.date_of_birth], format='%Y%m%d')

        return df
