from typing import Optional

import pandas as pd

from src.datamodel.DataColumns import CommonColumns as cc
from src.repository.BaseDbRepository import BaseDbRepository
from src.util.ConcurrentUtil import ConcurrentUtil


class PatientRepository(BaseDbRepository):

    def get_patients_info(self, patients, columns):
        self.logger.debug(f'get_patients_info: columns={columns}')
        self.db_manager.open_ssh_tunnel()
        date_cols = [c for c in columns if c in cc.date_columns]
        if not date_cols:
            date_cols = None
        df = self.db_manager.request_patient_info(patients, columns, parse_dates=date_cols)
        self.db_manager.close_ssh_tunnel()
        if df.empty:
            return None

        df = df.drop_duplicates()
        return df

    def get_dead_patients(
            self, columns: Optional[list] = None, date_patient_map: Optional[dict] = None
    ) -> Optional[pd.DataFrame]:
        self.logger.debug(f'get_dead_patients: columns={columns}, '
                          f'date_patient_map length = {len(date_patient_map)}')
        patient_groups = self._group_patient_params(date_patient_map) if date_patient_map else[None]
        params = [(patient_info, columns) for patient_info in patient_groups]

        self.db_manager.open_ssh_tunnel()
        res_dfs = ConcurrentUtil.do_async_job(self.db_manager.request_dead_patients, params)
        self.db_manager.close_ssh_tunnel()

        if all([r is None for r in res_dfs]):
            return None
        df = pd.concat(res_dfs)

        return None if df.empty else df
