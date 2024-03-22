import logging
from abc import ABCMeta
from typing import Optional

import numpy as np
import pandas as pd

from src.datamodel.CodeFormat import CodeFormat
from src.db.DatabaseManager import DatabaseManager
from src.util.ConcurrentUtil import ConcurrentUtil
from src.datamodel.Event import Event
from src.datamodel.DataColumns import CommonColumns as cc


class BaseDbRepository(metaclass=ABCMeta):

    def __init__(self, db_manager: DatabaseManager):
        self.logger = logging.getLogger(type(self).__name__)
        self.db_manager = db_manager

    def _get_codes_info(self, event: Event, columns: list, date_patient_map: Optional[dict] = None,
                        include_icd9: bool = False, first_match: bool = False) -> Optional[pd.DataFrame]:
        """
        Get codes info
        Codes can have two formats:
        plain format (e.g. 'T31.2') means only exactly matched codes will be selected
        regex format (e.g. 'T31.2*') means all codes started from 'T31.2' will be selected (e.g. 'T31.2','T31.23' etc.)
        negative format (e.g. '~T31.2') means all codes except 'T31.2' will be selected
        All codes will be converted to its bases.
        For example:
        input codes ['T31', 'T31.3*']
        next codes will be selected ['T31','T31.3','T31.30','T31.31','T31.32','T31.33']
        (no other codes started from 'T31.3' exist)
        Then obtained codes will be converted to ['T31', 'T31.3']
        where 'T31.3' will contain all items of ['T31.3','T31.30','T31.31','T31.32','T31.33']
        :param event: event object with parameters to search records
        :param columns: list of columns to request
        :param date_patient_map: map of (start_date, end_date) on patients ids to get codes for
        :param include_icd9: default is False. If True than for all ICD10 code their ICD9 analogs will be found and
        ignoring this parameter
        their info will be added to the result. In the end all ICD9 codes will be converted to the corresponded ICD10.
        WARNING!!! Initial ICD9 codes will not be converted to ICD10 during the event search
        :param first_match: get only first (earliest) fitted record for each patient
        :return: dataframe from table with columns or None
        """
        self.logger.debug(f'_get_codes_info: codes = {event.codes}')

        # define counter column if it necessary
        patient_groups = self._group_patient_params(date_patient_map) if date_patient_map else [None]

        params = [(event.codes, event.get_data_table(), columns, patient_info, include_icd9, first_match,
                   event.negation, event.include_subcodes)
                  for patient_info in patient_groups]

        self.db_manager.open_ssh_tunnel()
        res_dfs = ConcurrentUtil.do_async_job(self.__get_code_info_job, params)
        self.db_manager.close_ssh_tunnel()
        self.logger.debug(f'concat result of {len(res_dfs)}')
        df = pd.concat(res_dfs) \
            if res_dfs and any([x is not None for x in res_dfs]) \
            else None
        self.logger.debug(f'return codes info for {event.id} with {df.shape} records')
        return df

    def __get_code_info_job(self, codes: Optional[list], table_name: str, columns: list, patients_info: list,
                            include_icd9: bool, first_match: bool, negation_event: bool = False,
                            include_subcodes: bool = False) -> Optional[pd.DataFrame]:
        self.logger.debug(f'__get_code_info_job: table={table_name}')
        if not codes:
            return self.__get_all_codes_info(table_name, columns, patients_info, first_match)

        df = self.__process_positive_event_codes(
            codes=codes, table_name=table_name, columns=columns,
            include_subcodes=include_subcodes, patients_info=patients_info,
            first_match=first_match, include_icd9=include_icd9
        )

        if negation_event:
            df = self.__process_negative_codes(positive_codes_df=df, patients_info=patients_info, codes=codes)
        self.logger.debug(f'finish with code info for {codes}')
        return df

    def __get_all_codes_info(
            self, table_name: str, columns: list, patients_info: Optional[list] = None, first_match: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Get all codes info
        :param table_name: table with codes
        :param columns: list of columns to request
        :param patients_info: list of lists of tuples with min date, max date and list of patients ids for db request
        :param first_match: get only first (earliest) fitted record for each patient
        :return: dataframe from table with columns or None
        """
        self.logger.debug(f'_get_all_codes_info: table name = {table_name}')
        df = self.db_manager.request_code_info(
            codes=None, table=table_name, columns=columns,
            include_subcodes=False, patients_info=patients_info, first_match=first_match
        )
        return df

    def __get_icd9_mapped_code_info(self, icd10_codes: list, table_name: str, columns: list, patients_info: list,
                                    first_match: bool, include_subcodes: bool) -> Optional[pd.DataFrame]:
        self.logger.debug(f'__get_icd9_mapped_code_info: codes={icd10_codes}')
        if not icd10_codes:
            return pd.DataFrame()

        icd10_subcodes = self.db_manager.request_subcodes(icd10_codes, table_name) if include_subcodes else icd10_codes
        if icd10_subcodes:
            icd10_codes = icd10_subcodes

        icd10_to_icd9_map_df = self.db_manager.request_icd9_icd10_map(icd10_codes,
                                                                      search_column=cc.icd10_code)
        if icd10_to_icd9_map_df is None or icd10_to_icd9_map_df.empty:
            self.logger.warning('No data')
            return pd.DataFrame()

        mapped_icd9_codes = icd10_to_icd9_map_df[cc.icd9_code].unique().tolist()
        icd9_df = self.db_manager.request_code_info(
            table=table_name, columns=columns, codes=mapped_icd9_codes, include_subcodes=False,
            patients_info=patients_info, first_match=first_match
        )
        if icd9_df is None:
            return None
        if cc.code in icd9_df.columns:
            # convert ICD9 to corresponded ICD10
            icd9_df = icd9_df.merge(icd10_to_icd9_map_df, 'inner', left_on=cc.code, right_on=cc.icd9_code)
            icd9_df[cc.code] = icd9_df[cc.icd10_code]
            icd9_df = icd9_df.drop(columns=[cc.icd9_code, cc.icd10_code]).drop_duplicates()

        return icd9_df

    def _group_patient_params(self, date_patient_map: dict, cohort_size: int = 10_000) -> list:
        """
        create list of list of tuples with min and max date for each patients' list.
        Each list of tuples won't be lower than cohort size
        :param date_patient_map: map {(min_date, max_date): list of patient ids}
        :param cohort_size: minimal size of grouping.
        return: list of lists of tuples
        """
        self.logger.debug(f'group patients for request')
        res = []
        counter = 0
        curr_patients = []
        for dates, patients in date_patient_map.items():
            min_date = None
            max_date = None
            if dates is not None:
                if dates[0] is not None:
                    min_date = dates[0].strftime('%Y-%m-%d')
                if dates[1] is not None:
                    max_date = dates[1].strftime('%Y-%m-%d')

            if len(patients) > 1.5 * cohort_size:
                # separate patients for the same time period
                res.append([(min_date, max_date, patients[i:i + cohort_size])
                            for i in range(0, len(patients), cohort_size)])
            else:
                curr_patients.append((min_date, max_date, patients))
                counter += len(patients)
                if counter >= cohort_size:
                    res.append(curr_patients)
                    curr_patients = []
                    counter = 0
        if curr_patients:
            res.append(curr_patients)
        return res

    def __process_negative_codes(
            self, positive_codes_df: pd.DataFrame, patients_info: list, codes: list
    ) -> Optional[pd.DataFrame]:
        # Process negative codes together. If at least one negative code is found at previous level patient record
        # exclude it record at this event. All codes come from the single event
        self.logger.debug(f'process_negative_codes')

        positive_codes_df[cc.date] = pd.to_datetime(positive_codes_df[cc.date].astype(str))

        patient_date_range_df = self.__build_dataframe_from_patient_info(patients_info)

        col_within_time = 'within_time'
        col_min_date = 'min_date'
        col_max_date = 'max_date'

        patient_date_range_df = patient_date_range_df.merge(positive_codes_df, how='left', on=cc.patient_id)
        patient_date_range_df[col_within_time] = \
            (patient_date_range_df[cc.date] >= patient_date_range_df[col_min_date]) & \
            (patient_date_range_df[cc.date] <= patient_date_range_df[col_max_date])
        patient_date_range_df[col_within_time] = patient_date_range_df.groupby(
            [cc.patient_id, col_min_date, col_max_date]
        )[col_within_time].transform('sum')
        patient_date_range_df = patient_date_range_df[patient_date_range_df[col_within_time] == 0] \
            .drop_duplicates(subset=[cc.patient_id, col_max_date]) \
            .drop(columns=[cc.date, col_within_time])
        patient_date_range_df = patient_date_range_df.rename(columns={col_max_date: cc.date})
        patient_date_range_df = patient_date_range_df[positive_codes_df.columns]
        patient_date_range_df[cc.code] = CodeFormat.simple_to_negative(codes)
        # clean all columns except patient/code/date
        clear_cols = [x for x in patient_date_range_df.columns
                      if x not in [cc.patient_id, cc.code, cc.date]]
        patient_date_range_df[clear_cols] = np.nan
        return patient_date_range_df

    def __build_dataframe_from_patient_info(self, patients_info: list) -> pd.DataFrame:
        self.logger.debug('__build_dataframe_from_patient_info')
        patients_data = list()
        max_date_data = list()
        min_date_data = list()
        for min_date, max_date, patients in patients_info:
            patients_data.extend(patients)
            max_date_data.extend([max_date] * len(patients))
            min_date_data.extend([min_date] * len(patients))
        df = pd.DataFrame()
        df[cc.patient_id] = patients_data
        df['min_date'] = pd.Series(data=min_date_data, dtype='datetime64[ns]')
        df['max_date'] = pd.Series(data=max_date_data, dtype='datetime64[ns]')

        return df

    def __process_positive_event_codes(self, codes: Optional[list], table_name: str, columns: Optional[list] = None,
                                       include_subcodes: bool = False, patients_info: Optional[list] = None,
                                       first_match: bool = False, include_icd9: bool = False) -> Optional[pd.DataFrame]:
        self.logger.debug(f'process_positive_event_codes for codes {codes}')
        df = self.db_manager.request_code_info(
            codes=codes, table=table_name, columns=columns,
            include_subcodes=include_subcodes, patients_info=patients_info,
            first_match=first_match
        )

        if include_icd9:
            icd9_df = self.__get_icd9_mapped_code_info(
                icd10_codes=codes, table_name=table_name, columns=columns, patients_info=patients_info,
                first_match=first_match, include_subcodes=include_subcodes
            )
            if icd9_df is not None:
                df = pd.concat([df, icd9_df]).drop_duplicates()

        # convert all codes to base ones determined in config if subcodes flag is True and result df is not empty
        if include_subcodes and (df is not None) and (not df.empty) and (cc.code in df.columns):
            df = self.__convert_to_base_codes(df, codes)

        return df

    def __convert_to_base_codes(self, df: pd.DataFrame, codes: list) -> pd.DataFrame:
        self.logger.debug(f'_convert_to_base_codes: codes list={codes}')
        for bc in codes:
            df[bc] = np.where(df[cc.code].str.startswith(bc), bc, pd.NA)
        df[cc.code] = df[codes].agg(list, axis=1)
        df = df.explode(cc.code)
        df = df.drop(columns=codes).dropna().drop_duplicates()
        return df


