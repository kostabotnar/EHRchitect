import datetime
import logging
from datetime import timedelta
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd

from src.datamodel.CodeFormat import CodeFormat
from src.datamodel.DataColumns import CommonColumns as cc
from src.datamodel.ExperimentConfig import ExperimentConfig, ExperimentLevel, ExperimentTimeFrame, \
    ExperimentTimeInterval, MatchMode
from src.repository.EventRepository import EventRepository
from src.repository.PatientRepository import PatientRepository
from src.repository.CodeDescriptionRepository import CodeDescriptionRepository
from src.usecase.GetEventData import GetEventData
from src.util.FileProvider import FileProvider


class FindEventsChain:
    __col_total_time = 'total_time'
    __col_time_period = 't'

    def __init__(self, patient_repo: PatientRepository, event_repo: EventRepository,
                 cd_repo: CodeDescriptionRepository):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__patient_repo = patient_repo
        self.__event_repo = event_repo
        self.__cd_repo = cd_repo
        self.__file_provider = FileProvider()

    def execute(self, patient_group: tuple,
                experiment_config: Optional[ExperimentConfig] = None,
                include_icd9: bool = True):
        """
        Find chain of events event_1 -> t1 -> event_2 -> t2 -> ... -> event_N, where event_i corresponds to level #i
        Each event within a level contains list of codes and category of codes. Codes in the event's list are considered
        with OR condition.
        For example event.codes=[31.8, 31.9] means that a patient will be searched by having any of these codes.
        Codes can have two formats:
        plain format (e.g. 'T31.2') means only exactly matched codes will be selected
        regex format (e.g. 'T31.2*') means all codes started from 'T31.2' will be selected (e.g. 'T31.2','T31.23' etc.)
        All codes will be converted to its bases after DB search.
        For example:
        input codes ['T31', 'T31.3*']
        next codes will be searched in DB ['T31','T31.3','T31.30','T31.31','T31.32','T31.33']
        (no other codes started from 'T31.3' exist)
        Then obtained codes will be converted to ['T31', 'T31.3']
        where 'T31.3' will contain all items of ['T31.3','T31.30','T31.31','T31.32','T31.33']
        :param patient_group: tuple with number of group and a list of patient ids
        :param experiment_config: object of ExperimentConfig class, that describes config to build chain model
        MatchMode.patient_first_match - first match for patient,
        MatchMode.encounter_start - match code's first date within an encounter
        :param include_icd9: default is True. If True than for all ICD10 code their ICD9 analogs will be found and
        included into the event search. In the end all ICD9 codes will be converted to the corresponded ICD10.
        WARNING!!! ICD9 codes added to the event config manually will not be converted to ICD10 during the event search
        :return: None
        """
        self.logger.debug(f'execute: '
                          f'group number: {patient_group[0]} with {len(patient_group[1])} patients '
                          f'experiment_config = {experiment_config} '
                          f'include_icd9 = {include_icd9}')

        if None in experiment_config.levels:
            return None

        self.__build_events_chain(
            experiment_config, include_icd9, patient_group[1], patient_group[0]
        )

    def __build_events_chain(self, experiment_config: ExperimentConfig, include_icd9: bool, patients: list,
                             pgroup_number: int):
        """
        Get all matched events. Process experiment config by levels to get and match events records with each other.
        For positive events matching is carried out by each code, for negative event - by an event as a whole
        """
        self.logger.debug('__build_events_chain')

        # get all index events for the patients group
        date_patient_map = self.__init_date_patient_map(experiment_config.time_frame, patients)

        chain_df = None
        n_levels = len(experiment_config.levels)
        for i, experiment_level in enumerate(experiment_config.levels):
            curr_level_number = experiment_level.level
            self.logger.debug(f'__build_events_chain: build level {curr_level_number}')
            first_incident = experiment_level.match_mode == MatchMode.first_match
            df = self.__get_events_data(experiment_level, date_patient_map, experiment_config.time_frame, include_icd9,
                                        first_incident)
            if df is None or df.empty:
                self.logger.debug(f'No data for level {curr_level_number}')
                return None
            # calc time interval for each patient for the next level
            if i < n_levels - 1:
                next_level_dist = experiment_config.levels[i + 1].period
                date_patient_map = self.__get_date_patient_map(df, next_level_dist, experiment_config.time_frame)
            # rename columns with level number
            curr_level_columns = [cc.get_column_at_level(c, curr_level_number) for c in df.columns if c != cc.patient_id]
            df.columns = [cc.patient_id] + curr_level_columns
            if curr_level_number == 0:
                chain_df = df
            else:
                # merge with previous levels
                chain_df = self.__merge_levels(index_df=chain_df, target_df=df,
                                               index_level=experiment_config.get_level_by_number(curr_level_number - 1),
                                               target_level=experiment_level)
                # if not possible to connect levels, return None values
                if chain_df is None:
                    return None
                # save current level-to-level interaction
                file_dir, file_name = self.__file_provider.get_transition_group_location(
                    experiment_config.outcome_dir, curr_level_number, pgroup_number
                )
                self.__save_to_file(
                    df=chain_df,
                    file_dir=file_dir,
                    file_name=file_name,
                    index=cc.patient_id)

            curr_level_columns = [c for c in chain_df.columns if c.endswith(f'_{curr_level_number}')]
            # remove previous level to work further only with a new one
            chain_df = chain_df[[cc.patient_id] + curr_level_columns].drop_duplicates()
            # save current level record in a separate file
            file_dir, file_name = self.__file_provider.get_event_group_location(
                experiment_config.outcome_dir, curr_level_number, pgroup_number
            )
            self.__save_to_file(
                df=chain_df,
                file_dir=file_dir,
                file_name=file_name,
                index=cc.patient_id)

    def __get_events_data(self, level: ExperimentLevel, date_patient_map: Optional[dict] = None,
                          etf: ExperimentTimeFrame = None,
                          include_icd9: bool = True, first_incident: bool = False) -> Optional[pd.DataFrame]:
        self.logger.debug(f'__get_events_data: events={level.events}')

        columns = [cc.patient_id, cc.code, cc.date]
        df = GetEventData(self.__patient_repo, self.__event_repo) \
            .execute(level, columns, date_patient_map, etf, include_icd9, first_incident)
        if df is None:
            self.logger.warning('Nothing from events was found')
            return None
        df = df.drop_duplicates()
        return df

    def __match_events(self, index_df: pd.DataFrame, target_df: pd.DataFrame,
                       index_level: ExperimentLevel, target_level: ExperimentLevel) -> Optional[pd.DataFrame]:
        self.logger.debug(f'__match_events: between levels {index_level.level} and {target_level.level}')

        if target_df is None or target_df.empty or index_df is None or index_df.empty:
            # nothing to match
            self.logger.warning(f' Nothing to match between level {index_level.level} and {target_level.level}')
            return None
        # remove all patients from index data that are not in target data
        index_df = index_df[index_df[cc.patient_id].isin(target_df[cc.patient_id])]

        col_target_date = cc.get_column_at_level(cc.date, target_level.level)
        col_target_event_id = cc.get_column_at_level(cc.event_id, target_level.level)
        col_target_code = cc.get_column_at_level(cc.code, target_level.level)

        col_index_date = cc.get_column_at_level(cc.date, index_level.level)
        col_index_event_id = cc.get_column_at_level(cc.event_id, index_level.level)
        col_index_code = cc.get_column_at_level(cc.code, index_level.level)

        if target_level.match_mode == MatchMode.first_match:
            # take only the earliest target records of each patient+event+code
            target_df = target_df.groupby(
                [cc.patient_id, col_target_event_id, col_target_code], as_index=False
            )[col_target_date].min()

        index_cols = index_df.columns.tolist()
        target_cols = [c for c in target_df.columns if c != cc.patient_id]
        col_distance = cc.get_column_at_level(cc.time_interval, index_level.level)
        res_columns_order = index_cols + [col_distance] + target_cols

        # event and target dfs can have millions records and do not fit the memory
        # to avoid memory problem do matching with patients pagination
        patients = index_df[cc.patient_id].unique()
        total_patients = len(patients)
        bin_size = 100
        patients = np.array_split(patients, total_patients // bin_size + 1)
        merge_result = []
        for i, curr_patients in enumerate(patients):
            self.logger.debug(f'match {i * bin_size}..{(i + 1) * bin_size} from {total_patients}')

            curr_index_df = index_df[index_df[cc.patient_id].isin(curr_patients)]
            curr_target_df = target_df[target_df[cc.patient_id].isin(curr_patients)]

            curr_index_df = curr_index_df.merge(curr_target_df, on=cc.patient_id)
            del curr_target_df
            if curr_index_df is None or curr_index_df.empty:
                continue

            curr_index_df.drop_duplicates(inplace=True)
            curr_index_df = self.__filter_events_within_period(
                curr_index_df, index_level, target_level
            )
            if curr_index_df is None or curr_index_df.empty:
                continue

            if target_level.match_mode == MatchMode.first_match:
                # get the earliest index date
                group_col = [cc.patient_id, col_index_event_id, col_index_code, col_target_event_id,
                             col_target_code]
                curr_index_df['first_event'] = curr_index_df.groupby(group_col)[col_index_date].transform('min')
                curr_index_df = curr_index_df[curr_index_df['first_event'] == curr_index_df[col_index_date]]. \
                    drop(columns=['first_event'])

            curr_index_df = curr_index_df[res_columns_order]
            merge_result.append(curr_index_df)

        if not merge_result:
            self.logger.warning('Nothing to match')
            return None
        return pd.concat(merge_result)

    def __filter_events_within_period(self, df: pd.DataFrame, start_level: ExperimentLevel, end_level: ExperimentLevel):
        self.logger.debug(
            f'__filter_events_within_period: start level {start_level.level} end level {end_level.level} '
            f'period={end_level.period}'
        )
        col_code = cc.get_column_at_level(cc.code, end_level.level)
        col_target_event_id = cc.get_column_at_level(cc.event_id, end_level.level)
        col_distance = cc.get_column_at_level(cc.time_interval, start_level.level)
        col_start_date = cc.get_column_at_level(cc.date, start_level.level)
        col_end_date = cc.get_column_at_level(cc.date, end_level.level)

        is_negative_code = df[col_code].str.startswith(CodeFormat.negation_word)
        # exclude negative codes from mask
        time_mask = ~is_negative_code

        df[col_distance] = (df[col_end_date] - df[col_start_date]).dt.days
        # time mask for positive codes
        df_res = []
        for event in end_level.events:
            period = event.period if event.period is not None else end_level.period
            df_event = df[df[col_target_event_id] == event.id]
            if period is None:
                event_time_mask = time_mask & df_event[col_distance] >= 0
            elif type(period) is ExperimentTimeInterval:
                min_t = period.get_min_t_days()
                max_t = period.get_max_t_days()
                event_time_mask = time_mask & (df_event[col_distance] >= min_t) & (df_event[col_distance] <= max_t)
                # for negative events get only records with maximal distance
                event_time_mask = event_time_mask | (is_negative_code & (df_event[col_distance] == max_t))
            else:
                event_time_mask = time_mask & (df_event[col_distance] >= 0) & (df_event[col_distance] <= period)
                # for negative events get only records with maximal distance
                event_time_mask = event_time_mask | (is_negative_code & (df_event[col_distance] == period))

            df_res.append(df_event[event_time_mask])
        return pd.concat(df_res)

    def __filter_first_events_within_encounter(self, df):
        """
        Leave only first presents of the code within encounter
        :param df: data frame
        :return: filtered data frame
        """
        self.logger.debug('__filter_events_with_encounter')
        df['min'] = df.groupby([cc.encounter_id, cc.code])[cc.date].transform('min')
        df = df[df[cc.date] == df['min']].drop(columns=['min', cc.encounter_id])
        return df

    def __filter_first_patient_match(self, df: pd.DataFrame, index_level_number: int,
                                     target_level_number: int) -> pd.DataFrame:
        # get the earliest record of matched code pairs for each patient
        self.logger.debug(f'__filter_first_patient_match '
                          f'between levels {index_level_number} and {target_level_number}')
        index_date_col = cc.get_column_at_level(cc.date, index_level_number)
        index_code_col = cc.get_column_at_level(cc.code, index_level_number)
        target_date_col = cc.get_column_at_level(cc.date, target_level_number)
        target_code_col = cc.get_column_at_level(cc.code, target_level_number)
        first_match_col = "first_match"
        # get first target date for each index date
        df[first_match_col] = df.groupby(
            [cc.patient_id, index_code_col, target_code_col, index_date_col]
        )[target_date_col].transform('min')
        df = df[df[target_date_col] == df[first_match_col]].drop(columns=[first_match_col])
        # get first index date for each target date
        df[first_match_col] = df.groupby(
            [cc.patient_id, index_code_col, target_code_col, target_date_col]
        )[index_date_col].transform('min')
        df = df[df[index_date_col] == df[first_match_col]].drop(columns=[first_match_col])

        df = df.drop_duplicates(subset=[cc.patient_id, index_code_col, index_date_col, target_code_col])
        return df

    def __save_to_file(
            self, df: pd.DataFrame, file_dir: Path, file_name: str, file_format: str = 'parquet', index=None
    ):
        self.logger.debug(f'__save_to_file: {file_name} index={index}')
        if cc.patient_id in df.columns:
            df = df.sort_values(by=[cc.patient_id], ascending=[True])
        if index is not None:
            df = df.set_index(index)
        self.__file_provider.save_dataframe_file(df=df, file_dir=file_dir, filename=file_name, file_format=file_format)

    def __valid_params(self, events_levels, distances):
        self.logger.debug('__valid_params')
        if len(events_levels) <= 1:
            self.logger.debug(f'Should be more than 1 level: len(events_levels)={len(events_levels)}')
            return False
        if len(distances) != len(events_levels) - 1:
            self.logger.debug(f'distances and events does not match: '
                              f'len(distance) = {len(distances)}, len(events_levels) = {len(events_levels)}')
            return False
        if None in events_levels:
            self.logger.debug(f'None value is not allowed in events. Use instead undefined event: '
                              f'events_levels = {events_levels}')
            return False
        return True

    def __init_date_patient_map(self, etf: ExperimentTimeFrame, patients: list) -> dict:
        self.logger.debug('init experiment time frame')
        if etf is None:
            return {None: set(patients)}
        min_d = datetime.datetime.strptime(str(etf.min_date), '%Y-%m-%d') if etf.min_date is not None else None
        max_d = datetime.datetime.strptime(str(etf.max_date), '%Y-%m-%d') if etf.max_date is not None else None
        return {(min_d, max_d): set(patients)}

    def __add_time_interval(
            self, event_dates: pd.Series, time_interval: Union[int, ExperimentTimeInterval, type(None)]
    ) -> tuple:
        if type(time_interval) is ExperimentTimeInterval:
            min_t = time_interval.get_min_t_days()
            max_t = time_interval.get_max_t_days()
            if max_t is None:
                end_dates = datetime.datetime.now()
            else:
                end_dates = event_dates + timedelta(days=max_t)
            if min_t is not None:
                event_dates = event_dates + timedelta(days=min_t)
        else:  # if time_interval is number just add it to the date
            end_dates = event_dates + timedelta(days=time_interval)
        return event_dates, end_dates

    def __get_date_patient_map(self, df: pd.DataFrame, time_interval: Union[int, ExperimentTimeInterval, type(None)],
                               etf: ExperimentTimeFrame) -> dict:
        self.logger.debug(f'__get_time_interval: t={time_interval} time frame = {etf}')

        df = df.groupby([cc.date])[cc.patient_id].apply(list).reset_index()

        col_start_date = 'start_date'
        df[col_start_date] = df[cc.date]
        col_end_date = 'end_date'
        df[col_end_date] = datetime.datetime.now()

        # append time interval
        if time_interval is not None:
            start_dates, end_dates = self.__add_time_interval(df[cc.date], time_interval)
            df[col_start_date] = start_dates
            df[col_end_date] = end_dates
        else:
            df[col_end_date] = datetime.datetime.now()

        # filter by experiment time frame
        if etf is not None and etf.max_date is not None:
            max_d = datetime.datetime.strptime(str(etf.max_date), '%Y-%m-%d')
            df[col_end_date] = [min(v, max_d) for v in df[col_end_date]]

        date_patient_map = {
            (row[col_start_date], row[col_end_date], row[cc.date]): set(row[cc.patient_id])
            for i, row in df.iterrows()}
        return date_patient_map

    def __append_total_time(self, df, level_number):
        sum_columns = [cc.get_column_at_level(cc.time_interval, i)
                       for i in range(level_number - 1)]
        df[self.__col_total_time] = df[sum_columns].sum(axis=1)
        return df

    def __merge_levels(self, index_df: pd.DataFrame, target_df: pd.DataFrame, index_level: ExperimentLevel,
                       target_level: ExperimentLevel):
        if index_df is None:
            return target_df
        if target_df is None or target_df.empty:
            return None
        self.logger.debug(f"merge level {index_level.level} with {index_df.shape} records "
                          f"and {target_level.level} with {target_df.shape}")
        index_df = self.__match_events(
            index_df=index_df,
            target_df=target_df,
            index_level=index_level,
            target_level=target_level
        )
        if index_df is None or index_df.empty:
            self.logger.warning(f'Nothing was matched between {index_level.level} and {target_level.level} levels')
            return None

        return index_df

    def __merge_chain_results(
            self, level_chain_file_dict: dict, experiment_config: ExperimentConfig
    ) -> pd.DataFrame:
        self.logger.debug('__merge_chain_results')
        # start merging from the last level to the first
        levels = sorted(list(level_chain_file_dict.keys()), reverse=True)
        file_name = self.__file_provider.get_result_file_path(
            f'{experiment_config.outcome_dir}/{level_chain_file_dict[levels[0]]}'
        )
        res_chain = pd.read_parquet(file_name, engine='pyarrow').reset_index()
        patients = res_chain[cc.patient_id].unique()
        # since patients will be the same number prepare data for merging
        total_patients = len(patients)
        curr, step = 0, 1000 if total_patients > 1000 else total_patients
        patient_bins = np.array_split(patients, total_patients // step)

        for level in levels[1:]:
            self.logger.debug(f'merge levels {level + 1} and {level}')
            file_name = self.__file_provider.get_result_file_path(
                f'{experiment_config.outcome_dir}/{level_chain_file_dict[level]}'
            )
            curr_chain = pd.read_parquet(file_name, engine='pyarrow').reset_index()
            # remove patients not in chain
            curr_chain = curr_chain[curr_chain[cc.patient_id].isin(patients)]
            # merge chains
            merge_cols = [cc.patient_id] + [c for c in curr_chain.columns if c.endswith(f'_{level}')]
            merge_result = []
            curr = 0
            for curr_patients in patient_bins:
                self.logger.debug(f'merge {curr}..{curr + step} from {total_patients}')
                curr = curr + step

                curr_chain_df = curr_chain[curr_chain[cc.patient_id].isin(curr_patients)]
                curr_res_df = res_chain[res_chain[cc.patient_id].isin(curr_patients)]

                curr_res_df = curr_res_df.merge(curr_chain_df, on=merge_cols)
                curr_res_df.drop_duplicates(inplace=True)
                merge_result.append(curr_res_df)

            res_chain = pd.concat(merge_result)

        # reverse order of chain columns
        columns = [cc.patient_id]
        for level in range(max(levels) + 1):
            columns.extend([c for c in res_chain.columns if c.endswith(f'_{level}')])
        res_chain = res_chain[columns]

        # todo: remove records according to mode (all records, first record)
        res_chain = self.__append_total_time(res_chain, len(experiment_config.levels))

        return res_chain
