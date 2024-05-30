import logging
from datetime import timedelta, datetime
from typing import Optional

import pandas as pd

from src.datamodel.Event import Event
from src.datamodel.Event import EventCategory, EventConstant
from src.datamodel.ExperimentConfig import ExperimentLevel, ExperimentTimeFrame
from src.repository.PatientRepository import PatientRepository
from src.repository.EventRepository import EventRepository
from src.util.ConcurrentUtil import ConcurrentUtil
from src.util.FileProvider import FileProvider

from src.datamodel.DataColumns import CommonColumns as cc


class GetEventData:
    def __init__(self, patient_repo: PatientRepository, event_repo: EventRepository):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__patient_repo = patient_repo
        self.__event_repo = event_repo
        self.__file_provider = FileProvider()

        self.logger.debug('Created')

    def execute(self, level: ExperimentLevel, columns: list, date_patient_map: Optional[dict],
                etf: Optional[ExperimentTimeFrame], include_icd9: bool = True, first_match: bool = False):
        events = [Event.from_experiment_event(e) for e in level.events]

        def adjust_columns(event, cols):
            if event.category == EventCategory.Medication:
                cols = cols + [cc.strength, cc.route, cc.brand]
            elif event.category == EventCategory.LabResult or event.category == EventCategory.VitalSign:
                if event.num_value is not None:
                    cols = cols + [cc.num_value]
                if event.text_value is not None:
                    cols = cols + [cc.text_value]
            return cols

        params = [(event,
                   adjust_columns(event, columns),
                   self.__adjust_event_period(event, date_patient_map, etf),
                   include_icd9,
                   first_match)
                  for event in events]
        res_data = ConcurrentUtil.do_async_job(self.__request_event_info, params_list=params)
        if len(res_data) == 0 or all(v is None for v in res_data):
            return None
        res_data = pd.concat(res_data)
        return res_data

    def __adjust_event_period(
            self, event: Event, date_patient_map: Optional[dict], etf: ExperimentTimeFrame
    ) -> Optional[dict]:
        self.logger.debug(
            f'Adjusting event period for event: {event}, '
            f'date_patient_map size {0 if date_patient_map is None else len(date_patient_map)}, '
            f'study time frame: {etf}')
        new_map = dict()
        if date_patient_map is None:
            return None
        for dates, patients in date_patient_map.items():
            if dates is None:
                new_map[None] = patients
                continue
            if event.period is None:
                new_map[(dates[0], dates[1])] = patients
                continue
            else:
                delta = timedelta(days=event.period.min_t) if event.period.min_t is not None else timedelta(days=0)
                start_date = dates[0] + delta
                delta = timedelta(days=event.period.max_t) if event.period.max_t is not None else timedelta(
                    days=365_000)
                end_date = dates[1] + delta

                # filter by experiment time frame
                if etf is not None:
                    if etf.max_date is not None:
                        max_d = datetime.strptime(str(etf.max_date), '%Y-%m-%d')
                        end_date = min(end_date, max_d)
                    if etf.min_date is not None:
                        min_d = datetime.strptime(str(etf.min_date), '%Y-%m-%d')
                        start_date = max(start_date, min_d)

                new_map[(start_date, end_date)] = patients

        return new_map

    def __request_event_info(self, event: Event, columns: list, date_patient_map: Optional[dict] = None,
                             include_icd9: bool = True, first_match: bool = False) -> Optional[pd.DataFrame]:
        df = None
        if event.category == EventCategory.Patient:
            patient_columns = [cc.patient_id, cc.date_of_death]
            if EventConstant.DEATH in event.codes:
                df = self.__patient_repo.get_dead_patients(columns=patient_columns, date_patient_map=date_patient_map)
                if df is None:
                    return None
                df.columns = [cc.patient_id, cc.date]
                df[cc.code] = EventConstant.DEATH
                col_left = [c for c in columns if c not in df.columns]
                if col_left:
                    df[col_left] = None
                df = df[columns]
        else:
            df = self.__event_repo.get_event_info(
                event=event, columns=columns, date_patient_map=date_patient_map,
                include_icd9=include_icd9, first_match=first_match
            )

        if (df is not None) and (not df.empty):
            df[cc.event_id] = event.id
            if event.has_attribute_events():
                df = self.__filter_attribute_evens(df, event)
        self.logger.debug(f'Returning event info for event: {event.id}, data size: {0 if df is None else df.shape}')
        return df

    def __get_attribute_events(self, df: pd.DataFrame, event: Event, exclude: bool = False) -> Optional[pd.DataFrame]:
        # if no event to filter return df as is
        if (exclude and not event.exclusion_events) or (not exclude and not event.having_events):
            return None
        gdf = df.groupby(cc.date)[cc.patient_id].apply(list).reset_index()

        period = event.exclusion_period if exclude else event.having_period
        events = event.exclusion_events if exclude else event.having_events
        if period is not None:
            gdf['start_date'] = gdf[cc.date] + timedelta(days=period.min_t)
            gdf['end_date'] = gdf[cc.date] + timedelta(days=period.max_t)
        else:
            gdf['start_date'] = None
            gdf['end_date'] = gdf[cc.date]  # todo make it None for the default case
        date_patient_map = {(row['start_date'], row["end_date"]): row[cc.patient_id]
                            for i, row in gdf.iterrows()}
        # get all patients with attribute events
        params = [(e, [cc.patient_id, cc.date],
                   self.__make_date_patient_map_for_event(e, df, date_patient_map),
                   True, False)
                  for e in events]
        res_dfs = ConcurrentUtil.do_async_job(self.__request_event_info, params)
        res_dfs = [d for d in res_dfs if (d is not None) and (not d.empty)]

        res_df = pd.concat(res_dfs) if res_dfs else None
        return res_df

    def __make_date_patient_map_for_event(self, event: Event, df: pd.DataFrame, common_map: dict) -> dict:
        if event.period is None:
            return common_map
        gdf = df.groupby(cc.date)[cc.patient_id].apply(list).reset_index()
        gdf['start_date'] = gdf[cc.date] + timedelta(days=event.period.min_t)
        gdf['end_date'] = gdf[cc.date] + timedelta(days=event.period.max_t)
        date_patient_map = {(row['start_date'], row["end_date"]): row[cc.patient_id]
                            for i, row in gdf.iterrows()}
        return date_patient_map

    def __filter_excluded_events(self, df: pd.DataFrame, excl_df: pd.DataFrame, event: Event) -> Optional[pd.DataFrame]:
        # remove all patients with exclusion events
        rows_count = df.shape[0]
        if excl_df is None or excl_df.empty:
            self.logger.debug('Nothing was excluded')
            return df

        df = df.merge(excl_df, on=cc.patient_id, how='left', suffixes=('', '_excl'))
        # records without any excluded events
        df_na = df[df[cc.date + '_excl'].isna()]
        # records with excluded events
        df = df[~df[cc.date + '_excl'].isna()]
        df['date_diff'] = (df[cc.date + '_excl'] - df[cc.date]).dt.days
        # check each date with each exclusion date
        for excl_event in event.exclusion_events:
            excl_df = df[df[cc.event_id + '_excl'] == excl_event.id]
            if event.exclusion_period is None and excl_event.period is None:
                excl_df = excl_df[excl_df['date_diff'] < 0]
            else:
                min_t = excl_event.period.min_t if excl_event.period is not None else event.exclusion_period.min_t
                max_t = excl_event.period.max_t if excl_event.period is not None else event.exclusion_period.max_t
                excl_df = excl_df[(excl_df['date_diff']).between(min_t, max_t)]
            excl_df = excl_df[[cc.patient_id, cc.date]].drop_duplicates()
            excl_df = excl_df.set_index([cc.patient_id, cc.date])
            # remove from all data excluded patients+dates
            df = df.set_index([cc.patient_id, cc.date])
            df = df[~df.index.isin(excl_df.index)]
            df = df.reset_index()
            if df.empty:
                break

        if (df is not None and not df.empty) or (df_na is not None and not df_na.empty):
            df = pd.concat([df, df_na])
        else:
            return None

        df = df.drop(
            columns=[cc.date + '_excl', cc.event_id + '_excl', 'date_diff']).drop_duplicates()
        self.logger.debug(f'Excluded {rows_count - df.shape[0]} records')
        return df

    def __filter_having_events(self, df: pd.DataFrame, having_df: pd.DataFrame, event: Event) -> Optional[pd.DataFrame]:
        rows_count = df.shape[0]
        # filter patients with having events
        if having_df is None or having_df.empty:
            self.logger.debug('No patients were found')
            return None

        df = df.merge(having_df, on=cc.patient_id, suffixes=('', '_having'))
        df['date_diff'] = (df[cc.date + '_having'] - df[cc.date]).dt.days
        for having_event in event.having_events:
            having_df = df[df[cc.event_id + '_having'] == having_event.id]
            if event.having_period is None and having_event.period is None:
                having_df = having_df[having_df['date_diff'] < 0]
            else:
                min_t = having_event.period.min_t if having_event.period is not None else event.having_period.min_t
                max_t = having_event.period.max_t if having_event.period is not None else event.having_period.max_t
                having_df = having_df[having_df['date_diff'].between(min_t, max_t)]
            having_df = having_df[[cc.patient_id, cc.date]].drop_duplicates()
            having_df = having_df.set_index([cc.patient_id, cc.date])
            # remove from all data excluded patients+dates
            df = df.set_index([cc.patient_id, cc.date])
            df = df[df.index.isin(having_df.index)]
            df = df.reset_index()
            if df.empty:
                break

        df = df.drop(
            columns=[cc.date + '_having', cc.event_id + '_having', 'date_diff']
        ).drop_duplicates()
        self.logger.debug(f'Excluded {rows_count - df.shape[0]} records')
        return df

    def __filter_attribute_evens(self, df: pd.DataFrame, event: Event) -> Optional[pd.DataFrame]:
        self.logger.debug(f'Filter attribute events for {event.id}')

        if event.exclusion_events:
            self.logger.debug(f'exclude patients with {[e.id for e in event.exclusion_events]} '
                              f'within {event.exclusion_period} days')
            excl_df = self.__get_attribute_events(df, event, exclude=True)
            df = self.__filter_excluded_events(df, excl_df, event)

        if event.having_events:
            self.logger.debug(f'Filter patients with {event.having_events} '
                              f'within {event.having_period} days')
            having_df = self.__get_attribute_events(df, event, exclude=False)
            df = self.__filter_having_events(df, having_df, event)
        return df
