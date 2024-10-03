from typing import Optional

import pandas as pd

from src.repository.BaseDbRepository import BaseDbRepository
from src.datamodel.Event import Event
from src.datamodel.DataColumns import CommonColumns as cc


class EventRepository(BaseDbRepository):

    def get_event_info(self, event: Event, columns: Optional[list] = None, date_patient_map: Optional[dict] = None,
                       first_incident: bool = False, include_icd9: bool = True) -> Optional[pd.DataFrame]:
        """
        Get vitals signs info by their ids
        :param event: event object with parameters to search records
        :param columns: list of columns to request
        :param date_patient_map: date to patient ids list map
        :param first_incident: get only first (earliest) fitted record for each patient
        :param include_icd9: IfTrue, then all ICD10 codes will be matched to ICD9. Records will be searched by both ICD10 and ICD9 codes.
        :return: dataframe from vitals signs table with columns or None
        """
        df = self._get_codes_info(event=event, columns=columns, include_icd9=include_icd9,
                                  date_patient_map=date_patient_map, first_incident=first_incident)
        if df is None or df.empty:
            return None
        for c in df.columns:
            if c in cc.date_columns:
                self.logger.debug(f'convert date column {c} to datetime')
                df[c] = pd.to_datetime(df[c], format='%Y-%m-%d')

        return df
