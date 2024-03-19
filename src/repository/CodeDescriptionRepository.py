from src.repository.BaseDbRepository import BaseDbRepository

import pandas as pd


class CodeDescriptionRepository(BaseDbRepository):

    def get_code_description(self, codes: list):
        self._logger.debug(f'get_code_description: codes={codes}')
        if len(codes) == 0:
            return None

        # find all descriptions from terminology table
        df = self._db_manager.request_codes_description(codes)
        if df.empty:
            return None
        df = df.drop_duplicates()
        df = df.fillna('No Description')
        return df
