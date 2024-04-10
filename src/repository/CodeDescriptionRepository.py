import logging

from src.db.DatabaseManager import DatabaseManager


class CodeDescriptionRepository:

    def __init__(self, db_manager: DatabaseManager):
        self.logger = logging.getLogger(type(self).__name__)
        self.db_manager = db_manager

    def get_code_description(self, codes: list):
        self.logger.debug(f'get_code_description: codes={codes}')
        if len(codes) == 0:
            return None

        # find all descriptions from terminology table
        self.db_manager.open_ssh_tunnel()
        df = self.db_manager.request_codes_description(codes)
        self.db_manager.close_ssh_tunnel()
        if df.empty:
            return None
        df = df.drop_duplicates()
        df = df.fillna('No Description')
        return df
