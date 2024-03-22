import logging
import sys
from typing import Optional

import pandas as pd
import pymysql
from pymysql.connections import Connection
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder

import src.db.QueryBuilder as QB
from src.config.AppConfig import AppConfig
from src.db.SqlDataElement import SqlTable


class DatabaseManager:

    def __init__(self, app_config: AppConfig, db_name: Optional[str] = None, local_access=True):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.database_name = db_name
        self.__app_config = app_config
        self.__sql_engine = None
        self.__local_access = local_access
        self.tunnel: Optional[SSHTunnelForwarder] = None

    def __create_sql_engine(self):
        self.logger.debug('Create SQL engine')
        db_port = self.__app_config.localport if self.__local_access else self.tunnel.local_bind_port
        url = f'mysql+pymysql://{self.__app_config.mysql_username}:{self.__app_config.mysql_password}' \
              f'@{self.__app_config.localhost}:{db_port}/{self.database_name}'
        print(url)
        self.__sql_engine = create_engine(url, echo=False, pool_pre_ping=True)

    def open_ssh_tunnel(self):
        """
        Open an SSH tunnel and connect using a username and password.
        :return tunnel: Global SSH tunnel connection
        """
        if self.__local_access:
            self.logger.debug('Local access. No SSH needed')
        else:
            self.logger.debug("Open SSH tunnel")
            self.tunnel = SSHTunnelForwarder(
                (self.__app_config.ssh_host, 22),
                allow_agent=False,
                ssh_username=self.__app_config.ssh_username,
                ssh_password=self.__app_config.ssh_password,
                remote_bind_address=(self.__app_config.localhost, self.__app_config.localport)
            )
            self.tunnel.start()
            self.logger.debug(f"Tunnel is up: {self.tunnel.tunnel_is_up} | {self.tunnel.local_bind_address}")

        if self.__sql_engine is None:
            self.__create_sql_engine()

    def close_ssh_tunnel(self):
        """
        Closes the SSH tunnel connection.
        """
        if self.tunnel is not None:
            self.logger.debug('Close SSH tunnel')
            self.tunnel.stop()
        self.__sql_engine = None

    def connect_to_db(self, local_infile: bool = False) -> Connection:
        self.logger.debug(f'Connect to DB {self.database_name}')

        try:
            conn = pymysql.connect(
                host=self.__app_config.localhost,
                user=self.__app_config.mysql_username,
                passwd=self.__app_config.mysql_password,
                db=self.database_name,
                port=self.__app_config.localport if self.__local_access else self.tunnel.local_bind_port,
                local_infile=local_infile
            )
        except pymysql.Error as e:
            self.logger.debug(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

        # Get Cursor
        self.logger.debug(f'Database {self.database_name} connected')
        return conn

    def __exec_query(self, conn: Connection, query: str):
        self.logger.debug(f'Execute query : {query}')
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        return cur.fetchall()

    def create_table(self, table: SqlTable):
        self.logger.debug(f'Create table {table.name}')

        conn = self.connect_to_db()
        query = QB.create_table(table.name, table.column_names(), table.column_types(),
                                primary_keys=table.primary_keys(),
                                foreign_keys=table.foreign_keys())
        res = self.__exec_query(conn, query)
        if res is None or len(res) == 0:
            self.logger.debug(f'Table {table.name} was created')
        conn.close()

    def create_db(self, db_name: str):
        self.logger.debug(f'Create DB {db_name}')
        self.database_name = None
        conn = self.connect_to_db()
        query = QB.create_db(db_name)
        res = self.__exec_query(conn, query)
        if res is None or len(res) == 0:
            self.database_name = db_name
            self.logger.debug(f'DB {db_name} was created')
        conn.close()

    def create_indexes(self, table: SqlTable):
        self.logger.debug(f'Add indexes into table {table.name}')
        indexes_dict = {f'idx_{i}': i for i in table.indexes()
                        # MariaDB creates indexes for PK and FK automatically
                        if i not in table.primary_keys() and i not in table.foreign_key_names()}
        if len(indexes_dict) == 0:
            self.logger.debug('No indexes to add')
            return
        query = QB.create_index(table.name, indexes_dict)

        conn = self.connect_to_db()
        res = self.__exec_query(conn, query)
        if res is None or len(res) == 0:
            self.logger.debug(f'Indexes for table {table.name} were created')
        conn.close()

    def upload_file_to_sql(self, file_name: str, table_name: str):
        self.logger.debug(f'Upload data from file {file_name} into table {table_name}')
        query = QB.upload_table_from_file(self.database_name, table_name, file_name)
        conn = self.connect_to_db(local_infile=True)
        self.__exec_query(conn, query)
        conn.close()
        self.logger.debug(f'Data from {file_name} uploaded successfully')

    def __do_request_df(self, sql_query: str, parse_dates: list = None) -> Optional[pd.DataFrame]:
        if len(sql_query) > 400:
            self.logger.debug(f'__do_request_df: {sql_query[:200]} ... {sql_query[-200:]}')
        else:
            self.logger.debug(f'__do_request_df: {sql_query}')

        conn = self.connect_to_db()
        if conn is None:
            return None

        try:
            conn.connect()
            self.logger.debug('perform request')
            df = pd.read_sql(sql_query, conn, parse_dates=parse_dates)
        except BaseException as e:
            self.logger.debug(e)
            df = None
        finally:
            self.logger.debug('close db connection')
            conn.close()
        return df

    def __do_request(self, sql_query):
        self.logger.debug(f'__do_request: {sql_query[:500]}')

        conn = self.connect_to_db()
        if conn is None:
            return None

        try:
            conn.connect()
            self.logger.debug('perform request')
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                result = cursor.fetchall()
        except BaseException as e:
            self.logger.debug(e)
            result = None
        finally:
            conn.close()
        return result

    def request_subcodes(self, codes, table_name) -> Optional[list]:
        """
        get sub codes of codes
        :param codes: list of codes
        :param table_name: table name for search subcodes
        :return: list of subcodes
        """
        self.logger.debug(f'request_subcodes: code={codes}, table={table_name}')
        query = QB.get_subcodes(codes, table_name)
        db_result = self.__do_request(query)
        if db_result is None:
            return None
        result = [x[0] for x in db_result]
        return list(set(result))

    def request_icd9_icd10_map(self, codes, search_column):
        self.logger.debug(f'request_icd9_icd10_map: code N={len(codes)}')
        query = QB.get_icd9_icd10_map(codes, search_column)
        result = self.__do_request_df(query)
        return result

    def request_dead_patients(
            self, patients_info: Optional[list] = None, columns: Optional[list] = None
    ) -> Optional[pd.DataFrame]:
        self.logger.debug(f'request_dead_patient_ids: column={columns}')
        query = QB.request_dead_patients(patients_info, columns)
        result = self.__do_request_df(query)
        return result

    def request_patient_info(self, patients, columns, parse_dates: list = None):
        self.logger.debug(f'request_patient_info: columns={columns}, dates columns={parse_dates}')
        query = QB.get_patient_info(patients, columns)
        result = self.__do_request_df(query, parse_dates=parse_dates)
        return result

    def request_codes_description(self, codes):
        self.logger.debug(f'request_codes_description: codes={codes}'[:500])
        query = QB.get_codes_description(codes)
        result = self.__do_request_df(query)
        return result

    def request_code_info(self, codes: Optional[list], table: str, columns: Optional[list] = None,
                          include_subcodes: bool = False, patients_info: Optional[list] = None,
                          first_match: bool = False) -> Optional[pd.DataFrame]:
        """
        Get codes description from table
        :param codes: codes for search
        :param table: table where to search codes
        :param columns: columns to return
        :param include_subcodes: get subcodes of the given codes. For example T31 code with include subcodes flag gives
        codes list to search like T31.1, T31.2, T31.3 etc.
        :param patients_info: list of tuples with min_date, max_date, patients ids for this dates
        :param first_match: get only first (earliest) fitted record for each patient
        :return: dataframe with result
        """
        self.logger.debug(f'request_codes_info: codes={codes} table={table} '
                          f'column={columns} include_subcodes = {include_subcodes}')

        query = QB.get_code_info(codes, table, columns, include_subcodes, patients_info, first_match)
        result = self.__do_request_df(query)
        return result.dropna().drop_duplicates() if result is not None else None

    def request_patient_codes_to_date(
            self, date_patient_dict: dict, table_name: str, columns: list, date_column: str
    ) -> pd.DataFrame:
        self.logger.debug(f'request_patient_codes_to_date')
        query = QB.request_patient_codes_to_date(date_patient_dict, table_name, columns, date_column)
        result = self.__do_request_df(query)
        return result.dropna().drop_duplicates()
