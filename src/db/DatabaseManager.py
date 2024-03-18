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

    def __init__(self, app_config: AppConfig, db_name: Optional[str] = None, local_access=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.database_name = db_name
        self.__app_config = app_config
        self.__sql_engine = None
        self.__local_access = local_access
        self.tunnel: Optional[SSHTunnelForwarder] = None

    def __create_sql_engine(self):
        self.logger.debug('Create SQL engine')
        db_port = self.__app_config.localport if self.__local_access else self.tunnel.local_bind_port
        url = f'mysql+pymysql://{self.__app_config.database_username}:{self.__app_config.database_password}' \
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
        self.logger.debug('Connect to DB')

        try:
            conn = pymysql.connect(
                host=self.__app_config.localhost,
                user=self.__app_config.database_username,
                passwd=self.__app_config.database_password,
                db=self.database_name,
                port=self.__app_config.localport if self.__local_access else self.tunnel.local_bind_port,
                local_infile=local_infile
            )
        except pymysql.Error as e:
            self.logger.debug(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

        # Get Cursor
        self.logger.debug('Success')
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

    def upload_df_to_sql(self, df: pd.DataFrame, table_name: str) -> bool:
        self.logger.debug(f'Upload df to sql: table={table_name}')
        success = True
        with self.__sql_engine.begin() as con:
            try:
                df.to_sql(f'{table_name}', con=con, if_exists='append', index=False, chunksize=1_000_000)
            except Exception as e:
                self.logger.error(e)
                success = False
        self.logger.debug('uploaded')
        return success

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

    def get_med_drug_codes(self):
        self.logger.debug(f'get_med_drug_codes')

        conn = self.connect_to_db()
        query = QB.get_distinct_values(self.database_name, 'medication_drug', 'code', True)
        res = self.__exec_query(conn, query)
        if res is None or len(res) == 0:
            self.logger.debug(f'there is no drug codes found')
        conn.close()
        res = [c[0] for c in res]
        return res

    def read_med_drug_data(self, table, columns, codes) -> Optional[pd.DataFrame]:
        self.logger.debug(f'read_med_drug_data for {len(codes)} codes')

        conn = self.connect_to_db()
        query = QB.get_med_drug_data(self.database_name, table, columns, codes)
        df = pd.read_sql(query, conn)
        conn.close()

        if df is None or df.empty:
            self.logger.debug(f'No data found')
            return df
        return df.drop_duplicates()

    def get_med_ingredients_by_unique_id(self, table, columns, unique_ids):
        self.logger.debug(f'get_med_ingredients_by_unique_id')

        conn = self.connect_to_db()
        query = QB.get_med_ingredients_by_unique_id(self.database_name, table, columns, unique_ids)
        df = pd.read_sql(query, conn)
        conn.close()

        if df is None or df.empty:
            self.logger.debug(f'No data found')
            return df
        return df.drop_duplicates()

    def get_codes_description(self, columns, codes_and_system):
        self.logger.debug(f'get_med_ingredients_by_unique_id')

        conn = self.connect_to_db()
        query = QB.get_codes_description(self.database_name, 'standardized_terminology', columns, codes_and_system)
        df = pd.read_sql(query, conn)
        conn.close()

        if df is None or df.empty:
            self.logger.debug(f'No data found')
            return df
        return df.drop_duplicates()
