import logging

from src.db.SqlDataElement import ForeignKey
from src.util.Error import QueryBuilderError

logger = logging.getLogger('QueryBuilder')


def create_table(name: str, columns: list, types: list, primary_keys: list = None,
                 foreign_keys: list[ForeignKey] = None) -> str:
    logger.debug(f'Compose create table SQL request for table {name}')
    if len(columns) != len(types):
        raise QueryBuilderError(
            f'Columns number should be the same as type number. Found {len(columns)} columns and {len(types)} types')
    columns_str_list = [f'{c} {t}' for c, t in zip(columns, types)]
    columns_str = ', '.join(columns_str_list)
    query_params = [columns_str]

    if primary_keys is not None and len(primary_keys) > 0:
        pk_str = ', '.join(primary_keys)
        primary_key_str = f'PRIMARY KEY ({pk_str})'
        query_params.append(primary_key_str)

    if foreign_keys is not None and len(foreign_keys) > 0:
        fk_list = [str(fk) for fk in foreign_keys]
        foreign_key_str = ', '.join(fk_list)
        query_params.append(foreign_key_str)

    query_params = ', '.join(query_params)
    query = f'CREATE TABLE {name} ({query_params});'
    logger.debug(f'Composed query : {query}')
    return query


def create_db(db_name: str) -> str:
    logger.debug(f'Compose create "{db_name}" DB SQL request')
    query = f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET = 'utf8mb4' COLLATE = 'utf8mb4_bin';"
    logger.debug(f'Composed query : {query}')
    return query


def create_index(table_name: str, indexes: dict) -> str:
    """
    Add indexes in table
    :param table_name: table name
    :param indexes: dictionary {index name : column name}. Only one column for each index is supported
    :return:
    """
    logger.debug(f'Compose add index request for table {table_name}')
    index_str = ', '.join([f'ADD INDEX {i} ({c})' for i, c in indexes.items()])
    query = f'ALTER TABLE {table_name} {index_str}'
    logger.debug(f'Composed query: {query}')
    return query


def upload_table_from_file(db_name: str, table_name: str, file_name: str) -> str:
    return f"LOAD DATA LOCAL INFILE '{file_name}' INTO TABLE {db_name}.{table_name} " \
           f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"


def create_med_drug_description_table(db_name: str, table_name: str):
    return f"create table {db_name}.{table_name} as (" \
           "select md.code as md_code, md.code_system as md_code_system, mi.code as mi_code, st.code_description " \
           f"from {db_name}.medication_drug md " \
           f"inner join {db_name}.medication_ingredient mi on md.unique_id = mi.unique_id " \
           f"inner join {db_name}.standardized_terminology st on mi.code = st.code and mi.code_system = st.code_system" \
           ");"


def get_distinct_values(db, table, column, column_is_index=False):
    query = f'SELECT DISTINCT {column} FROM {db}.{table}'
    if column_is_index:
        query += f' ORDER BY {column}'
    query += ';'
    return query


def get_med_drug_data(db, table, columns, codes):
    columns_str = ','.join(columns)
    codes_str = in_expression('code', codes)
    return f'select {columns_str} from {db}.{table} where {codes_str};'


def get_med_ingredients_by_unique_id(db, table, columns, unique_ids):
    columns_str = ','.join(columns)
    unique_ids_str = in_expression("unique_id", unique_ids)
    return f'select {columns_str} from {db}.{table} where {unique_ids_str};'


def in_expression(column, values):
    l = [f"'{x}'" for x in values]
    s = ','.join(l)
    return f'{column} in ({s})'


def get_codes_description(db, table, columns, codes_and_system):
    columns_str = ','.join(columns)
    codes_and_system_str = [f"(code='{c}' and code_system='{s}')" for c, s in codes_and_system]
    codes_and_system_str = ' or '.join(codes_and_system_str)
    return f'select {columns_str} from {db}.{table} where {codes_and_system_str}';