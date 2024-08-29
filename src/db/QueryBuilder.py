import logging
from typing import Optional

from src.db import SqlUtil
from src.db.SqlDataElement import ForeignKey
from src.util.Error import QueryBuilderError
from src.datamodel.DataColumns import CommonColumns as cc, CommonTables as ct

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


def get_codes_description(codes_and_system: list):
    codes_and_system_str = []
    for c, s in codes_and_system:
        systems = [f"'{system}'" for system in s]
        systems = ",".join(systems)
        code_system_str = f"(code='{c}' and code_system in ({systems}))"
        codes_and_system_str.append(code_system_str)
    codes_and_system_str = ' or '.join(codes_and_system_str)
    return f'select * from {ct.code_description} where {codes_and_system_str};'


def get_subcodes(codes, table_name):
    like_expr = SqlUtil.like_expression(cc.code, codes)
    return f"SELECT {cc.code} FROM {table_name} " \
           f"WHERE {like_expr}"


def get_icd9_icd10_map(codes, code_search_column):
    list_expr = SqlUtil.in_expression(code_search_column, codes)
    return f'select {cc.icd9_code}, {cc.icd10_code} ' \
           f'from {ct.icd9_map_icd10} ' \
           f'where {list_expr}'


def request_dead_patients(patients_info: Optional[list] = None, columns: Optional[list] = None) -> str:
    columns = SqlUtil.selected_columns_expr(columns)
    # date-patient condition
    date_patient_condition = compose_date_patient_condition(patients_info, cc.date_of_death, ct.patient)

    # add date condition
    where = f'where not isnull({cc.date_of_death})'
    where += f' and ({date_patient_condition})' if date_patient_condition else ''
    return f'select {columns} ' \
           f'from {ct.patient} ' \
           f'{where}'


def get_patient_info(patients, columns=None):
    in_expression = SqlUtil.in_expression(cc.patient_id, patients)
    columns = SqlUtil.selected_columns_expr(columns)
    return f'select {columns} from {ct.patient} where {in_expression}'


def compose_date_patient_condition(patients_info: list, date_column: str, table: str) -> Optional[str]:
    if not patients_info:
        return None
    patient_id_col = f'{table}.{cc.patient_id}'
    date_col = f'{table}.{date_column}'
    res = []
    for min_date, max_date, patients in patients_info:
        condition = [SqlUtil.in_expression(patient_id_col, patients)] if patients else []
        if min_date is not None:
            condition.append(f"{date_col} >= '{min_date}'")
        if max_date is not None:
            condition.append(f"{date_col} <= '{max_date}'")
        if condition:
            condition = '(' + ' AND '.join(condition) + ')'
            res.append(condition)
    return ' OR '.join(res) if res else None


def get_code_info(codes: list, table: str, columns: list, include_subcodes: bool = False,
                  patients_info: Optional[list] = None, first_incident=False, num_value: str = None,
                  text_value: str = None
                  ) -> str:
    # select column expression
    request_columns = []
    for c in columns:
        if c == cc.type:
            c = f'{ct.encounter}.{cc.type}'
        else:
            c = f'{table}.{c}'
        request_columns.append(c)

    # group by expression
    if first_incident and cc.date in columns:
        date_column = f'{table}.{cc.date}'
        code_column = f'{table}.{cc.code}'
        patient_column = f'{table}.{cc.patient_id}'
        request_columns.remove(date_column)
        request_columns.append(f'min({date_column}) as {cc.date}')
        group_by_expr = f'group by {",".join([patient_column, code_column])}'
    else:
        group_by_expr = ''
    columns_expr = SqlUtil.selected_columns_expr(request_columns)

    # from expression
    if cc.type in columns:
        from_expr = f'{table} left join {ct.encounter} ' \
                    f'on {table}.{cc.encounter_id} = {ct.encounter}.{cc.encounter_id}'
    else:
        from_expr = f'{table}'

    # date-patient condition
    date_patient_condition = compose_date_patient_condition(patients_info, cc.date, table)

    # codes condition
    codes_condition = None
    if codes:
        request_code_column = f'{table}.{cc.code}'
        codes_condition = SqlUtil.like_expression(request_code_column, codes) if include_subcodes else \
            SqlUtil.in_expression(request_code_column, codes)

    # values conditions for labs and vitals
    values_condition = None
    if num_value is not None:
        values_condition = f'{cc.num_value}{num_value}'
    elif text_value is not None:
        values_condition = f'{cc.text_value}="{text_value}"'

    # request body
    cond_list = [codes_condition, date_patient_condition, values_condition]
    where = ' and '.join([f"({x})" for x in cond_list if x is not None])
    where_expr = f'where {where}' if len(where) > 0 else ''

    request = f"select {columns_expr} from {from_expr} {where_expr} {group_by_expr}"

    return request

