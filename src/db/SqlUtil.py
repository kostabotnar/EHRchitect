from copy import deepcopy
from typing import Generator


def in_expression(column, values):
    l = [f"'{x}'" for x in values]
    s = ','.join(l)
    return f'{column} in ({s})'


def not_in_expression(column, values):
    l = [f"'{x}'" for x in values]
    s = ','.join(l)
    return f'{column} not in ({s})'


def like_expression(column, values):
    chunks = [f"{column} LIKE '{val}%'" for val in values]
    like_exp = ' OR '.join(chunks)
    return like_exp


def selected_columns_expr(columns, prefix=None):
    if columns is None:
        return '*'
    elif prefix is None:
        return ','.join(columns)
    else:
        return f'{prefix}.' + f',{prefix}.'.join(columns)
