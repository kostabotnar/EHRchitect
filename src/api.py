class Command:
    create_new_db = 'createdb'


class Argument:
    db_name = 'db_name'


class Option:
    url = 'url'
    archive = 'archive'
    local_access = 'local_access'
    set_index = 'set_index'


def validate(**kwargs) -> bool:
    if Command.create_new_db in kwargs:
        validate_create_db(**kwargs)
    return False


def validate_create_db(**kwargs) -> bool:
    if kwargs[Argument.db_name] is not None and kwargs[Argument.db_name].strip() == "":
        raise ValueError(
            f'Value Error: Invalid {Argument.db_name} value: "{kwargs[Argument.db_name]}"'
        )
    if kwargs[Option.url] is not None and kwargs[Option.url].strip() == "":
        raise ValueError(
            f'Value Error: Invalid --{Option.url} value: "{kwargs[Option.url]}"'
        )
    if kwargs[Option.archive] is not None and kwargs[Option.archive].strip() == "":
        raise ValueError(
            f'Value Error: Invalid --{Option.archive} value: "{kwargs[Option.archive]}"'
        )
    return True
