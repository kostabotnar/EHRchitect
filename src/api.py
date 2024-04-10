class Command:
    create_new_db = 'createdb'
    append_data = 'append'
    run_study = "run_study"


class Argument:
    db_name = 'db_name'
    study = 'study'


class Option:
    url = 'url'
    archive = 'archive'
    local_access = 'local_access'
    create_new_db = 'create_new_db'
    set_index = 'set_index'
    drop_csv = 'drop_csv'

    format_values = {'TNX'}  # OMOP, MIMICIV


def validate(command: str, **kwargs) -> bool:
    if command == Command.create_new_db:
        return validate_data_import(**kwargs)
    if command == Command.append_data:
        return validate_data_import(**kwargs)
    if command == Command.run_study:
        return validate_run_study(**kwargs)
    return False


def validate_data_import(**kwargs) -> bool:
    valid_url = kwargs[Option.url] is not None and len(kwargs[Option.url].strip()) > 0
    valid_archive = kwargs[Option.archive] is not None and len(kwargs[Option.archive].strip()) > 0
    if kwargs[Argument.db_name] is not None and kwargs[Argument.db_name].strip() == "":
        raise ValueError(
            f'Value Error: Invalid {Argument.db_name} value: "{kwargs[Argument.db_name]}"'
        )
    if not valid_url and not valid_archive:
        raise ValueError(
            f'Value Error: either {Option.url} or {Option.archive} should be set. '
            f'{Option.url} value: "{kwargs[Option.url]}. "'
            f'{Option.archive} value: "{kwargs[Option.archive]}."'
        )
    return True


def validate_run_study(**kwargs) -> bool:
    if kwargs[Argument.db_name].strip() == "":
        raise ValueError(
            f'Value Error: Invalid {Argument.db_name} value: "{kwargs[Argument.db_name]}"'
        )
    if len(kwargs[Argument.study]) == 0:
        raise ValueError(
            f'Value Error: AT least one study should be set'
        )
    if any([s[-4:] != 'json' for s in kwargs[Argument.study]]):
        raise ValueError(
            f'Value Error: Invalid {Argument.study} value. All study files should have JSON format'
        )
    return True
