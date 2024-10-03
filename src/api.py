class Command:
    create_new_db = 'createdb'
    append_data = 'append'
    run_study = "run_study"
    validate_study = "validate_study"


class Option:
    database = 'db'
    study = 's'
    url = 'url'
    archive = 'arch'
    local_access = 'la'
    set_index = 'set_index'
    drop_csv = 'drop_csv'
    out_dir = 'out'

    format_values = {'TNX'}  # OMOP, MIMICIV


def validate(command: str, **kwargs) -> bool:
    if command == Command.create_new_db:
        return validate_data_import(**kwargs)
    if command == Command.append_data:
        return validate_data_import(**kwargs)
    if command == Command.run_study:
        return validate_run_study(**kwargs)
    if command == Command.validate_study:
        return validate_validate_study(**kwargs)
    return False


def validate_data_import(**kwargs) -> bool:
    valid_url = kwargs[Option.url] is not None and len(kwargs[Option.url].strip()) > 0
    valid_archive = kwargs[Option.archive] is not None and len(kwargs[Option.archive].strip()) > 0
    if kwargs[Option.database] is not None and kwargs[Option.database].strip() == "":
        raise ValueError(
            f'Value Error: Invalid {Option.database} value: "{kwargs[Option.database]}"'
        )
    if not valid_url and not valid_archive:
        raise ValueError(
            f'Value Error: either {Option.url} or {Option.archive} should be set. '
            f'{Option.url} value: "{kwargs[Option.url]}. "'
            f'{Option.archive} value: "{kwargs[Option.archive]}."'
        )
    if valid_url and not valid_archive:
        raise ValueError(
            f'Value Error: {Option.archive} should be to store data from the {Option.url}. '
            f'{Option.url} value: "{kwargs[Option.url]}. "'
            f'{Option.archive} value: "{kwargs[Option.archive]}."'
        )
    return True


def validate_run_study(**kwargs) -> bool:
    if kwargs[Option.database].strip() == "":
        raise ValueError(
            f'Value Error: Invalid {Option.database} value: "{kwargs[Option.database]}"'
        )
    if len(kwargs[Option.study]) == 0:
        raise ValueError(
            f'Value Error: AT least one study should be set'
        )
    if len(kwargs[Option.out_dir]) == 0:
        raise ValueError(
            f'Value Error: Invalid {Option.out_dir} value: "{kwargs[Option.out_dir]}"'
        )
    if any([s[-4:] != 'json' for s in kwargs[Option.study]]):
        raise ValueError(
            f'Value Error: Invalid {Option.study} value. All study files should have JSON format'
        )
    return True


def validate_validate_study(**kwargs) -> bool:
    if len(kwargs[Option.study]) == 0:
        raise ValueError(
            f'Value Error: AT least one study should be set'
        )
    if any([s[-4:] != 'json' for s in kwargs[Option.study]]):
        raise ValueError(
            f'Value Error: Invalid {Option.study} value. All study files should have JSON format'
        )
    return True
