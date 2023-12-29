class Command:
    create_new_db = 'createdb'
    load_data = 'load'
    run_study = "run_study"


class Argument:
    db_name = 'db_name'
    study = 'study'


class Option:
    url = 'url'
    archive = 'archive'
    local_access = 'local_access'
    set_index = 'set_index'


def validate(**kwargs) -> bool:
    if Command.create_new_db in kwargs:
        return validate_create_db(**kwargs)
    if Command.load_data in kwargs:
        return validate_create_db(**kwargs)
    if Command.run_study in kwargs:
        return validate_run_study(**kwargs)
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
            f'Value Error: All study files should have JSON format'
        )
    return True
