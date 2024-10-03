import click

from src import Application
from src.api import Option as opt, validate, Command

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def runner(command: str, **kwargs):
    try:
        validate(command, **kwargs)
        if command == Command.create_new_db:
            Application.create_db(db_name=kwargs[opt.database],
                                  url=kwargs[opt.url],
                                  archive=kwargs[opt.archive],
                                  local_access=kwargs[opt.local_access],
                                  set_index=kwargs[opt.set_index],
                                  drop_csv=kwargs[opt.drop_csv],
                                  new_db=True)
        elif command == Command.append_data:
            Application.create_db(db_name=kwargs[opt.database],
                                  url=kwargs[opt.url],
                                  archive=kwargs[opt.archive],
                                  local_access=kwargs[opt.local_access],
                                  set_index=kwargs[opt.set_index],
                                  drop_csv=kwargs[opt.drop_csv],
                                  new_db=False)
        elif command == Command.run_study:
            Application.run_study(db_name=kwargs[opt.database], out_dir=kwargs[opt.out_dir],
                                  study_list=kwargs[opt.study], local_db=kwargs[opt.local_access])
        elif command == Command.validate_study:
            Application.validate_study(study_list=kwargs[opt.study])
    except ValueError as e:
        print(e)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version='1.1.0')
def run():
    pass


@run.command()
@click.option(f'--{opt.database}',
              help='Name of the database to be created')
@click.option(f'--{opt.url}',
              default=None,
              help='URL obtained from the TriNetX export option. '
                   'If it is null, --archive option is used as a data source. '
                   'If neither --url nor --archive are set, an empty database will be created.')
@click.option(f'--{opt.archive}',
              default=None,
              help='Archive full file name to store data from URL. '
                   'If URL is null, then this option is a path to the existing data archive. '
                   'If neither --url nor --archive are set, an empty database will be created.')
@click.option(f'--{opt.local_access}',
              default=True,
              help='If False, then SSH connection will be used. '
                   'Otherwise, the local host DB connection will be establish')
@click.option(f'--{opt.set_index}',
              default=True,
              help='If FALSE, than table indexes will not be created. '
                   'It will speed up data upload but increase data search time. Default value is TRUE')
@click.option(f'--{opt.drop_csv}', default=True,
              help='If TRUE, then .csv files with the common data model will be deleted after data uploaded to the '
                   'database.')
def create(**kwargs):
    runner(command=Command.create_new_db, **kwargs)


@run.command()
@click.option(f'--{opt.database}',
              help='Name of the database to append data to')
@click.option(f'--{opt.url}',
              default=None,
              help='URL obtained from the TriNetX export option. '
                   'If it is null, --archive option is used as a data source. '
                   'If neither --url nor --archive are set, an empty database will be created.')
@click.option(f'--{opt.archive}',
              default=None,
              help='Archive full file name to store data from URL. '
                   'If URL is null, then this option is a path to the existing data archive. '
                   'If neither --url nor --archive are set, an empty database will be created.')
@click.option(f'--{opt.local_access}',
              default=True,
              help='If False, then SSH connection will be used. '
                   'Otherwise, the local host DB connection will be establish')
@click.option(f'--{opt.set_index}',
              default=True,
              help='If FALSE, than table indexes will not be created. '
                   'It will speed up data upload but increase data search time. Default value is TRUE')
@click.option(f'--{opt.drop_csv}', default=True,
              help='If TRUE, then .csv files with the common data model will be deleted after data uploaded to the '
                   'database.')
def append(**kwargs):
    runner(command=Command.append_data, **kwargs)


@run.command()
@click.option(f'--{opt.database}',
              help='Name of the database to select data from')
@click.option(f'--{opt.study}', multiple=True,
              help='Study configuration file to be executed. Multiple studies can be set using --s option')
@click.option(f'--{opt.out_dir}',
              help='Path to the directory where the results will be stored.')
@click.option(f'--{opt.local_access}',
              default=True,
              help='If False, then SSH connection will be used. '
                   'Otherwise, the local host DB connection will be establish.')
def run_study(**kwargs):
    runner(command=Command.run_study, **kwargs)


@run.command()
@click.option(f'--{opt.study}', multiple=True,
              help='Study configuration file to be validated. Multiple studies can be set using --s option')
def validate_study(**kwargs):
    runner(command=Command.validate_study, **kwargs)


if __name__ == '__main__':
    run()
