import click

from src import Application
from src.api import Argument as arg, Option as opt, validate, Command

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def runner(command: str, **kwargs):
    try:
        validate(command, **kwargs)
        if command == Command.create_new_db:
            Application.create_db(db_name=kwargs[arg.db_name],
                                  url=kwargs[opt.url],
                                  archive=kwargs[opt.archive],
                                  local_access=kwargs[opt.local_access],
                                  set_index=kwargs[opt.set_index],
                                  drop_csv=kwargs[opt.drop_csv],
                                  new_db=True)
        elif command == Command.append_data:
            Application.create_db(db_name=kwargs[arg.db_name],
                                  url=kwargs[opt.url],
                                  archive=kwargs[opt.archive],
                                  local_access=kwargs[opt.local_access],
                                  set_index=kwargs[opt.set_index],
                                  drop_csv=kwargs[opt.drop_csv],
                                  new_db=False)
        elif command == Command.run_study:
            Application.run_study(db_name=kwargs[arg.db_name], study_list=kwargs[arg.study],
                                  local_db=kwargs[opt.local_access])
    except ValueError as e:
        print(e)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version='1.0.0')
def run():
    pass


@run.command()
@click.argument(arg.db_name)
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
def createdb(**kwargs):
    runner(command=Command.create_new_db, **kwargs)


@run.command()
@click.argument(arg.db_name)
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
@click.argument(arg.db_name)
@click.argument(arg.study, nargs=-1)
@click.option(f'--{opt.local_access}',
              default=True,
              help='If False, then SSH connection will be used. '
                   'Otherwise, the local host DB connection will be establish')
def run_study(**kwargs):
    runner(command=Command.run_study, **kwargs)


if __name__ == '__main__':
    run()
