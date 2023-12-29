import click
from src.api import Argument as arg, Option as opt, validate

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def runner(**kwargs):
    try:
        validate(**kwargs)
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
              help='Archive file name to store data from URL. '
                   'If URL is null, then this option is a path to the existing data archive. '
                   'If it is null, it will be constructed from the database name and timestamp. '
                   'If neither --url nor --archive are set, an empty database will be created.')
@click.option(f'--{opt.local_access}',
              default=True,
              help='If False, then SSH connection will be used. '
                   'Otherwise, the local host DB connection will be establish')
@click.option(f'--{opt.set_index}',
              default=True,
              help='If FALSE, than table indexes will not be created. '
                   'It will speed up data upload but increase data search time. Default value is TRUE')
def createdb(**kwargs):
    runner(createdb=True, **kwargs)


@run.command()
@click.argument(arg.db_name)
@click.option(f'--{opt.url}',
              default=None,
              help='URL obtained from the TriNetX export option. '
                   'If it is null, --archive option is used as a data source. '
                   'If neither --url nor --archive are set, an empty database will be created.')
@click.option(f'--{opt.archive}',
              default=None,
              help='Archive file name to store data from URL. '
                   'If URL is null, then this option is a path to the existing data archive. '
                   'If it is null, it will be constructed from the database name and timestamp. '
                   'If neither --url nor --archive are set, an empty database will be created.')
@click.option(f'--{opt.local_access}',
              default=True,
              help='If False, then SSH connection will be used. '
                   'Otherwise, the local host DB connection will be establish')
@click.option(f'--{opt.set_index}',
              default=True,
              help='If FALSE, than table indexes will not be created. '
                   'It will speed up data upload but increase data search time. Default value is TRUE')
def load(**kwargs):
    runner(load=True, **kwargs)


@run.command()
@click.argument(arg.db_name)
@click.argument(arg.study, nargs=-1)
def run_study(**kwargs):
    runner(run_study=True, **kwargs)


if __name__ == '__main__':
    run()
