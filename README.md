# EHRchitect
### User Guide for EHRchitect Command-Line Interface

This guide provides detailed instructions on how to use the command-line interface (CLI) of the EHRchitect application. The CLI is built using the `click` library and offers a simple way to interact with the application for database creation, data appending, and running studies.

#### Getting Started

Before you begin, ensure you have the necessary Python environment and dependencies installed.

#### CLI Commands Overview

The EHRchitect CLI provides three main commands:

1. **createdb**: Creates a new database with the option to populate it from a specified data source.
2. **append**: Appends data to an existing database from a specified data source.
3. **run_study**: Runs a study using data from a specified database.

#### Command Details

##### 1. Creating a New Database (`createdb`)

To create a new database, use the `createdb` command followed by required and optional arguments:

```
python EHRchitect createdb DB_NAME [OPTIONS]
```

- `DB_NAME`: The name of the database to create.

Options:

- `--url URL`: The URL obtained from the TriNetX export option. If this is null, the `--archive` option is used as a data source.
- `--archive ARCHIVE`: The archive file name to store data from the URL. If the URL is null, this option is a path to the existing data archive.
- `--local_access [True|False]`: If False, then an SSH connection will be used. Otherwise, the local host DB connection will be established.
- `--set_index [True|False]`: If FALSE, then table indexes will not be created.
- `--drop_csv [True|False]`: If TRUE, then .csv files with the common data model will be deleted after data is uploaded to the database.

##### 2. Appending Data to an Existing Database (`append`)

To append data to an existing database, use the `append` command with similar options as the `createdb` command:

```
python EHRchitect append DB_NAME [OPTIONS]
```

The options are the same as those described for the `createdb` command.

##### 3. Running a Study (`run_study`)

To run a study using data from a specified database, use the `run_study` command:

```
python EHRchitect run_study DB_NAME STUDY_NAME [OPTIONS]
```

- `DB_NAME`: The name of the database to use.
- `STUDY_NAME`: The name(s) of the study or studies to run. Multiple study names can be provided separated by spaces.

Option:

- `--local_access [True|False]`: If False, then an SSH connection will be used. Otherwise, the local host DB connection will be established.

#### General Options

- `-h, --help`: Shows the help message and exits.
- `--version`: Shows the version of the CLI.

#### Running the CLI

To run any of the commands, navigate to the directory containing the `__main__.py` file in your terminal or command prompt, and then execute the command as shown in the examples above.

This guide should help you get started with the EHRchitect CLI. For more detailed information about each command and its options, you can always run the command followed by `--help` to see the help message.
