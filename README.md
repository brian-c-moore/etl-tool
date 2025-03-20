# ETL Tool

A generic Extract-Transform-Load (ETL) utility for extracting data from various sources, applying configurable transformations, deduplicating records, and loading the processed data into a destination. The tool is configured via a YAML file and supports multiple input and output formats.

## Features

- Multi-Source Input: Read data from JSON, CSV, XLSX, XML files or from PostgreSQL using SQL queries.
- Flexible Transformation: Apply a variety of built-in transformations (e.g., epoch-to-date, regex extraction, case conversion) using mapping rules.
- Deduplication: Optionally deduplicate records based on composite keys.
- Multiple Output Destinations: Load data to PostgreSQL tables (using COPY or custom SQL) or to file formats (CSV, XLSX, XML, JSON).
- Customizable Loading: Use custom SQL commands with preload, postload, and batching options when writing to PostgreSQL.
- Configurable Logging: Set logging levels (none, info, debug) to control verbosity.

## Requirements

- Go 1.18 or later
- PostgreSQL (if using database sources or destinations)
- Libraries: pgx, govaluate, excelize, yaml

## Installation

1. Clone the repository:

       git clone https://your-repository-url.git
       cd etl-tool

2. Build the project:

       go build -o etl-tool .

3. (Optional) Set environment variables (e.g., DB_CREDENTIALS)

## Configuration

The tool is configured via a YAML file. The configuration file is divided into these main sections:

### source

Defines the input data source.

- type: (Required) Input source type. Valid values:
  - json (JSON file)
  - csv (CSV file)
  - xlsx (Excel file)
  - xml (XML file)
  - postgres (PostgreSQL query)
- file: (Required for file-based sources) Path to the input file.
- query: (Required for postgres) SQL query to execute.

### destination

Defines the output data destination.

- type: (Required) Destination type. Valid values:
  - postgres (PostgreSQL table)
  - csv (CSV file)
  - xlsx (Excel file)
  - xml (XML file)
  - json (JSON file)
- target_table: (Required for postgres) Name of the target database table.
- file: (Required for file-based destinations) Path to the output file.
- loader: (Optional, for postgres) Additional options for loading data:
  - mode: Loader mode (e.g., sql to use custom SQL commands).
  - command: (Required if mode is sql) The custom SQL command to execute.
  - preload: List of SQL commands to run before loading.
  - postload: List of SQL commands to run after loading.
  - batch_size: Number of records per batch when using custom SQL.

### mappings

An ordered list of mapping rules that transform the input records.

Each mapping rule includes:
- source: (Required) Field name in the input record.
- target: (Required) Field name in the output record.
- transform: (Optional) Transformation function to apply (e.g., "regexExtract:pattern").
- params: (Optional) Additional parameters required by the transform.

### dedup

(Optional) Deduplication settings.

- keys: (Required) List of field names used to form a composite key for deduplication.

## Usage

Run the tool with the following command-line options:

       etl-tool -config config/etl-config.yaml -input /path/to/input.csv -db "postgres://user:password@host:port/dbname" -loglevel info

Command-line options:

- -config: Path to the YAML configuration file (default: config/etl-config.yaml).
- -input: (Optional) Override the input file path specified in the configuration.
- -db: PostgreSQL connection string. If not provided, the tool will use the DB_CREDENTIALS environment variable.
- -loglevel: Logging level (none, info, debug).

## Logging

The tool supports these logging levels:

- none: No logging output.
- info: Basic informational messages.
- debug: Detailed debug messages.

Set the log level using the -loglevel option.

## License
ETL Tool is released under the MIT License. See the LICENSE file for details.

## Acknowledgments
This tool was designed to address a gap I saw in existing automation tools. Golang seemed like the best choice to accomplish my goals, but my experience has been with other programming languages. I’ve leveraged ChatGPT for assistance with coding and as a way to teach myself a new language while building something useful. The overall design, architecture, and direction are entirely my own.
