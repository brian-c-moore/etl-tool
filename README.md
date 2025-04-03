# etl-tool

## Overview

etl-tool is a command-line application for performing Extract, Transform, and Load (ETL) operations. It reads data from various sources, applies transformations based on a configuration file, and writes the results to various destinations. It's designed to be flexible and driven by a declarative YAML configuration.

## Features

*   Configuration-driven ETL processes using YAML.
*   Supports multiple data sources: CSV, JSON, XLSX, XML, YAML, PostgreSQL.
*   Supports multiple data destinations: CSV, JSON, XLSX, XML, YAML, PostgreSQL.
*   Data filtering capabilities using expressions applied before transformations.
*   Extensive record transformation and validation rules (type conversion, string manipulation, date/time handling, regex, hashing, required fields, numeric ranges, allowed values).
*   Data deduplication based on specified keys and strategies (first, last, min, max), applied after transformations.
*   Configurable error handling (halt or skip) with optional error logging to a file.
*   Optional FIPS compliance mode (restricts certain crypto algorithms like MD5 hashing).
*   Dry-run mode to preview actions without writing data to the destination.
*   Environment variable expansion (Unix-style `$VAR`, `${VAR}` and Windows-style `%VAR%`) in configuration file paths and connection strings.

## Usage

etl-tool -config <config_file.yaml> [options]

## Key Options

*   `-config string`: Path to the YAML configuration file (default: "config/etl-config.yaml").
*   `-input string`: Override the input file path specified in the config (ignored for source type 'postgres').
*   `-output string`: Override the output file path/table specified in the config (ignored for destination type 'postgres').
*   `-db string`: PostgreSQL connection string (overrides DB_CREDENTIALS environment variable).
*   `-loglevel string`: Logging level (none, error, warn, info, debug) (default: "info").
*   `-dry-run`: Perform all steps except writing to the destination.
*   `-fips`: Enable FIPS compliance mode.
*   `-help`: Show the help message.

## Environment Variables

*   `DB_CREDENTIALS`: PostgreSQL connection string (used if -db flag is not set).
*   Other variables (e.g., `$MY_PATH`, `${VAR_NAME}`, `%WIN_PATH%`) can be used within configuration file paths and connection strings for expansion.

## Building and Testing

*   Build: `go build -o etl-tool ./cmd/etl-tool/main.go`
*   Run Tests: `go test ./...`
*   Run Tests (Docker): `./docker-test.sh`

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
This tool was designed to address a gap I saw in existing automation tools. Golang seemed like the best choice to accomplish my goals, but my experience has been with other programming languages. I’ve leveraged AI for assistance with coding and as a way to teach myself a new language while building something useful. The overall design, architecture, and direction are entirely my own.
