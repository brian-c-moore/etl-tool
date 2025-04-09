# etl-tool

## Overview

etl-tool is a command-line application for performing Extract, Transform, and Load (ETL) operations. It reads data from various sources, applies transformations based on a configuration file, and writes the results to various destinations. The workflow follows the sequence: Extract -> Filter -> Transform -> Flatten -> Deduplicate -> Load.

## Features

*   Configuration-driven ETL processes using YAML.
*   Supports multiple data sources: CSV, JSON, XLSX, XML, YAML, PostgreSQL.
*   Supports multiple data destinations: CSV, JSON, XLSX, XML, YAML, PostgreSQL.
*   Data Flattening: Expands records containing lists into multiple records based on configuration.
*   Data filtering capabilities using expressions (`govaluate` syntax).
*   Record transformation and validation rules (type conversions, string manipulation, date handling, hashing, conditional logic, etc.).
*   Data deduplication based on specified keys and strategies (first, last, min, max).
*   Configurable error handling (halt or skip) with optional error file output (CSV).
*   Optional FIPS compliance mode (restricts MD5 hashing).
*   Dry-run mode to preview actions without writing data.
*   Environment variable expansion in configuration paths and connection strings (supports `$VAR`, `${VAR}`, `%VAR%`).
*   Masking of sensitive credentials in log output.

## Usage

etl-tool -config <config_file.yaml> [options]

## Key Options

*   `-config string`: Path to the YAML configuration file (default: "config/etl-config.yaml"). Environment variables expanded.
*   `-input string`: Override the input file path specified in the config (ignored for source type 'postgres'). Environment variables expanded.
*   `-output string`: Override the output file path/table specified in the config (ignored for destination type 'postgres'). Environment variables expanded.
*   `-db string`: PostgreSQL connection string (overrides DB_CREDENTIALS environment variable). Environment variables expanded. Credentials masked in logs.
*   `-loglevel string`: Logging level (none, error, warn/warning, info, debug) (default: "info").
*   `-dry-run`: Perform all steps except writing to the destination.
*   `-fips`: Enable FIPS compliance mode.
*   `-help`: Show the help message.

## Environment Variables

*   `DB_CREDENTIALS`: PostgreSQL connection string (used if -db flag is not set). Environment variables expanded. Credentials masked in logs.
*   Universal Variable Expansion: File paths and connection strings support Unix-style (`$VAR`, `${VAR}`) and Windows-style (`%VAR%`) variable expansion. Unset variables become empty strings.

## Building and Testing

*   Build: `go build -o etl-tool ./cmd/etl-tool/main.go`
*   Run Tests: `go test ./...`
*   Run Tests (Docker): Build image (`docker build -t etl-test .`) and run tests within the container (`docker run --rm etl-test`). A helper script like `./docker-test.sh` might automate this.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
This tool was designed to address a gap I saw in existing automation tools. Golang seemed like the best choice to accomplish my goals, but my experience has been with other programming languages. I’ve leveraged AI for assistance with coding and as a way to teach myself a new language while building something useful. The overall design, architecture, and direction are entirely my own.
