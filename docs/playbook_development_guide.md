ETL-TOOL Playbook Development Guide

**1. Introduction**

This guide provides practical instructions, best practices, and examples for developing ETL "playbooks" (configuration files) for the `etl-tool`. It assumes you have a basic understanding of command-line tools, YAML syntax, and general ETL concepts.

The goal is to help you build robust, maintainable, and efficient ETL processes using `etl-tool`.

**2. Core Concepts**

*   **Playbook:** A YAML configuration file (`config.yaml` by default, specified via `-config`) that defines a single ETL process.
*   **Declarative:** You define *what* you want to achieve (source, destination, transformations), and `etl-tool` handles the execution.
*   **Workflow Sequence:** `etl-tool` processes data in a specific order:
    1.  **Extract:** Read all data from the source.
    2.  **Filter:** Apply the `filter` expression to remove source records.
    3.  **Transform:** Apply `mappings` sequentially to each remaining record.
    4.  **Flatten:** (If configured) Expand records based on list/slice fields.
    5.  **Deduplicate:** (If configured) Remove duplicate records based on keys and strategy.
    6.  **Load:** Write the final set of records to the destination.
*   **Record:** Data is processed as a sequence of records, represented internally as `map[string]interface{}` (like a dictionary or hash map).

**3. Getting Started**

*   **Installation:** Ensure you have Go installed (version 1.23 or later). Build the tool:
    ```bash
    go build -o etl-tool ./cmd/etl-tool/main.go
    ```
*   **Create a Config File:** Create a YAML file (e.g., `my_job.yaml`).
*   **Minimal Example (CSV to JSON):**

    *Input CSV (`input.csv`):*
    ```csv
    user_id,email_address,value
    1,alice@example.com,100.5
    2,bob@test.org,250
    ```

    *Playbook (`my_job.yaml`):*
    ```yaml
    source:
      type: csv
      file: input.csv
    destination:
      type: json
      file: output.json
    mappings:
      - { source: user_id, target: userId, transform: mustToInt } # Convert to integer
      - { source: email_address, target: email }
      - { source: value, target: amount, transform: mustToFloat } # Convert to float
    ```

*   **Run the Tool:**
    ```bash
    ./etl-tool -config my_job.yaml -loglevel debug
    ```
    Check `output.json` for the result and review the debug logs.

**4. Building Blocks: Configuration Sections**

Let's dive into each section of the YAML configuration file.

**4.1 Logging (`logging`)**

*   **Purpose:** Controls the verbosity of log messages printed during execution.
*   **Key Parameter:**
    *   `level`: `none`, `error`, `warn` (or `warning`), `info` (default), `debug`. Case-insensitive.
*   **Example:**
    ```yaml
    logging:
      level: debug
    ```
*   **Tips & Best Practices:**
    *   Use `debug` during development and troubleshooting to see detailed steps and transformations.
    *   Use `info` or `warn` for standard production runs to reduce noise.
    *   Use `error` for minimal production logging, capturing only critical failures.
    *   The `-loglevel` command-line flag overrides this setting.

**4.2 Source (`source`)**

*   **Purpose:** Defines where to read the initial data from.
*   **Required Parameters:**
    *   `type`: The format/source type (e.g., `csv`, `json`, `xlsx`, `xml`, `yaml`, `postgres`).
*   **Conditional Parameters:**
    *   `file`: Required for file types (`csv`, `json`, `xlsx`, `xml`, `yaml`). Path to the input file. Supports environment variable expansion. Can be overridden by `-input` flag.
    *   `query`: Required for `postgres` type. The SQL query to execute.
*   **Format-Specific Parameters:**
    *   `delimiter` (CSV): Single character delimiter (default `,`).
    *   `commentChar` (CSV): Single character for comment lines (default disabled).
    *   `sheetName` / `sheetIndex` (XLSX): Specify sheet by name (preferred) or 0-based index. Defaults to active/first sheet.
    *   `xmlRecordTag` (XML): Tag name of repeating record elements (default `record`).
*   **Examples:**
    ```yaml
    # CSV Source
    source:
      type: csv
      file: /data/input/daily_sales.csv
      delimiter: '|'
      commentChar: '#'

    # XLSX Source (read specific sheet)
    source:
      type: xlsx
      file: $HOME/reports/report.xlsx # Env var expansion
      sheetName: Raw Data

    # XML Source
    source:
      type: xml
      file: input_feed.xml
      xmlRecordTag: transaction # Records are <transaction>...</transaction>

    # PostgreSQL Source
    source:
      type: postgres
      # Query: Fetches active users, requires DB connection via -db or DB_CREDENTIALS
      query: "SELECT user_id, email, status, last_login FROM users WHERE status = 'active'"
    ```
*   **Tips & Best Practices:**
    *   Ensure file paths are correct and the tool has read permissions. Use absolute paths or paths relative to where `etl-tool` is run.
    *   Use environment variables (`$VAR`, `${VAR}`, `%VAR%`) for paths that might change between environments (e.g., `$INPUT_DATA_PATH/file.csv`).
    *   For PostgreSQL, ensure the connection string is correct and credentials are secure (use `DB_CREDENTIALS` env var or pass via `-db`, avoid hardcoding in the file). Note that passwords in the connection string will be masked in log output.
    *   Validate your SQL query independently before using it in the playbook.
    *   Be mindful of XLSX sheet names vs. indices, especially if the file structure might change. Naming is usually more robust.

**4.3 Destination (`destination`)**

*   **Purpose:** Defines where the final processed data should be written.
*   **Required Parameters:**
    *   `type`: The format/destination type (e.g., `csv`, `json`, `xlsx`, `xml`, `yaml`, `postgres`).
*   **Conditional Parameters:**
    *   `file`: Required for file types. Path to the output file. Supports environment variable expansion. Can be overridden by `-output` flag.
    *   `target_table`: Required for `postgres` type. Name of the database table (optionally schema-qualified, e.g., `public.results`). Can be overridden by `-output` flag for file types but NOT for Postgres table name.
*   **Format-Specific Parameters:**
    *   `delimiter` (CSV): Single character delimiter (default `,`).
    *   `sheetName` (XLSX): Sheet name to write to (default `Sheet1`). Will overwrite existing sheet.
    *   `xmlRecordTag` (XML): Tag name for record elements (default `record`).
    *   `xmlRootTag` (XML): Tag name for the root element (default `records`).
    *   `loader` (Postgres): Optional settings for loading data.
        *   `mode`: "" (empty, default) uses high-performance `COPY FROM`. `"sql"` uses custom commands.
        *   `command`: Required if `mode: sql`. The SQL statement (e.g., `INSERT`, `UPDATE`, function call) executed for *each record*. Use placeholders `$1`, `$2`, etc., corresponding to the *alphabetical order* of the target field names from your mappings.
        *   `preload`: Optional list of SQL commands run once *before* `sql` mode loading (e.g., `TRUNCATE table`).
        *   `postload`: Optional list of SQL commands run once *after* `sql` mode loading (e.g., `ANALYZE table`).
        *   `batch_size`: Optional (for `sql` mode). Number of records per transaction (default `0` means no batching, each record is a transaction). Batching improves performance for `sql` mode.
*   **Examples:**
    ```yaml
    # JSON Destination
    destination:
      type: json
      file: /processed_data/output.json

    # XLSX Destination
    destination:
      type: xlsx
      file: %REPORT_OUTPUT_DIR%\final_report.xlsx # Env var expansion
      sheetName: Processed Results

    # PostgreSQL Destination (using default COPY)
    destination:
      type: postgres
      target_table: public.analytics_data

    # PostgreSQL Destination (using custom SQL for Upsert)
    destination:
      type: postgres
      target_table: public.user_profiles
      loader:
        mode: sql
        # Assuming mappings result in fields: email, user_id, updated_at
        # Alphabetical order: email ($1), updated_at ($2), user_id ($3)
        command: |
          INSERT INTO public.user_profiles (user_id, email, updated_at)
          VALUES ($3, $1, $2)
          ON CONFLICT (user_id) DO UPDATE SET
            email = EXCLUDED.email,
            updated_at = EXCLUDED.updated_at;
        batch_size: 500 # Process 500 records per transaction
        preload:
          - "CREATE TEMP TABLE IF NOT EXISTS stage_user_profiles (LIKE public.user_profiles);" # Example preload
        postload:
          - "ANALYZE public.user_profiles;"
    ```
*   **Tips & Best Practices:**
    *   Ensure the tool has *write* permissions for the output directory/file.
    *   For file outputs, consider using environment variables for output paths.
    *   PostgreSQL `COPY` (`mode: ""` or omitted) is significantly faster than `sql` mode for bulk inserts. Use `COPY` whenever possible.
    *   Use `sql` mode primarily for `UPDATE` operations, complex inserts with functions, or upserts (`INSERT ... ON CONFLICT`).
    *   **Security:** Be extremely careful with the `command` in `sql` mode. Do *not* directly interpolate record values into the SQL string. Always use the `$1`, `$2` placeholders provided by `etl-tool`, which uses parameterized queries to prevent SQL injection.
    *   Understand the placeholder order for `sql` mode: it's based on the *alphabetical order* of the final target field names produced by your `mappings`.
    *   Use `batch_size` > 0 with `sql` mode for better performance than single-row transactions. Tune the size based on your data and database performance.
    *   Use `preload`/`postload` for setup/cleanup tasks related to `sql` mode loading (e.g., truncating, indexing, analyzing).

**4.4 Filter (`filter`)**

*   **Purpose:** Selectively skip source records *before* they are transformed.
*   **Syntax:** Uses `govaluate` expression syntax (see `govaluate` documentation for details). You can use field names from the *source* record as variables. Standard operators (`==`, `!=`, `>`, `<`, `>=`, `<=`, `&&`, `||`, `!`) and some functions are available.
*   **Example:**
    ```yaml
    # Keep records where status is 'active' AND amount is positive, OR priority is high
    filter: "(status == 'active' && amount > 0) || priority >= 10"

    # Example using a hypothetical 'contains' function (if available in govaluate)
    # filter: "contains(product_tags, 'sale')"
    ```
*   **Tips & Best Practices:**
    *   Filtering early can significantly improve performance by reducing the number of records that need transformation.
    *   Test complex filter expressions carefully using `-dry-run -loglevel debug` to see which records are kept/skipped.
    *   Ensure field names used in the filter match the *source* record structure.

**4.5 Mappings (`mappings`)**

*   **Purpose:** The core of the transformation logic. Defines how source fields are transformed, validated, and mapped to target fields.
*   **Structure:** An array (`[]`) of mapping rules. Each rule is a map (`{}`).
*   **Rule Parameters:**
    *   `source`: Required. Input field name. Can be a source field or the `target` of a *previous* rule in the sequence.
    *   `target`: Required. Output field name. Must be unique across all rules in the `mappings` section.
    *   `transform`: Optional. Name of the function to apply (see list below). Can include a shorthand parameter (e.g., `validateRegex:pattern`). If omitted, the `source` value is assigned directly to `target`.
    *   `params`: Optional. A map of parameters needed by the `transform` function (e.g., date formats, regex patterns, validation criteria).
*   **Execution:** Rules are executed sequentially for each record. The output (`target`) of one rule can be used as the `source` for a subsequent rule.
*   **Transformation Functions:** (See README or man page for full descriptions)
    *   **Type Conversion:** `toString`, `toInt`, `toFloat`, `toBool` (permissive), `mustToInt`, `mustToFloat`, `mustToBool` (strict).
    *   **String Manipulation:** `toUpperCase`, `toLowerCase`, `trim`, `replaceAll`, `substring`, `regexExtract`.
    *   **Date/Time:** `epochToDate`, `mustEpochToDate`, `dateConvert`, `mustDateConvert`, `multiDateConvert`, `calculateAge`.
    *   **Hashing/Utility:** `hash`, `coalesce`, `branch`.
    *   **Validations:** `validateRequired`, `validateRegex`, `validateNumericRange`, `validateAllowedValues`.
*   **Examples:**
    ```yaml
    mappings:
      # Simple rename
      - source: old_id
        target: new_id

      # Convert to uppercase string
      - source: status_code
        target: status_upper
        transform: toUpperCase

      # Convert epoch seconds to date string
      - source: created_ts
        target: creation_date
        transform: epochToDate

      # Convert date string format
      - source: event_date_str # e.g., "05/15/2024"
        target: iso_date
        transform: dateConvert
        params:
          inputFormat: "01/02/2006"
          outputFormat: "2006-01-02"

      # Validate email using shorthand
      - source: user_email
        target: email_validated
        transform: validateRegex:\S+@\S+\.\S+ # Returns error if invalid

      # Conditional value based on other fields
      - source: category # Input value used if no branch matches
        target: priority
        transform: branch
        params:
          branches:
            - condition: "category == 'urgent'"
              value: 1
            - condition: "category == 'high'"
              value: 2
            - condition: "startsWith(category, 'low')" # Requires govaluate built-in/custom func
              value: 3
            # Default branch (optional)
            - condition: "true" # Always true, acts as default
              value: 5

      # Hash multiple fields
      - source: user_id # Source for hash doesn't matter, only fields param
        target: record_hash
        transform: hash
        params:
          algorithm: sha256
          fields: ["user_id", "email", "timestamp"] # Hash based on these fields

      # Ensure field exists and is not empty/whitespace
      - source: required_field
        target: required_field # Often validate in place
        transform: validateRequired

      # Strict integer conversion (will halt/skip on error)
      - source: quantity_str
        target: quantity
        transform: mustToInt
    ```
*   **Tips & Best Practices:**
    *   Break down complex transformations into multiple steps using intermediate target fields.
    *   Use `must*` variants (e.g., `mustToInt`) when a failure to convert/validate should stop the process (in `halt` mode) or skip the record (in `skip` mode).
    *   Use permissive variants (`toInt`, `toFloat`, etc.) when a `nil` result is acceptable on failure.
    *   Use `toString` before applying string manipulation functions if the input might not be a string.
    *   Refer to `govaluate` documentation for available functions and syntax in `filter` and `branch` conditions.
    *   Ensure target names are unique.
    *   Be mindful of FIPS mode when using the `hash` transform (MD5 is disallowed).

**4.6 Flattening (`flattening`)**

*   **Purpose:** To transform records containing a list/slice into multiple output records, one for each item in the list. Occurs *after* transformations and *before* deduplication.
*   **Key Parameters:**
    *   `sourceField`: Required. The field (dot-notation supported) in the *transformed* record containing the list/slice.
    *   `targetField`: Required. The new field name in the *output* record that will hold each item from the source list.
    *   `includeParent`: Optional bool (default `true`). If true, copies all other fields from the parent record into each flattened record. If false, the flattened record contains *only* the `targetField`.
    *   `errorOnNonList`: Optional bool (default `false`). If true, generates an error (halt/skip) if `sourceField` isn't found, is nil, or isn't a slice. If false, silently skips flattening for that record.
    *   `conditionField`: Optional string. Parent field to check before flattening.
    *   `conditionValue`: Optional string. Required value of `conditionField` to trigger flattening.
*   **Example:**
    *Input Record (after mapping):*
    ```json
    { "orderId": 123, "customer": "CustA", "items": ["SKU1", "SKU2"] }
    ```
    *Flattening Config:*
    ```yaml
    flattening:
      sourceField: items
      targetField: itemSku
      includeParent: true
    ```
    *Output Records:*
    ```json
    [
      { "orderId": 123, "customer": "CustA", "itemSku": "SKU1" },
      { "orderId": 123, "customer": "CustA", "itemSku": "SKU2" }
    ]
    ```
    *Flattening Config (Conditional):*
    ```yaml
    flattening:
      sourceField: items
      targetField: itemSku
      includeParent: true
      conditionField: processFlag
      conditionValue: "yes"
    ```
    *(Only flattens if `processFlag: "yes"` exists in the parent record)*
*   **Tips & Best Practices:**
    *   Flattening is useful for normalizing data where one record logically represents multiple sub-items.
    *   Ensure `sourceField` exists and contains a list *after* the mapping steps are complete.
    *   Be aware that `includeParent: true` copies *all* other top-level fields. If the `sourceField` itself was nested (e.g., `details.items`), the `details` map (without `items`) will be copied.
    *   Combine flattening with `dedup` if the flattened items might introduce duplicates you want to remove. Choose `dedup.keys` based on the fields present *after* flattening.
    *   Use `errorOnNonList: true` if missing list data constitutes an error for your workflow.

**4.7 Deduplication (`dedup`)**

*   **Purpose:** Removes duplicate records based on the values in specified key fields. Occurs *after* transformations and flattening.
*   **Key Parameters:**
    *   `keys`: Required array of string target field names that form the composite key for uniqueness.
    *   `strategy`: Optional string defining which record to keep (default `first`).
        *   `first`: Keep the first record encountered with a given key combination.
        *   `last`: Keep the last record encountered.
        *   `min`: Keep the record with the minimum value in `strategyField`.
        *   `max`: Keep the record with the maximum value in `strategyField`.
    *   `strategyField`: Required string target field name when `strategy` is `min` or `max`. Used for comparison.
*   **Example:**
    ```yaml
    # Keep only the latest record per user_id
    dedup:
      keys: ["user_id"]
      strategy: max
      strategyField: updated_at # Assumes updated_at exists and is comparable (e.g., time.Time or comparable string/number)

    # Keep first record based on composite key
    dedup:
      keys: ["order_id", "product_sku"]
      strategy: first # Default, can be omitted
    ```
*   **Tips & Best Practices:**
    *   Ensure the fields listed in `keys` and `strategyField` exist in the records *after* the mapping and flattening steps.
    *   The `min` and `max` strategies rely on the `CompareValues` logic. Ensure the `strategyField` contains comparable data (numbers, strings, time.Time). Comparison errors are logged as warnings.
    *   Deduplication happens relatively late; consider if filtering or transformations could remove duplicates earlier more efficiently.

**4.8 Error Handling (`errorHandling`)**

*   **Purpose:** Controls how the tool behaves when errors occur during the processing of individual records (mapping transformations, validation failures, flattening errors).
*   **Key Parameters:**
    *   `mode`: Required. `halt` (default) stops the entire process immediately. `skip` logs/writes the error and continues with the next record.
    *   `logErrors`: Optional bool (defaults to `true` if `mode` is `skip`, ignored otherwise). If true, logs details of skipped records and errors.
    *   `errorFile`: Optional string. Path to a CSV file where skipped *original* records and the error message will be appended if `mode` is `skip`. Supports environment variable expansion.
*   **Example:**
    ```yaml
    # Stop immediately on any record processing error
    errorHandling:
      mode: halt

    # Skip bad records, log them, and write details to an error file
    errorHandling:
      mode: skip
      logErrors: true # Explicitly true (or omit for default skip behavior)
      errorFile: /etl_errors/job_failures.csv
    ```
*   **Tips & Best Practices:**
    *   Use `halt` mode during development to catch errors quickly.
    *   Use `skip` mode for production runs where processing should continue despite some bad data.
    *   Always use `errorFile` with `skip` mode in production to capture failed records for later analysis or reprocessing. Ensure the directory for `errorFile` exists and is writable.
    *   Set `logErrors: false` in `skip` mode if detailed console logging of skipped records is too verbose for production, relying solely on the `errorFile`.

**4.9 FIPS Mode (`fipsMode`)**

*   **Purpose:** Enforces FIPS 140-2 compliance restrictions, primarily affecting cryptographic operations.
*   **Key Parameter:**
    *   `fipsMode`: Optional bool (default `false`). If `true`, enables FIPS mode.
*   **Behavior:** Currently, the main impact is disallowing the `md5` algorithm in the `hash` transformation. Other crypto might be affected depending on the Go standard library's FIPS mode behavior.
*   **Example:**
    ```yaml
    fipsMode: true
    ```
*   **Tips & Best Practices:**
    *   Only enable FIPS mode if required by your operating environment or security policies.
    *   Be aware that enabling it restricts algorithm choices (specifically MD5 hashing).
    *   The `-fips` command-line flag overrides this setting.

**5. Advanced Topics & Tips**

*   **Environment Variables:** Use `$VAR`, `${VAR}`, or `%VAR%` extensively in `file`, `target_table`, and `db` connection strings to make playbooks portable and avoid hardcoding sensitive information or environment-specific paths.
*   **Dry Runs:** *Always* use `-dry-run` when developing or modifying playbooks. Combine with `-loglevel debug` to see exactly what records *would* be written and identify issues in filtering, transformation, flattening, or deduplication without affecting the destination.
*   **Debugging:**
    *   Start with `-loglevel debug`. Look for warnings and errors.
    *   Use `-dry-run`.
    *   Simplify your playbook: Comment out sections (filter, flattening, dedup, complex mappings) to isolate the problem area.
    *   Test complex `filter` or `branch` conditions separately if possible.
    *   Check file permissions and paths carefully.
    *   Validate source data manually if transformations yield unexpected results.
*   **Performance:**
    *   **Postgres:** `COPY` (default loader mode) is much faster than `sql` mode for inserts.
    *   **SQL Mode Batching:** If using `sql` mode, set `batch_size` to a reasonable value (e.g., 100-5000) to significantly improve performance over single-row commits.
    *   **Filtering:** Apply filters (`filter` section) early to reduce the number of records processed by transformations.
    *   **Memory:** Processing very large files reads the entire source into memory. Consider splitting large files or using database sources if memory becomes a constraint. `etl-tool` is not designed for streaming massive datasets that don't fit in memory.
    *   **Transform Complexity:** Very complex regex or numerous chained transformations can add overhead.

**6. Best Practices for Playbook Development**

*   **Keep it Simple:** Start with the basic flow and add complexity (filtering, flattening, complex transforms) incrementally.
*   **Use Comments:** Add YAML comments (`#`) to explain complex logic, field sources, or why certain choices were made.
*   **Version Control:** Store your playbooks in Git or another version control system.
*   **Separate Credentials:** Use environment variables (`DB_CREDENTIALS`, custom vars passed to `-db`) for database credentials, not the playbook file itself.
*   **Test Thoroughly:** Use `-dry-run` and `-loglevel debug` extensively during development. Test with representative sample data, including edge cases and potential "bad" data.
*   **Choose Error Handling Wisely:** `halt` for development, `skip` + `errorFile` for robust production runs.
*   **Monitor Error Files:** If using `skip` mode with an `errorFile`, establish a process for reviewing and addressing the errors captured.
*   **Understand the Order:** Remember the fixed processing order (Extract -> Filter -> Transform -> Flatten -> Deduplicate -> Load) when designing your logic.

**7. Common Problems and Pitfalls**

*   **YAML Errors:** Incorrect indentation or syntax. Use a YAML validator or IDE plugin.
*   **File Not Found:** Incorrect path, missing environment variable expansion, or permissions issues. Check paths relative to execution location. Use `debug` logging.
*   **Database Connection Errors:** Incorrect connection string, firewall issues, wrong credentials, database down. Check credentials (masking in logs helps), network connectivity.
*   **Invalid Transform Parameters:** Wrong data type or missing required `params` for a function. Check `debug` logs and the function documentation in the man page/README.
*   **Filter Expression Errors:** Incorrect syntax or using field names that don't exist in the *source* record. Test with `-dry-run`.
*   **Mapping Logic Errors:** Incorrect `source`/`target` naming, unexpected `nil` values, transform functions not behaving as expected (e.g., permissive vs. strict). Use `-dry-run -loglevel debug`.
*   **Flattening Issues:** `sourceField` doesn't exist or isn't a list *after* mapping; `targetField` name clashes; parent data incorrect due to nesting complexity. Debug mappings first, then flattening.
*   **Deduplication Issues:** `keys` or `strategyField` don't exist *after* mapping/flattening; comparison errors for `min`/`max` due to incompatible data types in `strategyField`. Debug the record structure just before deduplication using `-dry-run`.
*   **Permissions Issues:** Cannot read input file or write output/error file. Verify permissions for the user running `etl-tool`.
*   **Performance Bottlenecks:** Often due to `sql` mode without batching, complex transforms on huge datasets, or insufficient memory. Analyze logs, consider `COPY` mode, add batching, or process data in chunks if possible.

**8. Limitations**

`etl-tool` is powerful but has limitations:

*   **Single Node:** It runs as a single process on one machine. It's not a distributed ETL system like Spark or Flink.
*   **Batch Oriented:** Designed for processing datasets available at the start. Not suitable for real-time streaming ETL (like Kafka streams).
*   **Limited State:** Primarily processes records independently. No built-in features for complex joins, aggregations, or lookups across the *entire* dataset *during* the transformation phase (these usually happen in the source query or post-load). `branch` and `coalesce` offer limited cross-field logic within a single record.
*   **Memory Usage:** Reads the entire source dataset (after filtering) into memory before processing/writing. Very large files might exceed available RAM.
*   **No GUI:** Purely a command-line tool.
*   **Limited Complex Data Structures:** While it handles nested data in JSON/YAML/Postgres sources, transformations primarily operate on flat fields or simple lists (for flattening). Complex manipulations of deeply nested structures might require custom tooling.
*   **Binary Formats:** No built-in support for formats like Avro, Parquet, Protobuf etc.

**9. Conclusion**

`etl-tool` provides a flexible, configuration-driven approach to common ETL tasks. By understanding its workflow, configuration options, and transformation capabilities, you can build powerful data processing playbooks. Remember to leverage logging, dry runs, and incremental development for best results. Refer back to the man page and README for specific function and parameter details.
