package config

// Define constants for configuration keys, types, modes etc.
const (
	SourceTypeJSON     = "json"
	SourceTypeCSV      = "csv"
	SourceTypeXLSX     = "xlsx"
	SourceTypeXML      = "xml"
	SourceTypeYAML     = "yaml"
	SourceTypePostgres = "postgres"

	DestinationTypeJSON     = "json"
	DestinationTypeCSV      = "csv"
	DestinationTypeXLSX     = "xlsx"
	DestinationTypeXML      = "xml"
	DestinationTypeYAML     = "yaml"
	DestinationTypePostgres = "postgres"

	LoaderModeSQL = "sql" // For custom SQL loading in Postgres

	ErrorHandlingModeHalt = "halt" // Stop processing on first record error
	ErrorHandlingModeSkip = "skip" // Skip records with errors and continue

	DedupStrategyFirst = "first" // Keep the first record encountered
	DedupStrategyLast  = "last"  // Keep the last record encountered
	DedupStrategyMin   = "min"   // Keep the record with the minimum value in StrategyField
	DedupStrategyMax   = "max"   // Keep the record with the maximum value in StrategyField

	DefaultLogLevel        = "info"
	DefaultLoaderBatchSize = 0 // 0 or less means no batching for custom SQL
	DefaultXMLRecordTag    = "record"
	DefaultXMLRootTag      = "records" // Default root tag for XML writer
	DefaultCSVDelimiter    = ","
	DefaultSheetName       = "Sheet1" // Default sheet name for XLSX writer
	DefaultDedupStrategy   = DedupStrategyFirst
)

// ETLConfig defines the overall structure for the ETL configuration YAML file.
type ETLConfig struct {
	// Logging configuration specifies the verbosity level.
	Logging LoggingConfig `yaml:"logging"`
	// Source defines the origin of the data (file type, path, query, options).
	Source SourceConfig `yaml:"source"`
	// Destination defines where the processed data will be written (type, path, table, options).
	Destination DestinationConfig `yaml:"destination"`
	// Filter is an optional expression (using govaluate syntax) evaluated against each input record.
	// Records for which the expression evaluates to false are skipped *before* transformations.
	// Example: "status == 'active' && amount > 0"
	Filter string `yaml:"filter,omitempty"`
	// Mappings define the transformation and validation rules applied to the data.
	Mappings []MappingRule `yaml:"mappings"`
	// --- ADDED ---
	// Flattening specifies optional configuration to expand records based on a list/slice field.
	// This occurs *after* mapping/transformation and *before* deduplication.
	Flattening *FlatteningConfig `yaml:"flattening,omitempty"`
	// --- END ADDED ---
	// Dedup specifies optional deduplication settings based on key fields, applied *after* transformations (and flattening).
	Dedup *DedupConfig `yaml:"dedup,omitempty"`
	// ErrorHandling defines how record-level processing errors (transformations, validations, flattening) are handled.
	ErrorHandling *ErrorHandlingConfig `yaml:"errorHandling,omitempty"`
	// FIPSMode indicates if FIPS compliance restrictions should be enforced (e.g., allowed crypto algorithms).
	// Can be overridden by the -fips command-line flag.
	FIPSMode bool `yaml:"fipsMode,omitempty"`
}

// LoggingConfig holds settings related to logging verbosity.
type LoggingConfig struct {
	// Level defines the logging detail (e.g., "none", "error", "warn", "info", "debug").
	// Defaults to "info".
	Level string `yaml:"level"`
}

// SourceConfig details the input source properties.
type SourceConfig struct {
	// Type indicates the format of the input source.
	// Supported types: "json", "csv", "xlsx", "xml", "yaml", "postgres". Required.
	Type string `yaml:"type"`
	// File specifies the path to the input file for file-based sources (json, csv, xlsx, xml, yaml).
	// Ignored for "postgres" type. Environment variables are expanded. Required for file types.
	File string `yaml:"file,omitempty"`
	// Query specifies the SQL query for "postgres" input source. Required for "postgres".
	// Ignored for file-based types.
	Query string `yaml:"query,omitempty"`

	// --- Format Specific Options ---
	// CSV Delimiter character (default: ","). Use '\t' for tab.
	Delimiter string `yaml:"delimiter,omitempty"`
	// CSV Comment character (e.g., "#"). Lines starting with this char are ignored. Default is disabled.
	CommentChar string `yaml:"commentChar,omitempty"`
	// XLSX Sheet name to read from. Takes precedence over SheetIndex if both are set.
	// Defaults to the first/active sheet if neither is specified.
	SheetName string `yaml:"sheetName,omitempty"`
	// XLSX Sheet index (0-based) to read from. Used if SheetName is not set.
	// Defaults to the first/active sheet (index 0) if neither is specified.
	SheetIndex *int `yaml:"sheetIndex,omitempty"` // Use pointer to distinguish 0 from unset
	// XML Tag name of the repeating elements that represent records (e.g., "item", "transaction").
	// Defaults to "record".
	XMLRecordTag string `yaml:"xmlRecordTag,omitempty"`
	// YAML specific options could be added here if needed (e.g., document index)
}

// DestinationConfig details the output destination properties.
type DestinationConfig struct {
	// Type indicates the format of the output destination.
	// Supported types: "json", "csv", "xlsx", "xml", "yaml", "postgres". Required.
	Type string `yaml:"type"`
	// TargetTable specifies the name of the table for "postgres" destination. Required for "postgres".
	// Ignored for file-based types.
	TargetTable string `yaml:"target_table,omitempty"`
	// File specifies the path to the output file for file-based destinations (json, csv, xlsx, xml, yaml).
	// Required for file-based types. Ignored for "postgres". Environment variables are expanded.
	File string `yaml:"file,omitempty"`
	// Loader provides specific configuration for PostgreSQL loading (e.g., custom SQL, batching).
	// Only applicable for "postgres" type.
	Loader *LoaderConfig `yaml:"loader,omitempty"`

	// --- Format Specific Options ---
	// CSV Delimiter character (default: ","). Use '\t' for tab.
	Delimiter string `yaml:"delimiter,omitempty"`
	// XLSX Sheet name to write to. Defaults to "Sheet1".
	SheetName string `yaml:"sheetName,omitempty"`
	// XML Tag name for the repeating elements representing records. Defaults to "record".
	XMLRecordTag string `yaml:"xmlRecordTag,omitempty"`
	// XML Tag name for the root element. Defaults to "records".
	XMLRootTag string `yaml:"xmlRootTag,omitempty"`
	// YAML specific options could be added here if needed (e.g., indentation)
}

// MappingRule defines a single transformation or validation step.
type MappingRule struct {
	// Source field name from the input record or a previously mapped target field. Required.
	Source string `yaml:"source"`
	// Target field name in the output record. Required.
	Target string `yaml:"target"`
	// Transform specifies the name of the transformation or validation function to apply
	// (e.g., "toUpperCase", "epochToDate", "validateRequired", "hash"). Optional.
	// Can include simple parameters like "regexExtract:pattern".
	Transform string `yaml:"transform,omitempty"`
	// Params provides additional configuration for complex transformations/validations
	// (e.g., date formats, regex patterns, hashing algorithm, validation rules). Optional.
	Params map[string]interface{} `yaml:"params,omitempty"`
}

// FlatteningConfig defines settings for expanding records based on a list/slice field.
type FlatteningConfig struct {
	// SourceField specifies the field containing the list/slice to flatten. Required.
	// Supports dot-notation for nested fields (e.g., "details.addresses").
	SourceField string `yaml:"sourceField"`
	// TargetField specifies the name of the field in the *output* record where each
	// individual item from the source list will be placed. Required.
	TargetField string `yaml:"targetField"`
	// IncludeParent, if true (default), copies all top-level fields from the original
	// (parent) record into each flattened record. If false, only the TargetField is included.
	IncludeParent *bool `yaml:"includeParent,omitempty"` // Default: true
	// ErrorOnNonList, if true, causes processing to halt or skip (based on global ErrorHandling)
	// if the SourceField does not exist, is nil, or is not a list/slice type.
	// If false (default), such records are silently skipped during flattening.
	ErrorOnNonList *bool `yaml:"errorOnNonList,omitempty"` // Default: false
	// ConditionField is an optional field (dot-notation supported) in the parent record
	// whose value must match ConditionValue for flattening to occur for that record.
	ConditionField string `yaml:"conditionField,omitempty"`
	// ConditionValue is the required value for the ConditionField to enable flattening.
	// Required if ConditionField is set. Comparison is string-based.
	ConditionValue string `yaml:"conditionValue,omitempty"`
}

// DedupConfig defines settings for removing duplicate records based on specified key fields.
// Deduplication happens *after* all transformations and flattening have been applied.
type DedupConfig struct {
	// Keys is a list of target field names used to construct a composite key for identifying duplicates. Required.
	Keys []string `yaml:"keys"`
	// Strategy defines how to handle duplicate keys. Default is "first".
	// "first": Keeps the first record encountered with the key.
	// "last": Keeps the last record encountered with the key.
	// "min": Keeps the record with the minimum value in the 'strategyField'.
	// "max": Keeps the record with the maximum value in the 'strategyField'.
	Strategy string `yaml:"strategy,omitempty"`
	// StrategyField is the target field name used for comparison when strategy is "min" or "max". Required for those strategies.
	StrategyField string `yaml:"strategyField,omitempty"`
}

// LoaderConfig holds settings specific to PostgreSQL loading mechanisms.
type LoaderConfig struct {
	// Mode specifies the loading strategy. Currently supports "sql" for custom commands.
	// If empty or omitted, the default high-performance PostgreSQL COPY mechanism is used.
	Mode string `yaml:"mode,omitempty"` // "" (default) or LoaderModeSQL
	// Command is the custom SQL command (e.g., INSERT, UPDATE, function call) executed for each record
	// when mode is "sql". Use placeholders like $1, $2 corresponding to the order of target fields
	// based on alphabetical sorting of the target field names. Required if mode is "sql".
	Command string `yaml:"command,omitempty"`
	// Preload lists SQL commands executed once *before* any records are loaded (e.g., TRUNCATE, temporary setup).
	// Only applicable if mode is "sql". Optional.
	Preload []string `yaml:"preload,omitempty"`
	// Postload lists SQL commands executed once *after* all records are loaded (e.g., ANALYZE, reporting function call).
	// Only applicable if mode is "sql". Optional.
	Postload []string `yaml:"postload,omitempty"`
	// BatchSize defines the number of records processed in a single transaction/batch when mode is "sql".
	// A value of 0 or less disables batching (each record is a separate command/transaction). Default is 0.
	BatchSize int `yaml:"batch_size,omitempty"`
}

// ErrorHandlingConfig defines how record-level processing errors are managed.
type ErrorHandlingConfig struct {
	// Mode specifies the behavior when a record fails processing (transformation or validation).
	// "halt" (default): Stops the entire ETL process on the first record error.
	// "skip": Logs the error, skips the problematic record, and continues processing subsequent records.
	Mode string `yaml:"mode"` // ErrorHandlingModeHalt or ErrorHandlingModeSkip
	// LogErrors indicates whether details of skipped records/errors should be logged.
	// Defaults to true if mode is "skip", otherwise ignored.
	LogErrors *bool `yaml:"logErrors,omitempty"` // Pointer to distinguish explicit false from unset
	// ErrorFile specifies an optional path to a file where skipped records and their errors will be written.
	// If provided and mode is "skip", failed records (original data + error message) are appended.
	// The format is typically CSV. Environment variables are expanded.
	ErrorFile string `yaml:"errorFile,omitempty"`
}