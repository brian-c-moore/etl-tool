package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"etl-tool/internal/logging"
)

// --- Test Helper Functions ---

// createTempConfigFile creates a temporary YAML file with the given content for testing.
// It returns the path to the temporary file and a cleanup function.
func createTempConfigFile(t *testing.T, content string) (string, func()) {
	t.Helper()
	tempDir := t.TempDir()
	tempFile, err := os.CreateTemp(tempDir, "test-config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp config file: %v", err)
	}
	if _, err := tempFile.WriteString(content); err != nil {
		tempFile.Close()
		t.Fatalf("Failed to write to temp config file: %v", err)
	}
	filePath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Failed to close temp config file: %v", err)
	}
	cleanup := func() {}
	return filePath, cleanup
}

// assertValidationError checks if the error contains all expected substrings.
func assertValidationError(t *testing.T, err error, expectedSubstrings ...string) {
	t.Helper()
	if err == nil {
		t.Errorf("Expected a validation error, but got nil")
		return
	}
	errStr := err.Error()
	for _, sub := range expectedSubstrings {
		if !strings.Contains(errStr, sub) {
			t.Errorf("Validation error missing expected substring %q.\nError was: %q", sub, errStr)
		}
	}
}

// --- LoadConfig Tests ---

// TestLoadConfig_Success tests loading a valid configuration file.
func TestLoadConfig_Success(t *testing.T) {
	validYAML := `
logging:
  level: debug
source:
  type: csv
  file: /input/data.csv
  delimiter: '|'
destination:
  type: postgres
  target_table: output_table
filter: "amount > 100"
mappings:
  - source: col1
    target: out1
    transform: toUpperCase
  - source: col2
    target: out2
    transform: toInt
dedup:
  keys: ["out1"]
  strategy: last
errorHandling:
  mode: skip
  errorFile: /errors/skipped.csv
fipsMode: true
`
	filePath, cleanup := createTempConfigFile(t, validYAML)
	defer cleanup()

	cfg, err := LoadConfig(filePath)

	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if cfg == nil {
		t.Fatal("LoadConfig() returned nil config, want non-nil")
	}

	if cfg.Logging.Level != "debug" {
		t.Errorf("cfg.Logging.Level = %q, want %q", cfg.Logging.Level, "debug")
	}
	if cfg.Source.Type != "csv" {
		t.Errorf("cfg.Source.Type = %q, want %q", cfg.Source.Type, "csv")
	}
	if cfg.Source.Delimiter != "|" {
		t.Errorf("cfg.Source.Delimiter = %q, want %q", cfg.Source.Delimiter, "|")
	}
	if cfg.Destination.Type != "postgres" {
		t.Errorf("cfg.Destination.Type = %q, want %q", cfg.Destination.Type, "postgres")
	}
	if cfg.Filter != "amount > 100" {
		t.Errorf("cfg.Filter = %q, want %q", cfg.Filter, "amount > 100")
	}
	if len(cfg.Mappings) != 2 {
		t.Fatalf("len(cfg.Mappings) = %d, want 2", len(cfg.Mappings))
	}
	if cfg.Mappings[0].Target != "out1" {
		t.Errorf("cfg.Mappings[0].Target = %q, want %q", cfg.Mappings[0].Target, "out1")
	}
	if cfg.Dedup == nil || cfg.Dedup.Strategy != "last" {
		t.Errorf("cfg.Dedup.Strategy = %v, want %q", cfg.Dedup, "last")
	}
	if cfg.ErrorHandling == nil || cfg.ErrorHandling.Mode != "skip" {
		t.Errorf("cfg.ErrorHandling.Mode = %v, want %q", cfg.ErrorHandling, "skip")
	}
	if cfg.ErrorHandling.ErrorFile != "/errors/skipped.csv" {
		t.Errorf("cfg.ErrorHandling.ErrorFile = %q, want %q", cfg.ErrorHandling.ErrorFile, "/errors/skipped.csv")
	}
	if !cfg.FIPSMode {
		t.Error("cfg.FIPSMode = false, want true")
	}
}

// TestLoadConfig_Defaults tests that default values are applied correctly.
func TestLoadConfig_Defaults(t *testing.T) {
	minimalYAML := `
source:
  type: json
  file: /in.json
destination:
  type: json
  file: /out.json
mappings:
  - source: id
    target: id
`
	filePath, cleanup := createTempConfigFile(t, minimalYAML)
	defer cleanup()

	cfg, err := LoadConfig(filePath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if cfg == nil {
		t.Fatal("LoadConfig() returned nil config, want non-nil")
	}

	if cfg.Logging.Level != DefaultLogLevel {
		t.Errorf("cfg.Logging.Level = %q, want default %q", cfg.Logging.Level, DefaultLogLevel)
	}
	if cfg.ErrorHandling == nil || cfg.ErrorHandling.Mode != ErrorHandlingModeHalt {
		t.Errorf("cfg.ErrorHandling.Mode = %v, want default %q", cfg.ErrorHandling, ErrorHandlingModeHalt)
	}
	if cfg.ErrorHandling != nil && cfg.ErrorHandling.LogErrors != nil && *cfg.ErrorHandling.LogErrors {
		t.Errorf("cfg.ErrorHandling.LogErrors = true, want nil or false for default halt mode")
	}
	if cfg.FIPSMode {
		t.Error("cfg.FIPSMode = true, want default false")
	}
	if cfg.Source.Type == SourceTypeCSV && cfg.Source.Delimiter != DefaultCSVDelimiter {
		t.Errorf("CSV Source Delimiter not defaulted correctly")
	}
	if cfg.Source.Type == SourceTypeXML && cfg.Source.XMLRecordTag != DefaultXMLRecordTag {
		t.Errorf("XML Source XMLRecordTag not defaulted correctly")
	}
	destDefaultsYAML := `
source: { type: csv, file: in.csv }
destination:
  type: xml
  file: out.xml
mappings: [{ source: a, target: b }]
`
	filePathDest, cleanupDest := createTempConfigFile(t, destDefaultsYAML)
	defer cleanupDest()
	cfgDest, err := LoadConfig(filePathDest)
	if err != nil {
		t.Fatalf("LoadConfig() for destination defaults failed: %v", err)
	}

	if cfgDest.Destination.Type == DestinationTypeCSV && cfgDest.Destination.Delimiter != DefaultCSVDelimiter {
		t.Errorf("CSV Destination Delimiter not defaulted correctly")
	}
	if cfgDest.Destination.Type == DestinationTypeXLSX && cfgDest.Destination.SheetName != DefaultSheetName {
		t.Errorf("XLSX Destination SheetName not defaulted correctly")
	}
	if cfgDest.Destination.Type == DestinationTypeXML {
		if cfgDest.Destination.XMLRecordTag != DefaultXMLRecordTag {
			t.Errorf("XML Destination XMLRecordTag not defaulted correctly")
		}
		if cfgDest.Destination.XMLRootTag != DefaultXMLRootTag {
			t.Errorf("XML Destination XMLRootTag not defaulted correctly")
		}
	}
	dedupDefaultYAML := `
source: { type: json, file: in.json }
destination: { type: json, file: out.json }
mappings: [{ source: id, target: id }]
dedup:
  keys: [id]
`
	filePathDedup, cleanupDedup := createTempConfigFile(t, dedupDefaultYAML)
	defer cleanupDedup()
	cfgDedup, err := LoadConfig(filePathDedup)
	if err != nil {
		t.Fatalf("LoadConfig() for dedup defaults failed: %v", err)
	}
	if cfgDedup.Dedup == nil || cfgDedup.Dedup.Strategy != DefaultDedupStrategy {
		t.Errorf("cfgDedup.Dedup.Strategy = %v, want default %q", cfgDedup.Dedup, DefaultDedupStrategy)
	}
	errorSkipDefaultYAML := `
source: { type: json, file: in.json }
destination: { type: json, file: out.json }
mappings: [{ source: id, target: id }]
errorHandling:
  mode: skip
`
	filePathErrSkip, cleanupErrSkip := createTempConfigFile(t, errorSkipDefaultYAML)
	defer cleanupErrSkip()
	cfgErrSkip, err := LoadConfig(filePathErrSkip)
	if err != nil {
		t.Fatalf("LoadConfig() for error skip defaults failed: %v", err)
	}
	if cfgErrSkip.ErrorHandling == nil || cfgErrSkip.ErrorHandling.LogErrors == nil || !*cfgErrSkip.ErrorHandling.LogErrors {
		t.Errorf("cfgErrSkip.ErrorHandling.LogErrors = %v, want defaulted true for skip mode", cfgErrSkip.ErrorHandling.LogErrors)
	}
}

// TestLoadConfig_FileNotFound tests loading a non-existent file.
func TestLoadConfig_FileNotFound(t *testing.T) {
	nonExistentPath := filepath.Join(t.TempDir(), "non_existent_config.yaml")
	_, err := LoadConfig(nonExistentPath)
	if err == nil {
		t.Fatalf("LoadConfig() error = nil, want file not found error")
	}
	if !strings.Contains(err.Error(), "failed to read config file") {
		t.Errorf("LoadConfig() error = %v, want error containing 'failed to read config file'", err)
	}
}

// TestLoadConfig_InvalidYAML tests loading a file with invalid YAML syntax.
func TestLoadConfig_InvalidYAML(t *testing.T) {
	invalidYAML := `
logging: level: info: # Invalid extra colon
source:
  type: csv
  file: /input.csv
mappings:
  - { source: id, target: out_id }
`
	filePath, cleanup := createTempConfigFile(t, invalidYAML)
	defer cleanup()
	_, err := LoadConfig(filePath)
	if err == nil {
		t.Fatalf("LoadConfig() error = nil, want YAML parsing error")
	}
	if !strings.Contains(err.Error(), "failed to parse YAML") && !strings.Contains(err.Error(), "yaml:") {
		t.Errorf("LoadConfig() error = %v, want error containing 'failed to parse YAML' or 'yaml:'", err)
	}
}

// TestLoadConfig_InvalidConfig tests loading valid YAML that fails schema validation.
func TestLoadConfig_InvalidConfig(t *testing.T) {
	invalidConfigYAML := `
logging:
  level: invalid_level
source:
  file: /input.csv
destination:
  type: postgres
  file: /output.json
mappings: [] # Empty mapping list is invalid
dedup:
  keys: ["id"]
  strategy: minimax
errorHandling:
  mode: continue
`
	filePath, cleanup := createTempConfigFile(t, invalidConfigYAML)
	defer cleanup()

	var logBuf strings.Builder
	logging.SetOutput(&logBuf)
	t.Cleanup(func() {
		logging.SetOutput(os.Stderr)
	})

	_, err := LoadConfig(filePath)
	if err == nil {
		t.Fatalf("LoadConfig() error = nil, want validation error")
	}
	assertValidationError(t, err,
		"Config.Logging.Level: invalid log level 'invalid_level'",
		"Config.Source.Type: is required",
		"Config.Destination.TargetTable: is required for destination type 'postgres'",
		"Config.Mappings: at least one mapping rule is required",
		"Config.Dedup.Strategy: invalid strategy 'minimax'",
		"Config.ErrorHandling.Mode: invalid error handling mode 'continue'",
	)
}

// --- ValidateConfig Tests ---

// TestValidateConfig_ValidCases tests various valid configuration snippets.
func TestValidateConfig_ValidCases(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }
	intPtr := func(i int) *int { return &i }

	testCases := []struct {
		name string
		cfg  *ETLConfig
	}{
		{
			name: "Minimal valid CSV to JSON",
			cfg: &ETLConfig{
				Logging: LoggingConfig{Level: "info"},
				Source: SourceConfig{
					Type: "csv",
					File: "in.csv",
				},
				Destination: DestinationConfig{
					Type: "json",
					File: "out.json",
				},
				Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
		},
		{
			name: "Postgres to Postgres with Loader",
			cfg: &ETLConfig{
				Logging: LoggingConfig{Level: "debug"},
				Source: SourceConfig{
					Type:  "postgres",
					Query: "SELECT * FROM source_table",
				},
				Destination: DestinationConfig{
					Type:        "postgres",
					TargetTable: "dest_table",
					Loader: &LoaderConfig{
						Mode:      "sql",
						Command:   "INSERT INTO dest_table (col1, col2) VALUES ($1, $2)",
						Preload:   []string{"TRUNCATE dest_table"},
						Postload:  []string{"ANALYZE dest_table"},
						BatchSize: 1000,
					},
				},
				Mappings: []MappingRule{{Source: "in_col1", Target: "col1"}, {Source: "in_col2", Target: "col2"}},
			},
		},
		{
			name: "XLSX Source with Sheet Name",
			cfg: &ETLConfig{
				Source: SourceConfig{
					Type:      "xlsx",
					File:      "data.xlsx",
					SheetName: "Input Data",
				},
				Destination: DestinationConfig{Type: "json", File: "out.json"},
				Mappings:    []MappingRule{{Source: "A", Target: "ID"}},
			},
		},
		{
			name: "XLSX Source with Sheet Index",
			cfg: &ETLConfig{
				Source: SourceConfig{
					Type:       "xlsx",
					File:       "data.xlsx",
					SheetIndex: intPtr(0),
				},
				Destination: DestinationConfig{Type: "json", File: "out.json"},
				Mappings:    []MappingRule{{Source: "A", Target: "ID"}},
			},
		},
		{
			name: "XML Source/Dest with Tags",
			cfg: &ETLConfig{
				Source: SourceConfig{
					Type:         "xml",
					File:         "in.xml",
					XMLRecordTag: "item",
				},
				Destination: DestinationConfig{
					Type:         "xml",
					File:         "out.xml",
					XMLRecordTag: "product",
					XMLRootTag:   "products",
				},
				Mappings: []MappingRule{{Source: "ItemID", Target: "ProductID"}},
			},
		},
		{
			name: "YAML Source/Dest",
			cfg: &ETLConfig{
				Source: SourceConfig{
					Type: "yaml",
					File: "config.yaml",
				},
				Destination: DestinationConfig{
					Type: "yaml",
					File: "output.yaml",
				},
				Mappings: []MappingRule{{Source: "setting", Target: "value"}},
			},
		},
		{
			name: "Filter expression",
			cfg: &ETLConfig{
				Source:      SourceConfig{Type: "json", File: "in.json"},
				Destination: DestinationConfig{Type: "json", File: "out.json"},
				Filter:      "status == 'active' && value > 0",
				Mappings:    []MappingRule{{Source: "id", Target: "id"}},
			},
		},
		{
			name: "Deduplication Min Strategy",
			cfg: &ETLConfig{
				Source:      SourceConfig{Type: "json", File: "in.json"},
				Destination: DestinationConfig{Type: "json", File: "out.json"},
				Mappings:    []MappingRule{{Source: "key", Target: "key"}, {Source: "val", Target: "val"}},
				Dedup: &DedupConfig{
					Keys:          []string{"key"},
					Strategy:      DedupStrategyMin,
					StrategyField: "val",
				},
			},
		},
		{
			name: "Error Handling Skip No Log",
			cfg: &ETLConfig{
				Source:      SourceConfig{Type: "json", File: "in.json"},
				Destination: DestinationConfig{Type: "json", File: "out.json"},
				Mappings:    []MappingRule{{Source: "id", Target: "id"}},
				ErrorHandling: &ErrorHandlingConfig{
					Mode:      ErrorHandlingModeSkip,
					LogErrors: boolPtr(false),
				},
			},
		},
		{
			name: "Valid Mapping Transforms",
			cfg: &ETLConfig{
				Source:      SourceConfig{Type: "json", File: "in.json"},
				Destination: DestinationConfig{Type: "json", File: "out.json"},
				Mappings: []MappingRule{
					{Source: "date", Target: "formattedDate", Transform: "dateConvert", Params: map[string]interface{}{"inputFormat": "2006-01-02", "outputFormat": "01/02/06"}},
					{Source: "code", Target: "prefix", Transform: "regexExtract:^([A-Z]+)"},
					{Source: "field", Target: "field", Transform: "validateRegex", Params: map[string]interface{}{"pattern": ".+"}},
					{Source: "value", Target: "value", Transform: "validateNumericRange", Params: map[string]interface{}{"min": 0}},
					{Source: "status", Target: "status", Transform: "validateAllowedValues", Params: map[string]interface{}{"values": []interface{}{"A", "B"}}},
					{Source: "pwd", Target: "hash", Transform: "hash", Params: map[string]interface{}{"algorithm": "sha256", "fields": []interface{}{"pwd"}}},
				},
				FIPSMode: false,
			},
		},
	}

	for i := range testCases {
		applyDefaults(testCases[i].cfg)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateConfig(tc.cfg)
			if err != nil {
				t.Errorf("ValidateConfig() returned unexpected error: %v", err)
			}
		})
	}
}

// TestValidateConfig_InvalidCases tests various invalid configuration snippets.
func TestValidateConfig_InvalidCases(t *testing.T) {
	intPtr := func(i int) *int { return &i }

	testCases := []struct {
		name               string
		cfg                *ETLConfig
		expectedErrStrings []string
	}{
		{
			name: "Invalid log level",
			cfg: &ETLConfig{
				Logging: LoggingConfig{Level: "trace"},
				Source:  SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Logging.Level: invalid log level 'trace'"},
		},
		{
			name: "Missing source type",
			cfg: &ETLConfig{
				Source: SourceConfig{File: "in.csv"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Source.Type: is required"},
		},
		{
			name: "Invalid source type",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "parquet", File: "in.pq"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Source.Type: invalid source type 'parquet'"},
		},
		{
			name: "Missing source file for file type",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "csv"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Source.File: is required for source type 'csv'"},
		},
		{
			name: "Missing source query for postgres type",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "postgres"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Source.Query: is required for source type 'postgres'"},
		},
		{
			name: "Invalid CSV delimiter",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "csv", File: "in.csv", Delimiter: ",,"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Source.Delimiter: '\",,\"' must be a single character"},
		},
		{
			name: "Invalid XLSX sheet index",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "xlsx", File: "in.xlsx", SheetIndex: intPtr(-1)}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Source.SheetIndex: cannot be negative"},
		},
		{
			name: "Invalid XLSX sheet name chars",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "xlsx", File: "in.xlsx", SheetName: "My*Sheet"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Source.SheetName: 'My*Sheet' contains invalid characters"},
		},
		{
			name: "Invalid XLSX sheet name length",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "xlsx", File: "in.xlsx", SheetName: strings.Repeat("a", 32)}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"exceeds maximum length of 31 characters"},
		},
		{
			name: "Invalid XML record tag",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "xml", File: "in.xml", XMLRecordTag: "invalid tag"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Source.XMLRecordTag: invalid XML name 'invalid tag': contains invalid characters"},
		},
		{
			name: "Missing destination type",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Destination.Type: is required"},
		},
		{
			name: "Missing destination file for file type",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Destination.File: is required for destination type 'json'"},
		},
		{
			name: "Missing target table for postgres",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "postgres"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Destination.TargetTable: is required for destination type 'postgres'"},
		},
		{
			name: "Invalid postgres loader mode",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "postgres", TargetTable: "t", Loader: &LoaderConfig{Mode: "copy"}}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Destination.Loader.Mode: invalid loader mode 'copy'"},
		},
		{
			name: "Missing command for sql loader mode",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "postgres", TargetTable: "t", Loader: &LoaderConfig{Mode: "sql"}}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Destination.Loader.Command: is required when loader mode is 'sql'"},
		},
		{
			name: "Invalid XML root tag",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "xml", File: "out.xml", XMLRootTag: "1root"}, Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Destination.XMLRootTag: invalid XML name '1root': cannot start with a digit or hyphen"},
		},
		{
			name: "Invalid filter syntax",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Filter: "status ==", Mappings: []MappingRule{{Source: "a", Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Filter: invalid expression syntax"},
		},
		{
			name: "Missing mappings",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{},
			},
			expectedErrStrings: []string{"Config.Mappings: at least one mapping rule is required"},
		},
		{
			name: "Mapping missing source",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Target: "b"}},
			},
			expectedErrStrings: []string{"Config.Mappings[0].Source: is required"},
		},
		{
			name: "Mapping missing target",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a"}},
			},
			expectedErrStrings: []string{"Config.Mappings[0].Target: is required"},
		},
		{
			name: "Mapping duplicate target",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "dup"}, {Source: "b", Target: "dup"}},
			},
			expectedErrStrings: []string{"Config.Mappings[1].Target: duplicate target field 'dup' defined"},
		},
		{
			name: "Mapping unknown transform",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b", Transform: "unknownFunc"}},
			},
			expectedErrStrings: []string{"Config.Mappings[0].Transform: unknown base transformation function 'unknownfunc'"},
		},
		{
			name: "Mapping missing required param (regex)",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b", Transform: "regexExtract"}},
			},
			// This test case now expects the specific error about the missing pattern.
			expectedErrStrings: []string{"Config.Mappings[0].Params: missing required parameter 'pattern' for transform 'regexextract' (and not provided via shorthand)"},
		},
		{
			name: "Mapping invalid param type (regex pattern)",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b", Transform: "regexExtract", Params: map[string]interface{}{"pattern": 123}}},
			},
			expectedErrStrings: []string{"Config.Mappings[0].Params: parameter 'pattern' must be a string"},
		},
		{
			name: "Mapping invalid regex syntax",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b", Transform: "regexExtract", Params: map[string]interface{}{"pattern": "("}}},
			},
			expectedErrStrings: []string{"Config.Mappings[0].Params: invalid regex pattern for 'regexextract'"},
		},
		{
			name: "Mapping numeric range min > max",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b", Transform: "validateNumericRange", Params: map[string]interface{}{"min": 100, "max": 50}}},
			},
			expectedErrStrings: []string{"Config.Mappings[0].Params: 'min' value (100) cannot be greater than 'max' value (50)"},
		},
		{
			name: "Mapping FIPS hash MD5",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b", Transform: "hash", Params: map[string]interface{}{"algorithm": "md5", "fields": []interface{}{"a"}}}}, FIPSMode: true,
			},
			expectedErrStrings: []string{"Config.Mappings[0].Params: hash algorithm 'md5' is not allowed in FIPS mode"},
		},
		{
			name: "Dedup missing keys",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}}, Dedup: &DedupConfig{Strategy: "first"},
			},
			expectedErrStrings: []string{"Config.Dedup.Keys: requires at least one key"},
		},
		{
			name: "Dedup invalid strategy",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}}, Dedup: &DedupConfig{Keys: []string{"b"}, Strategy: "middle"},
			},
			expectedErrStrings: []string{"Config.Dedup.Strategy: invalid strategy 'middle'"},
		},
		{
			name: "Dedup min strategy missing field",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}, {Source: "v", Target: "v"}}, Dedup: &DedupConfig{Keys: []string{"b"}, Strategy: "min"},
			},
			expectedErrStrings: []string{"Config.Dedup.StrategyField: is required when strategy is 'min' or 'max'"},
		},
		{
			name: "Invalid error handling mode",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}}, ErrorHandling: &ErrorHandlingConfig{Mode: "retry"},
			},
			expectedErrStrings: []string{"Config.ErrorHandling.Mode: invalid error handling mode 'retry'"},
		},
		{
			name: "Error file is directory",
			cfg: &ETLConfig{
				Source: SourceConfig{Type: "json", File: "in.json"}, Destination: DestinationConfig{Type: "json", File: "out.json"}, Mappings: []MappingRule{{Source: "a", Target: "b"}}, ErrorHandling: &ErrorHandlingConfig{Mode: "skip", ErrorFile: "/some/path/"},
			},
			expectedErrStrings: []string{"Config.ErrorHandling.ErrorFile: path '/some/path/' appears to be a directory"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			applyDefaults(tc.cfg)
			err := ValidateConfig(tc.cfg)
			if len(tc.expectedErrStrings) > 0 {
				assertValidationError(t, err, tc.expectedErrStrings...)
			} else if err != nil {
				t.Errorf("ValidateConfig() returned unexpected error: %v", err)
			}
		})
	}
}

// --- Helper Validation Function Tests ---

// TestIsValidEnumValue tests the enum validation helper.
func TestIsValidEnumValue(t *testing.T) {
	allowed := []string{"apple", "Banana", "CHERRY"}
	testCases := []struct {
		value string
		want  bool
	}{
		{"apple", true},
		{"Apple", true},
		{"APPLE", true},
		{"banana", true},
		{"BaNaNa", true},
		{"cherry", true},
		{"CHERRY", true},
		{"grape", false},
		{"", false},
		{" banana ", false},
	}
	for _, tc := range testCases {
		got := isValidEnumValue(tc.value, allowed)
		if got != tc.want {
			t.Errorf("isValidEnumValue(%q, %v) = %v, want %v", tc.value, allowed, got, tc.want)
		}
	}
}

// TestValidateSingleRuneString tests the single character validation helper.
func TestValidateSingleRuneString(t *testing.T) {
	testCases := []struct {
		name       string
		input      string
		fieldName  string
		allowEmpty bool
		wantErr    bool
	}{
		{"valid single ascii", ",", "delimiter", false, false},
		{"valid single multibyte", "世", "char", false, false},
		{"valid tab", "\t", "tabChar", false, false},
		{"empty allowed", "", "emptyAllowed", true, false},
		{"empty not allowed", "", "emptyNotAllowed", false, true},
		{"multiple ascii", ",,", "delimiter", false, true},
		{"multiple multibyte", "世界", "char", false, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSingleRuneString(tc.input, tc.fieldName, tc.allowEmpty)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateSingleRuneString(%q, %q, %v) error presence mismatch: got error = %v, wantErr %v", tc.input, tc.fieldName, tc.allowEmpty, err, tc.wantErr)
			}
		})
	}
}

// TestValidateSheetName tests the Excel sheet name validation helper.
func TestValidateSheetName(t *testing.T) {
	testCases := []struct {
		name      string
		sheetName string
		fieldName string
		wantErr   bool
	}{
		{"valid name", "Sheet1", "sheet", false},
		{"valid name with spaces", "My Data Sheet", "sheet", false},
		{"valid name max length", strings.Repeat("a", 31), "sheet", false},
		{"invalid char *", "My*Data", "sheet", true},
		{"invalid char /", "My/Data", "sheet", true},
		{"invalid char \\", "My\\Data", "sheet", true},
		{"invalid char ?", "My?Data", "sheet", true},
		{"invalid char [", "My[Data", "sheet", true},
		{"invalid char ]", "My]Data", "sheet", true},
		{"invalid char :", "My:Data", "sheet", true},
		{"invalid start quote", "'Sheet", "sheet", true},
		{"invalid end quote", "Sheet'", "sheet", true},
		{"invalid too long", strings.Repeat("a", 32), "sheet", true},
		{"invalid empty", "", "sheet", true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSheetName(tc.sheetName, tc.fieldName)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateSheetName(%q, %q) error presence mismatch: got error = %v, wantErr %v", tc.sheetName, tc.fieldName, err, tc.wantErr)
			}
		})
	}
}

// TestValidateXMLName tests the XML tag name validation helper.
func TestValidateXMLName(t *testing.T) {
	testCases := []struct {
		name    string
		xmlName string
		wantErr bool
	}{
		{"valid simple", "record", false},
		{"valid with numbers", "record1", false},
		{"valid with underscore", "record_item", false},
		{"valid with hyphen", "record-item", false},
		{"valid with dot", "record.item", false},
		{"invalid empty", "", true},
		{"invalid starts with number", "1record", true},
		{"invalid starts with hyphen", "-record", true},
		{"invalid starts with dot", ".record", false},
		{"invalid with space", "record item", true},
		{"invalid with ?", "record?", true},
		{"invalid with @", "record@", true},
		{"invalid starts with xml", "xmlRecord", true},
		{"invalid starts with XML", "XMLRecord", true},
		{"invalid starts with Xml", "XmlRecord", true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateXMLName(tc.xmlName)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateXMLName(%q) error presence mismatch: got error = %v, wantErr %v", tc.xmlName, err, tc.wantErr)
			}
		})
	}
}

// TestParseParamAsNumber tests the numeric parameter parsing helper used in validation.
func TestParseParamAsNumber(t *testing.T) {
	testCases := []struct {
		name   string
		input  interface{}
		wantF  float64
		wantOk bool
	}{
		{"int", 10, 10.0, true},
		{"float64", 12.34, 12.34, true},
		{"string int", "100", 100.0, true},
		{"string float", "-5.5", -5.5, true},
		{"string scientific", "1.2e3", 1200.0, true},
		{"string invalid", "abc", 0, false},
		{"bool true", true, 0, false},
		{"nil", nil, 0, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotF, gotOk := parseParamAsNumber(tc.input)
			if gotOk != tc.wantOk {
				t.Errorf("parseParamAsNumber(%v) ok = %v, want %v", tc.input, gotOk, tc.wantOk)
			}
			if gotOk && gotF != tc.wantF {
				t.Errorf("parseParamAsNumber(%v) float = %v, want %v", tc.input, gotF, tc.wantF)
			}
		})
	}
}

// TestParseParamAsInt tests the integer parameter parsing helper used in validation.
func TestParseParamAsInt(t *testing.T) {
	testCases := []struct {
		name   string
		input  interface{}
		wantI  int
		wantOk bool
	}{
		{"int", 10, 10, true},
		{"float64 whole", 12.0, 12, true},
		{"float64 fraction", 12.34, 0, false},
		{"string int", "100", 100, true},
		{"string float whole", "-5.0", -5, true},
		{"string float fraction", "5.5", 0, false},
		{"string invalid", "abc", 0, false},
		{"bool true", true, 0, false},
		{"nil", nil, 0, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotI, gotOk := parseParamAsInt(tc.input)
			if gotOk != tc.wantOk {
				t.Errorf("parseParamAsInt(%v) ok = %v, want %v", tc.input, gotOk, tc.wantOk)
			}
			if gotOk && gotI != tc.wantI {
				t.Errorf("parseParamAsInt(%v) int = %v, want %v", tc.input, gotI, tc.wantI)
			}
		})
	}
}

// TestIsFieldSet tests the reflection helper for checking non-zero fields.
func TestIsFieldSet(t *testing.T) {
	type testStruct struct {
		StrField    string
		IntField    int
		BoolField   bool
		SliceField  []string
		MapField    map[string]int
		PtrField    *string
		NilPtrField *int
	}
	strPtr := "hello"

	testCases := []struct {
		name      string
		instance  interface{}
		fieldName string
		wantSet   bool
	}{
		{"string set", testStruct{StrField: "abc"}, "StrField", true},
		{"string zero (empty)", testStruct{StrField: ""}, "StrField", false},
		{"int set", testStruct{IntField: 1}, "IntField", true},
		{"int zero", testStruct{IntField: 0}, "IntField", false},
		{"bool set (true)", testStruct{BoolField: true}, "BoolField", true},
		{"bool zero (false)", testStruct{BoolField: false}, "BoolField", false},
		{"slice set (non-nil)", testStruct{SliceField: []string{"a"}}, "SliceField", true},
		{"slice zero (nil)", testStruct{SliceField: nil}, "SliceField", false},
		{"slice zero (empty)", testStruct{SliceField: []string{}}, "SliceField", true},
		{"map set (non-nil)", testStruct{MapField: map[string]int{"a": 1}}, "MapField", true},
		{"map zero (nil)", testStruct{MapField: nil}, "MapField", false},
		{"map zero (empty)", testStruct{MapField: map[string]int{}}, "MapField", true},
		{"ptr set (non-nil)", testStruct{PtrField: &strPtr}, "PtrField", true},
		{"ptr zero (nil ptr)", testStruct{NilPtrField: nil}, "NilPtrField", false},
		{"field not exists", testStruct{}, "NonExistentField", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := reflect.ValueOf(tc.instance)
			gotSet := isFieldSet(v, tc.fieldName)
			if gotSet != tc.wantSet {
				t.Errorf("isFieldSet(%#v, %q) = %v, want %v", tc.instance, tc.fieldName, gotSet, tc.wantSet)
			}
		})
	}
}