package io

import (
	"reflect"
	"strings"
	"testing"

	"etl-tool/internal/config"
)

// --- Test NewInputReader ---

func TestNewInputReader(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name        string
		cfg         config.SourceConfig
		dbConnStr   string
		wantErr     bool
		wantErrMsg  string // Specific substring expected in the error
		wantType    reflect.Type
	}{
		// --- Valid Cases (Keep existing valid cases) ---
		{
			name:     "JSON Reader",
			cfg:      config.SourceConfig{Type: "json", File: "input.json"},
			wantType: reflect.TypeOf(&JSONReader{}),
			wantErr:  false,
		},
		{
			name: "CSV Reader Valid", // Renamed slightly
			cfg: config.SourceConfig{
				Type:      "csv",
				File:      "input.csv",
				Delimiter: ",",
			},
			wantType: reflect.TypeOf(&CSVReader{}), // Expect pointer type
			wantErr:  false,
		},
		{
			name: "XLSX Reader",
			cfg: config.SourceConfig{
				Type:      "xlsx",
				File:      "input.xlsx",
				SheetName: "Data",
			},
			wantType: reflect.TypeOf(&XLSXReader{}),
			wantErr:  false,
		},
		{
			name: "XML Reader",
			cfg: config.SourceConfig{
				Type:         "xml",
				File:         "input.xml",
				XMLRecordTag: "item",
			},
			wantType: reflect.TypeOf(&XMLReader{}),
			wantErr:  false,
		},
		{
			name:     "YAML Reader",
			cfg:      config.SourceConfig{Type: "yaml", File: "input.yaml"},
			wantType: reflect.TypeOf(&YAMLReader{}),
			wantErr:  false,
		},
		{
			name: "Postgres Reader Valid", // Renamed slightly
			cfg: config.SourceConfig{
				Type:  "postgres",
				Query: "SELECT * FROM source_table",
			},
			dbConnStr: "postgres://user:pass@host/db",
			wantType:  reflect.TypeOf(&PostgresReader{}),
			wantErr:   false,
		},
		{
			name:     "Case Insensitive Type (CSV)",
			cfg:      config.SourceConfig{Type: "Csv", File: "input.csv"},
			wantType: reflect.TypeOf(&CSVReader{}), // Expect pointer type
			wantErr:  false,
		},
		// --- Error Cases ---
		{
			name:        "Unsupported Type",
			cfg:         config.SourceConfig{Type: "parquet", File: "input.pq"},
			wantErr:     true,
			wantErrMsg:  "unsupported source type 'parquet'",
		},
		{
			name:        "Postgres Missing Connection String",
			cfg:         config.SourceConfig{Type: "postgres", Query: "SELECT 1"},
			dbConnStr:   "",
			wantErr:     true,
			wantErrMsg:  "database connection string (-db or DB_CREDENTIALS) is required for source type 'postgres'",
		},
		{
			name:        "Postgres Missing Query",
			cfg:         config.SourceConfig{Type: "postgres"},
			dbConnStr:   "postgres://user:pass@host/db",
			wantErr:     true,
			wantErrMsg:  "query is required in source config for type 'postgres'",
		},
		// --- ADDED: Test errors from underlying constructors ---
		{
			name: "CSV Invalid Delimiter (propagated)",
			cfg: config.SourceConfig{
				Type:      "csv",
				File:      "input.csv",
				Delimiter: ",,", // Invalid delimiter for NewCSVReader
			},
			wantErr:    true,
			wantErrMsg: "failed to create CSV reader: invalid delimiter", // Check for wrapped error
		},
		{
			name: "CSV Invalid Comment Char (propagated)",
			cfg: config.SourceConfig{
				Type:        "csv",
				File:        "input.csv",
				CommentChar: "//", // Invalid comment char for NewCSVReader
			},
			wantErr:    true,
			wantErrMsg: "failed to create CSV reader: invalid comment character", // Check for wrapped error
		},
		// Add similar propagated error tests for other types if their constructors can fail (currently XLSX, XML, YAML, JSON readers don't have failing constructors in this setup)
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := NewInputReader(tc.cfg, tc.dbConnStr)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewInputReader(%+v) error = nil, want error containing %q", tc.cfg, tc.wantErrMsg)
				}
				if tc.wantErrMsg != "" && !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("NewInputReader(%+v) error message = %q, want error containing %q", tc.cfg, err.Error(), tc.wantErrMsg)
				}
				if reader != nil {
					t.Errorf("NewInputReader(%+v) reader = %T, want nil when error occurs", tc.cfg, reader)
				}
			} else {
				if err != nil {
					t.Fatalf("NewInputReader(%+v) returned unexpected error: %v", tc.cfg, err)
				}
				if reader == nil {
					t.Fatalf("NewInputReader(%+v) returned nil reader, want type %v", tc.cfg, tc.wantType)
				}
				gotType := reflect.TypeOf(reader)
				if gotType != tc.wantType {
					wantTypeName := "nil"
					if tc.wantType != nil {
						wantTypeName = tc.wantType.String()
					}
					gotTypeName := "nil"
					if gotType != nil {
						gotTypeName = gotType.String()
					}
					t.Errorf("NewInputReader(%+v) returned type %s, want %s", tc.cfg, gotTypeName, wantTypeName)
				}
			}
		})
	}
}

// --- Test NewOutputWriter ---

func TestNewOutputWriter(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name        string
		cfg         config.DestinationConfig
		dbConnStr   string
		wantErr     bool
		wantErrMsg  string // Specific substring expected in the error
		wantType    reflect.Type
	}{
		// --- Valid Cases (Keep existing valid cases) ---
		{
			name:     "JSON Writer",
			cfg:      config.DestinationConfig{Type: "json", File: "output.json"},
			wantType: reflect.TypeOf(&JSONWriter{}),
			wantErr:  false,
		},
		{
			name: "CSV Writer Valid", // Renamed slightly
			cfg: config.DestinationConfig{
				Type:      "csv",
				File:      "output.csv",
				Delimiter: "|",
			},
			wantType: reflect.TypeOf(&CSVWriter{}), // Expect pointer type
			wantErr:  false,
		},
		{
			name: "XLSX Writer",
			cfg: config.DestinationConfig{
				Type:      "xlsx",
				File:      "output.xlsx",
				SheetName: "Results",
			},
			wantType: reflect.TypeOf(&XLSXWriter{}),
			wantErr:  false,
		},
		{
			name: "XML Writer",
			cfg: config.DestinationConfig{
				Type:         "xml",
				File:         "output.xml",
				XMLRecordTag: "result",
				XMLRootTag:   "results",
			},
			wantType: reflect.TypeOf(&XMLWriter{}),
			wantErr:  false,
		},
		{
			name:     "YAML Writer",
			cfg:      config.DestinationConfig{Type: "yaml", File: "output.yaml"},
			wantType: reflect.TypeOf(&YAMLWriter{}),
			wantErr:  false,
		},
		{
			name: "Postgres Writer Valid", // Renamed slightly
			cfg: config.DestinationConfig{
				Type:        "postgres",
				TargetTable: "destination_table",
			},
			dbConnStr: "postgres://user:pass@host/db",
			wantType:  reflect.TypeOf(&PostgresWriter{}),
			wantErr:   false,
		},
		{
			name: "Postgres Writer with Loader",
			cfg: config.DestinationConfig{
				Type:        "postgres",
				TargetTable: "destination_table",
				Loader: &config.LoaderConfig{
					Mode:    config.LoaderModeSQL,
					Command: "INSERT INTO...",
				},
			},
			dbConnStr: "postgres://user:pass@host/db",
			wantType:  reflect.TypeOf(&PostgresWriter{}),
			wantErr:   false,
		},
		{
			name:     "Case Insensitive Type (JSON)",
			cfg:      config.DestinationConfig{Type: "jSoN", File: "output.json"},
			wantType: reflect.TypeOf(&JSONWriter{}),
			wantErr:  false,
		},
		// --- Error Cases ---
		{
			name:        "Unsupported Type",
			cfg:         config.DestinationConfig{Type: "avro", File: "output.avro"},
			wantErr:     true,
			wantErrMsg:  "unsupported destination type 'avro'",
		},
		{
			name:        "Postgres Missing Connection String",
			cfg:         config.DestinationConfig{Type: "postgres", TargetTable: "dest_table"},
			dbConnStr:   "",
			wantErr:     true,
			wantErrMsg:  "database connection string (-db or DB_CREDENTIALS) is required for destination type 'postgres'",
		},
		{
			name:        "Postgres Missing Target Table",
			cfg:         config.DestinationConfig{Type: "postgres"},
			dbConnStr:   "postgres://user:pass@host/db",
			wantErr:     true,
			wantErrMsg:  "target_table is required in destination config for type 'postgres'",
		},
		// --- ADDED: Test errors from underlying constructors ---
		{
			name: "CSV Invalid Delimiter (propagated)",
			cfg: config.DestinationConfig{
				Type:      "csv",
				File:      "out.csv",
				Delimiter: ";;", // Invalid delimiter for NewCSVWriter
			},
			wantErr:    true,
			wantErrMsg: "failed to create CSV writer: invalid delimiter", // Check for wrapped error
		},
		// Add similar propagated error tests for other types if their constructors can fail
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer, err := NewOutputWriter(tc.cfg, tc.dbConnStr)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewOutputWriter(%+v) error = nil, want error containing %q", tc.cfg, tc.wantErrMsg)
				}
				if tc.wantErrMsg != "" && !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("NewOutputWriter(%+v) error message = %q, want error containing %q", tc.cfg, err.Error(), tc.wantErrMsg)
				}
				if writer != nil {
					t.Errorf("NewOutputWriter(%+v) writer = %T, want nil when error occurs", tc.cfg, writer)
				}
			} else {
				if err != nil {
					t.Fatalf("NewOutputWriter(%+v) returned unexpected error: %v", tc.cfg, err)
				}
				if writer == nil {
					t.Fatalf("NewOutputWriter(%+v) returned nil writer, want type %v", tc.cfg, tc.wantType)
				}
				gotType := reflect.TypeOf(writer)
				if gotType != tc.wantType {
					wantTypeName := "nil"
					if tc.wantType != nil {
						wantTypeName = tc.wantType.String()
					}
					gotTypeName := "nil"
					if gotType != nil {
						gotTypeName = gotType.String()
					}
					t.Errorf("NewOutputWriter(%+v) returned type %s, want %s", tc.cfg, gotTypeName, wantTypeName)
				}
			}
		})
	}
}
