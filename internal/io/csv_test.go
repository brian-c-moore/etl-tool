package io

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"testing"
)

// --- Test Helpers ---

// createTempCSV creates a temporary CSV file with specific content.
func createTempCSV(t *testing.T, content string) string {
	t.Helper()
	// Reuse createTempFile from io_test_helpers.go context.
	return createTempFile(t, content, "test_*.csv")
}

// readCSVFile reads the content of a CSV file back for verification.
// Uses standard encoding/csv reader for parsing.
func readCSVFile(t *testing.T, filePath string, delimiter rune) [][]string {
	t.Helper()
	f, err := os.Open(filePath)
	if err != nil {
		// If the file doesn't exist, return nil (for tests checking non-creation)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatalf("Failed to open CSV file for reading %s: %v", filePath, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = delimiter
	reader.FieldsPerRecord = -1 // Allow variable fields for testing error cases if needed

	rows, err := reader.ReadAll()
	// Use errors.Is for checking specific error types from csv package
	if err != nil && !errors.Is(err, csv.ErrTrailingComma) && err != io.EOF && !errors.Is(err, csv.ErrQuote) && !errors.Is(err, csv.ErrBareQuote) {
		// Fail only on unexpected errors
		t.Fatalf("Failed to read CSV file %s: %v", filePath, err)
	} else if err != nil {
		// Log expected/tolerated errors
		t.Logf("readCSVFile: encountered expected/tolerated error reading %s: %v", filePath, err)
	}
	return rows
}

// --- Test CSVReader ---

func TestNewCSVReader(t *testing.T) {
	testCases := []struct {
		name        string
		delimiter   string
		commentChar string
		wantDelim   rune
		wantComment rune
		wantErr     bool
		wantErrMsg  string
	}{
		{name: "Default", delimiter: "", commentChar: "", wantDelim: ',', wantComment: 0, wantErr: false},
		{name: "Comma delimiter", delimiter: ",", commentChar: "", wantDelim: ',', wantComment: 0, wantErr: false},
		{name: "Tab delimiter", delimiter: "\t", commentChar: "", wantDelim: '\t', wantComment: 0, wantErr: false},
		{name: "Pipe delimiter", delimiter: "|", commentChar: "", wantDelim: '|', wantComment: 0, wantErr: false},
		{name: "Hash comment", delimiter: ",", commentChar: "#", wantDelim: ',', wantComment: '#', wantErr: false},
		{name: "Semicolon comment", delimiter: ";", commentChar: ";", wantDelim: ';', wantComment: ';', wantErr: false},
		{name: "Empty comment char", delimiter: ",", commentChar: "", wantDelim: ',', wantComment: 0, wantErr: false},
		{name: "Invalid delimiter (multi)", delimiter: ",,", commentChar: "", wantErr: true, wantErrMsg: "invalid delimiter"},
		{name: "Invalid delimiter (empty)", delimiter: "", commentChar: "#", wantDelim: ',', wantComment: '#', wantErr: false}, // Uses default delim
		{name: "Invalid comment (multi)", delimiter: ",", commentChar: "//", wantErr: true, wantErrMsg: "invalid comment character"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := NewCSVReader(tc.delimiter, tc.commentChar)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewCSVReader(%q, %q) error = nil, want error containing %q", tc.delimiter, tc.commentChar, tc.wantErrMsg)
				}
				if tc.wantErrMsg != "" && !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("NewCSVReader(%q, %q) error msg = %q, want error containing %q", tc.delimiter, tc.commentChar, err.Error(), tc.wantErrMsg)
				}
			} else {
				if err != nil {
					t.Fatalf("NewCSVReader(%q, %q) unexpected error: %v", tc.delimiter, tc.commentChar, err)
				}
				if reader == nil {
					t.Fatal("NewCSVReader returned nil reader")
				}
				if reader.Delimiter != tc.wantDelim {
					t.Errorf("reader.Delimiter = %q, want %q", reader.Delimiter, tc.wantDelim)
				}
				if reader.CommentChar != tc.wantComment {
					t.Errorf("reader.CommentChar = %q, want %q", reader.CommentChar, tc.wantComment)
				}
			}
		})
	}
}

func TestCSVReader_Read(t *testing.T) {
	testCases := []struct {
		name        string
		csvContent  string
		delimiter   string
		commentChar string
		wantRecords []map[string]interface{}
		wantErr     bool
		wantErrMsg  string
	}{
		{
			name:        "Valid CSV comma",
			csvContent:  "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,", // Last value empty
			delimiter:   ",",
			commentChar: "",
			wantRecords: []map[string]interface{}{
				{"id": "1", "name": "Alice", "value": "100"},
				{"id": "2", "name": "Bob", "value": "200"},
				{"id": "3", "name": "Charlie", "value": ""},
			},
			wantErr: false,
		},
		{
			name:        "Valid CSV pipe delimiter",
			csvContent:  "key|val\nA|1\nB|2",
			delimiter:   "|",
			commentChar: "",
			wantRecords: []map[string]interface{}{
				{"key": "A", "val": "1"},
				{"key": "B", "val": "2"},
			},
			wantErr: false,
		},
		{
			name:        "With comments",
			csvContent:  "# Header Info\nid,name\n# Comment line\n1,Data1\n# Another comment\n2,Data2",
			delimiter:   ",",
			commentChar: "#",
			wantRecords: []map[string]interface{}{
				{"id": "1", "name": "Data1"},
				{"id": "2", "name": "Data2"},
			},
			wantErr: false,
		},
		{
			name:        "Quoted fields",
			csvContent:  `id,"full name","value"` + "\n" + `1,"Alice Smith","1,000"` + "\n" + `2,"Bob ""The Builder"" Jones","200"`,
			delimiter:   ",",
			commentChar: "",
			wantRecords: []map[string]interface{}{
				{"id": "1", "full name": "Alice Smith", "value": "1,000"},
				{"id": "2", "full name": `Bob "The Builder" Jones`, "value": "200"},
			},
			wantErr: false,
		},
		{
			name:        "Empty file",
			csvContent:  "",
			delimiter:   ",",
			commentChar: "",
			wantRecords: []map[string]interface{}{}, // Expect empty slice
			wantErr:     false,
		},
		{
			name:        "Header only",
			csvContent:  "col1,col2",
			delimiter:   ",",
			commentChar: "",
			wantRecords: []map[string]interface{}{}, // Expect empty slice
			wantErr:     false,
		},
		{
			name:        "Mismatched columns (skip)", // Reader logs warning and skips row
			csvContent:  "h1,h2\nval1,val2\nval3",
			delimiter:   ",",
			commentChar: "",
			wantRecords: []map[string]interface{}{
				{"h1": "val1", "h2": "val2"},
			},
			wantErr: false,
		},
		{
			name:        "Empty header (skip column)", // Reader logs warning, skips column
			csvContent:  "h1,,h3\nv1,v2,v3",
			delimiter:   ",",
			commentChar: "",
			wantRecords: []map[string]interface{}{
				{"h1": "v1", "h3": "v3"}, // Column with empty header is skipped
			},
			wantErr: false,
		},
		{
			name:        "Duplicate headers (last wins)", // Reader logs warning, last occurrence wins
			csvContent:  "h1,h2,h1\nv1,v2,v3",
			delimiter:   ",",
			commentChar: "",
			wantRecords: []map[string]interface{}{
				{"h1": "v3", "h2": "v2"}, // Value for h1 comes from the last 'h1' column
			},
			wantErr: false,
		},
		{
			name:        "CRLF line endings",
			csvContent:  "h1,h2\r\nv1,v2\r\nv3,v4",
			delimiter:   ",",
			commentChar: "",
			wantRecords: []map[string]interface{}{
				{"h1": "v1", "h2": "v2"},
				{"h1": "v3", "h2": "v4"},
			},
			wantErr: false,
		},
		{
			name:        "Malformed CSV (unclosed quote)",
			csvContent:  `"h1,"h2` + "\n" + `"v1,v2`,
			delimiter:   ",",
			commentChar: "",
			wantRecords: nil,
			wantErr:     true,
			wantErrMsg:  "parse error", // encoding/csv error
		},
		{
			name:        "Malformed CSV (trailing comma allowed)",
			csvContent:  "h1,h2,\nv1,v2,", // Add trailing commas
			delimiter:   ",",
			commentChar: "",
			wantRecords: []map[string]interface{}{ // Row has expected number of columns based on header
				{"h1": "v1", "h2": "v2"}, // Trailing comma field value is ignored as there's no header
			},
			wantErr: false, // Allow ErrTrailingComma in readCSVFile helper
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := createTempCSV(t, tc.csvContent)
			reader, errNew := NewCSVReader(tc.delimiter, tc.commentChar)
			if errNew != nil {
				t.Fatalf("NewCSVReader failed: %v", errNew)
			}
			gotRecords, errRead := reader.Read(filePath)

			if tc.wantErr {
				if errRead == nil {
					t.Fatalf("Read() error = nil, want error containing %q", tc.wantErrMsg)
				}
				if tc.wantErrMsg != "" && !strings.Contains(errRead.Error(), tc.wantErrMsg) {
					t.Errorf("Read() error msg = %q, want error containing %q", errRead.Error(), tc.wantErrMsg)
				}
			} else {
				if errRead != nil {
					t.Fatalf("Read() unexpected error: %v", errRead)
				}
				// Use helper for deep comparison
				if !compareRecordsDeep(t, gotRecords, tc.wantRecords) {
					// compareRecordsDeep logs details
				}
			}
		})
	}

	t.Run("File Not Found", func(t *testing.T) {
		reader, _ := NewCSVReader(",", "")
		nonExistentPath := filepath.Join(t.TempDir(), "non_existent_file.csv")
		_, err := reader.Read(nonExistentPath)
		if err == nil {
			t.Fatalf("Read() for non-existent file returned nil error, want error")
		}
		if !errors.Is(err, os.ErrNotExist) {
			// Use errors.Is for checking wrapped errors
			t.Errorf("Read() error type = %T, want os.ErrNotExist", err)
		}
	})
}

// --- Test CSVWriter ---

func TestNewCSVWriter(t *testing.T) {
	testCases := []struct {
		name       string
		delimiter  string
		wantDelim  rune
		wantErr    bool
		wantErrMsg string
	}{
		{name: "Default", delimiter: "", wantDelim: ',', wantErr: false},
		{name: "Comma", delimiter: ",", wantDelim: ',', wantErr: false},
		{name: "Tab", delimiter: "\t", wantDelim: '\t', wantErr: false},
		{name: "Pipe", delimiter: "|", wantDelim: '|', wantErr: false},
		{name: "Invalid delimiter (multi)", delimiter: ",,", wantErr: true, wantErrMsg: "invalid delimiter"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer, err := NewCSVWriter(tc.delimiter)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewCSVWriter(%q) error = nil, want error containing %q", tc.delimiter, tc.wantErrMsg)
				}
				if tc.wantErrMsg != "" && !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("NewCSVWriter(%q) error msg = %q, want error containing %q", tc.delimiter, err.Error(), tc.wantErrMsg)
				}
			} else {
				if err != nil {
					t.Fatalf("NewCSVWriter(%q) unexpected error: %v", tc.delimiter, err)
				}
				if writer == nil {
					t.Fatal("NewCSVWriter returned nil writer")
				}
				if writer.Delimiter != tc.wantDelim {
					t.Errorf("writer.Delimiter = %q, want %q", writer.Delimiter, tc.wantDelim)
				}
			}
		})
	}
}

func TestCSVWriter_WriteAndClose(t *testing.T) {
	// Test records now explicitly include all columns potentially present
	records := []map[string]interface{}{
		{"id": 1, "name": "Alice", "city": "New York", "active": true, "notes": ""}, // notes empty
		{"id": 2, "name": "Bob", "city": "London", "active": nil, "notes": "Some notes\nwith newline"}, // active nil, notes multiline
		{"id": 3, "name": "Charlie", "city": nil, "active": false, "notes": nil}, // city nil, notes nil
	}
	// Headers determined by scanning all keys in the first batch
	wantHeaders := []string{"active", "city", "id", "name", "notes"} // Expected sorted headers including 'notes'
	// Expected rows match the headers order
	wantRows := [][]string{
		{"true", "New York", "1", "Alice", ""},                          // notes is ""
		{"", "London", "2", "Bob", "Some notes\nwith newline"},          // active is "", notes multiline
		{"false", "", "3", "Charlie", ""},                               // city is "", notes is ""
	}

	testCases := []struct {
		name         string
		delimiter    string
		records      []map[string]interface{}
		setupDir     bool
		expectDir    string
		wantHeaders  []string
		wantRows     [][]string
		wantErr      bool
		wantErrMsg   string
		expectCreate bool // Whether the file is expected to be created
	}{
		{
			name:         "Write valid comma",
			delimiter:    ",",
			records:      records,
			wantHeaders:  wantHeaders,
			wantRows:     wantRows,
			wantErr:      false,
			expectCreate: true,
		},
		{
			name:         "Write valid pipe",
			delimiter:    "|",
			records:      records,
			wantHeaders:  wantHeaders,
			wantRows:     wantRows,
			wantErr:      false,
			expectCreate: true,
		},
		{
			name:         "Write empty records (first call)",
			delimiter:    ",",
			records:      []map[string]interface{}{},
			wantHeaders:  nil, // No headers written
			wantRows:     nil, // No rows written
			wantErr:      false,
			expectCreate: true, // File should be created empty
		},
		{
			name:         "Write nil records (first call)",
			delimiter:    ",",
			records:      nil,
			wantHeaders:  nil,
			wantRows:     nil,
			wantErr:      false,
			expectCreate: true, // File should be created empty
		},
		{
			name:      "Write with directory creation",
			delimiter: ",",
			records:   records[:1], // Only first record
			setupDir:  true,
			expectDir: "nested_dir",
			// Headers determined from first record only (now includes notes:"")
			wantHeaders:  []string{"active", "city", "id", "name", "notes"},
			wantRows:     [][]string{{"true", "New York", "1", "Alice", ""}},
			wantErr:      false,
			expectCreate: true,
		},
		{
			name: "Multiple writes build file",
			// Special case handled within the test logic
			expectCreate: true, // Assume file created in this case
		},
	}

	for _, tc := range testCases {
		if tc.name == "Multiple writes build file" {
			continue // Skip special case here
		}
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			filePath := filepath.Join(tmpDir, "output.csv")
			if tc.setupDir {
				filePath = filepath.Join(tmpDir, tc.expectDir, "output.csv")
			}

			writer, errNew := NewCSVWriter(tc.delimiter)
			if errNew != nil {
				t.Fatalf("NewCSVWriter failed: %v", errNew)
			}

			writeErr := writer.Write(tc.records, filePath)
			closeErr := writer.Close()

			finalErr := writeErr
			if finalErr == nil {
				finalErr = closeErr
			}

			if tc.wantErr {
				if finalErr == nil {
					t.Fatalf("Write/Close error = nil, want error containing %q", tc.wantErrMsg)
				}
				if tc.wantErrMsg != "" && !strings.Contains(finalErr.Error(), tc.wantErrMsg) {
					t.Errorf("Write/Close error msg = %q, want error containing %q", finalErr.Error(), tc.wantErrMsg)
				}
			} else {
				if finalErr != nil {
					t.Fatalf("Write/Close unexpected error: %v", finalErr)
				}

				if tc.expectDir != "" {
					dirPath := filepath.Join(tmpDir, tc.expectDir)
					if _, statErr := os.Stat(dirPath); os.IsNotExist(statErr) {
						t.Errorf("Expected directory %s was not created", dirPath)
					}
				}

				// Check file existence based on expectCreate flag
				_, errStat := os.Stat(filePath)
				if tc.expectCreate {
					if os.IsNotExist(errStat) {
						t.Fatalf("Expected file %s to exist, but it doesn't", filePath)
					} else if errStat != nil {
						t.Fatalf("Error checking output file %s: %v", filePath, errStat)
					}
				} else {
					// This branch is less likely now as we always create on first call
					if errStat == nil {
						t.Errorf("File %s exists, but expected it not to be created", filePath)
					} else if !os.IsNotExist(errStat) {
						t.Fatalf("Error checking output file status %s: %v", filePath, errStat)
					}
				}

				// Verify file content only if created and expected content exists
				if tc.expectCreate && (len(tc.wantHeaders) > 0 || len(tc.wantRows) > 0) {
					gotRows := readCSVFile(t, filePath, []rune(tc.delimiter)[0])
					// Allow empty file if no headers/rows were expected
					if len(gotRows) == 0 && (len(tc.wantRows) == 0 && len(tc.wantHeaders) == 0) {
						// This is okay, file was created empty
					} else if len(gotRows) == 0 && (len(tc.wantRows) > 0 || len(tc.wantHeaders) > 0) {
						t.Fatalf("Expected header and/or data rows, but got empty file content")
					} else if len(tc.wantHeaders) > 0 { // Only check header if expected
						if len(gotRows) == 0 { // Should have been caught above, but defense
							t.Fatalf("Expected header %v, but file is empty", tc.wantHeaders)
						}
						if !reflect.DeepEqual(gotRows[0], tc.wantHeaders) {
							t.Errorf("Header mismatch:\ngot:  %v\nwant: %v", gotRows[0], tc.wantHeaders)
						}
						dataRows := [][]string{}
						if len(gotRows) > 1 {
							dataRows = gotRows[1:]
						}
						if len(tc.wantRows) > 0 { // Only check data rows if expected
							if !reflect.DeepEqual(dataRows, tc.wantRows) {
								t.Errorf("Data rows mismatch:\ngot:  %v\nwant: %v", dataRows, tc.wantRows)
							}
						} else if len(dataRows) > 0 { // Header might exist, but no data rows expected
							t.Errorf("Expected no data rows, but got %d", len(dataRows))
						}
					} else if len(gotRows) > 0 { // No header expected, but got rows
						t.Errorf("Expected no header or data rows, but got %d rows", len(gotRows))
					}

				} else if tc.expectCreate { // Check if file is empty when no content expected
					contentBytes, _ := os.ReadFile(filePath)
					if len(contentBytes) > 0 {
						t.Errorf("Expected empty file %s, but it has content: %q", filePath, string(contentBytes))
					}
				}
			}
		})
	}

	// Test multiple writes to the same writer instance
	t.Run("Multiple writes build file", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "output_multi.csv")
		writer, _ := NewCSVWriter(",")

		record1 := []map[string]interface{}{{"a": 1, "b": 2}}
		record2 := []map[string]interface{}{{"a": 3, "b": 4, "c": 5}} // Adds column 'c'
		record3 := []map[string]interface{}{{"a": 6}}                 // Only 'a'

		err1 := writer.Write(record1, filePath) // Headers {a, b} written
		err2 := writer.Write(record2, filePath) // Uses established headers {a, b}
		err3 := writer.Write(record3, filePath) // Uses established headers {a, b}
		closeErr := writer.Close()

		if err1 != nil || err2 != nil || err3 != nil || closeErr != nil {
			t.Fatalf("Multiple writes unexpected errors: err1=%v, err2=%v, err3=%v, closeErr=%v", err1, err2, err3, closeErr)
		}

		gotRows := readCSVFile(t, filePath, ',')
		wantFullRows := [][]string{
			{"a", "b"}, // Headers from first write only
			{"1", "2"},
			{"3", "4"}, // Row from second write, missing "c"
			{"6", ""},  // Row from third write, missing "b"
		}
		if !reflect.DeepEqual(gotRows, wantFullRows) {
			t.Errorf("Multiple writes content mismatch:\ngot:  %v\nwant: %v", gotRows, wantFullRows)
		}
	})

	// Test closing idempotency
	t.Run("Close idempotency", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "close_idem.csv")
		writer, _ := NewCSVWriter(",")
		// Write something to ensure file is created
		_ = writer.Write([]map[string]interface{}{{"a": 1}}, filePath)

		err1 := writer.Close() // First close
		err2 := writer.Close() // Second close
		if err1 != nil {
			t.Errorf("First Close() failed: %v", err1)
		}
		if err2 != nil {
			t.Errorf("Second Close() failed (should be idempotent): %v", err2)
		}
	})

	t.Run("Directory Creation Failure", func(t *testing.T) {
		tmpDir := t.TempDir()
		// Create a file where a directory is expected
		conflictingFilePath := filepath.Join(tmpDir, "nested_dir_file")
		if err := os.WriteFile(conflictingFilePath, []byte("i am a file"), 0644); err != nil {
			t.Fatalf("Failed to create conflicting file: %v", err)
		}
		// Attempt to write where directory creation will fail
		filePath := filepath.Join(conflictingFilePath, "output.csv")
		writer, _ := NewCSVWriter(",")
		// Write should fail because os.MkdirAll fails
		err := writer.Write(records[:1], filePath) // Write triggers dir creation attempt
		writer.Close()                              // Close shouldn't cause issues

		if err == nil {
			t.Fatalf("Write did not return error when directory creation should fail")
		}

		// Check if the error message indicates directory creation failure
		// Error might be wrapped, so check substrings
		if !strings.Contains(err.Error(), "create directory") || (!strings.Contains(strings.ToLower(err.Error()), "not a directory") && !strings.Contains(strings.ToLower(err.Error()), "is a file")) {
			t.Errorf("Write error message %q does not indicate directory creation failure ('create directory' or 'not a directory' or 'is a file')", err.Error())
		}
	})
}

// --- Test CSVErrorWriter ---

func TestNewCSVErrorWriter(t *testing.T) {
	t.Run("Successful creation", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "errors.csv")

		writer, err := NewCSVErrorWriter(filePath)
		if err != nil {
			t.Fatalf("NewCSVErrorWriter() unexpected error: %v", err)
		}
		if writer == nil {
			t.Fatal("NewCSVErrorWriter() returned nil writer")
		}
		defer writer.Close() // Ensure cleanup

		if writer.filePath != filePath {
			t.Errorf("writer.filePath = %q, want %q", writer.filePath, filePath)
		}
		if writer.file == nil {
			t.Error("writer.file is nil after successful creation")
		}
		if writer.writer == nil {
			t.Error("writer.writer is nil after successful creation")
		}
		if writer.closed {
			t.Error("writer.closed is true after successful creation")
		}
	})

	t.Run("Directory creation", func(t *testing.T) {
		tmpDir := t.TempDir()
		nestedDir := filepath.Join(tmpDir, "errors_subdir")
		filePath := filepath.Join(nestedDir, "errors.csv")

		writer, err := NewCSVErrorWriter(filePath)
		if err != nil {
			t.Fatalf("NewCSVErrorWriter() with nested dir failed: %v", err)
		}
		defer writer.Close()

		if _, statErr := os.Stat(nestedDir); os.IsNotExist(statErr) {
			t.Errorf("Expected directory %s was not created", nestedDir)
		}
	})

	t.Run("File creation failure (permission)", func(t *testing.T) {
		// Create a read-only directory
		tmpDir := t.TempDir()
		readOnlyDir := filepath.Join(tmpDir, "read_only_dir")
		if err := os.Mkdir(readOnlyDir, 0555); err != nil { // Read and execute permissions only
			t.Fatalf("Failed to create read-only directory: %v", err)
		}
		// Attempt to create error file inside read-only dir
		filePath := filepath.Join(readOnlyDir, "errors.csv")
		_, err := NewCSVErrorWriter(filePath)
		if err == nil {
			t.Fatalf("NewCSVErrorWriter() succeeded unexpectedly in read-only directory")
		}
		// Check for permission error using errors.Is
		if !errors.Is(err, os.ErrPermission) {
			t.Errorf("NewCSVErrorWriter() error = %v, want os.ErrPermission", err)
		}
	})
}

func TestCSVErrorWriter_WriteAndClose(t *testing.T) {
	record1 := map[string]interface{}{"id": 1, "data": "good"}
	error1 := errors.New("processing failed")
	record2 := map[string]interface{}{"id": 2, "data": "bad", "reason": "validation"} // Has extra 'reason' field
	error2 := fmt.Errorf("reason: %s", "field mismatch")

	t.Run("Write to new file", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "new_errors.csv")
		writer, _ := NewCSVErrorWriter(filePath)

		errW1 := writer.Write(record1, error1)
		errW2 := writer.Write(record2, error2) // Headers determined by first write (record1)
		errC := writer.Close()

		if errW1 != nil || errW2 != nil || errC != nil {
			t.Fatalf("Write/Close errors: w1=%v, w2=%v, c=%v", errW1, errW2, errC)
		}

		// Verify content
		rows := readCSVFile(t, filePath, ',')
		if len(rows) != 3 { // Header + 2 rows
			t.Fatalf("Expected 3 rows, got %d", len(rows))
		}
		// Headers should be sorted keys of FIRST record + error message
		wantHeaders := []string{"data", "id", "etl_error_message"} // Sorted keys of record1 + error
		if !reflect.DeepEqual(rows[0], wantHeaders) {
			t.Errorf("Header mismatch:\ngot:  %v\nwant: %v", rows[0], wantHeaders)
		}
		// Data rows - order matches header from first write
		wantRow1 := []string{"good", "1", "processing failed"}
		wantRow2 := []string{"bad", "2", "reason: field mismatch"} // 'reason' key not in headers, ignored
		if !reflect.DeepEqual(rows[1], wantRow1) {
			t.Errorf("Row 1 mismatch:\ngot:  %v\nwant: %v", rows[1], wantRow1)
		}
		if !reflect.DeepEqual(rows[2], wantRow2) {
			t.Errorf("Row 2 mismatch:\ngot:  %v\nwant: %v", rows[2], wantRow2)
		}
	})

	t.Run("Append to existing file", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "append_errors.csv")
		// Pre-populate file with header and one row (matching format of record1)
		initialContent := "data,id,etl_error_message\nold_data,0,old_error\n"
		if err := os.WriteFile(filePath, []byte(initialContent), 0644); err != nil {
			t.Fatalf("Failed to create initial error file: %v", err)
		}

		writer, _ := NewCSVErrorWriter(filePath)

		errW1 := writer.Write(record1, error1) // Should append, headers read implicitly
		errW2 := writer.Write(record2, error2) // Should append, uses same established headers
		errC := writer.Close()

		if errW1 != nil || errW2 != nil || errC != nil {
			t.Fatalf("Append Write/Close errors: w1=%v, w2=%v, c=%v", errW1, errW2, errC)
		}

		// Verify content
		rows := readCSVFile(t, filePath, ',')
		if len(rows) != 4 { // Header + 1 existing data row + 2 new data rows
			t.Fatalf("Append: Expected 4 rows, got %d", len(rows))
		}
		// Verify header hasn't changed
		wantHeaders := []string{"data", "id", "etl_error_message"}
		if !reflect.DeepEqual(rows[0], wantHeaders) {
			t.Errorf("Append Header mismatch:\ngot:  %v\nwant: %v", rows[0], wantHeaders)
		}
		// Verify original data is still there
		wantOriginalRow := []string{"old_data", "0", "old_error"}
		if !reflect.DeepEqual(rows[1], wantOriginalRow) {
			t.Errorf("Append Original Row mismatch:\ngot:  %v\nwant: %v", rows[1], wantOriginalRow)
		}
		// Verify appended rows
		wantRowAppended1 := []string{"good", "1", "processing failed"}
		wantRowAppended2 := []string{"bad", "2", "reason: field mismatch"} // 'reason' column still not present
		if !reflect.DeepEqual(rows[2], wantRowAppended1) {
			t.Errorf("Append Row 2 mismatch:\ngot:  %v\nwant: %v", rows[2], wantRowAppended1)
		}
		if !reflect.DeepEqual(rows[3], wantRowAppended2) {
			t.Errorf("Append Row 3 mismatch:\ngot:  %v\nwant: %v", rows[3], wantRowAppended2)
		}
	})

	t.Run("Write with nil error", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "nil_error.csv")
		writer, _ := NewCSVErrorWriter(filePath)
		defer writer.Close()

		errW := writer.Write(record1, nil) // Write with nil error
		if errW != nil {
			t.Fatalf("Write with nil error failed: %v", errW)
		}
		writer.Close()

		rows := readCSVFile(t, filePath, ',')
		if len(rows) != 2 { // Header + 1 data row
			t.Fatalf("Nil error: Expected 2 rows, got %d", len(rows))
		}
		wantRow := []string{"good", "1", ""} // Error message column should be empty
		if !reflect.DeepEqual(rows[1], wantRow) {
			t.Errorf("Nil error Row mismatch:\ngot:  %v\nwant: %v", rows[1], wantRow)
		}
	})

	// Test closing idempotency and writing after close
	t.Run("Close idempotency and write after close", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "close_idem.csv")
		writer, _ := NewCSVErrorWriter(filePath)
		// Write something initially to establish headers
		_ = writer.Write(record1, error1)

		err1 := writer.Close() // First close
		err2 := writer.Close() // Second close

		if err1 != nil {
			t.Errorf("First Close() failed: %v", err1)
		}
		if err2 != nil {
			t.Errorf("Second Close() failed (should be idempotent): %v", err2)
		}

		// Try writing after close - should return an error
		errWAfter := writer.Write(record1, error1)
		if errWAfter == nil {
			t.Errorf("Write() after Close() did not return an error")
		} else {
			// Check for the specific error message
			expectedErr := "CSVErrorWriter: write called on closed writer"
			if errWAfter.Error() != expectedErr {
				t.Errorf("Write() after Close() error mismatch: got %q, want %q", errWAfter.Error(), expectedErr)
			}
		}
	})

	// Simulate write error (e.g., disk full) - check the close error path.
	t.Run("Simulate write error on close", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "write_err.csv")
		writer, _ := NewCSVErrorWriter(filePath)

		// Manually close the underlying file handle *before* calling Close().
		// Crucially, DO NOT set writer.file = nil here.
		writer.mu.Lock()
		var underlyingFileErr error
		if writer.file != nil {
			underlyingFileErr = writer.file.Close() // Close the file handle prematurely
			if underlyingFileErr != nil {
				t.Logf("Error closing underlying file for test setup: %v", underlyingFileErr)
				// Don't fail the test here, proceed to Close() check
			}
			// DO NOT SET writer.file = nil
		} else {
			// This part of the setup failed, cannot proceed reliably
			writer.mu.Unlock()
			t.Fatalf("Test setup error: writer.file was already nil before manual close attempt.")
		}
		writer.mu.Unlock()

		// Now call the writer's Close() method.
		// It should attempt Flush() (which might or might not error depending on buffer state),
		// and then attempt file.Close(), which *should* error because it's already closed.
		closeErr := writer.Close()

		if closeErr == nil {
			t.Errorf("Close() did not return an error after simulated write failure (closed file handle)")
		} else {
			// Check if the error message indicates a close error or possibly a flush error.
			t.Logf("Close() returned expected error after simulated write failure: %v", closeErr)
			// os.ErrClosed is the most likely error from file.Close()
			if !errors.Is(closeErr, os.ErrClosed) && // Check for specific closed error
				!strings.Contains(closeErr.Error(), "flush error") && // Also accept if flush failed first
				!strings.Contains(closeErr.Error(), "bad file descriptor") {
				t.Errorf("Close() error message %q doesn't indicate expected close/flush error (e.g. os.ErrClosed, 'flush error')", closeErr.Error())
			}
		}
	})
}