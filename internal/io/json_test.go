package io

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- Test JSONReader ---

func TestJSONReader_Read(t *testing.T) {
	testCases := []struct {
		name        string
		jsonContent string
		wantRecords []map[string]interface{}
		wantErr     bool
		wantErrMsg  string
	}{
		{
			name: "Valid JSON array",
			jsonContent: `[
				{"id": 1, "name": "Alice", "active": true},
				{"id": 2, "name": "Bob", "city": "New York"},
				{"id": 3, "name": "Charlie", "tags": ["a", "b"]}
			]`,
			wantRecords: []map[string]interface{}{
				{"id": float64(1), "name": "Alice", "active": true},
				{"id": float64(2), "name": "Bob", "city": "New York"},
				{"id": float64(3), "name": "Charlie", "tags": []interface{}{"a", "b"}},
			},
			wantErr: false,
		},
		{
			name: "Valid single JSON object",
			jsonContent: `{
				"id": 100, "value": "single"
			}`,
			wantRecords: []map[string]interface{}{
				{"id": float64(100), "value": "single"},
			},
			wantErr: false,
		},
		{
			name:        "Empty JSON array",
			jsonContent: `[]`,
			wantRecords: []map[string]interface{}{},
			wantErr:     false,
		},
		{
			name:        "JSON array with empty object",
			jsonContent: `[{}]`,
			wantRecords: []map[string]interface{}{
				{},
			},
			wantErr: false,
		},
		{
			name:        "JSON with nested structure",
			jsonContent: `[{"id": 1, "data": {"nested_key": "nested_value", "nested_arr": [1, 2]}}]`,
			wantRecords: []map[string]interface{}{
				{"id": float64(1), "data": map[string]interface{}{"nested_key": "nested_value", "nested_arr": []interface{}{float64(1), float64(2)}}},
			},
			wantErr: false,
		},
		{
			name:        "Empty file",
			jsonContent: ``,
			wantRecords: nil,
			wantErr:     true,
			wantErrMsg:  "unexpected end of JSON input",
		},
		{
			name:        "Malformed JSON (missing comma)",
			jsonContent: `[{"id": 1} {"id": 2}]`,
			wantRecords: nil,
			wantErr:     true,
			wantErrMsg:  "invalid character",
		},
		{
			name:        "Malformed JSON (trailing comma)",
			jsonContent: `[{"id": 1},]`,
			wantRecords: nil,
			wantErr:     true,
			wantErrMsg:  "invalid character", // Standard json lib often gives this for trailing comma
		},
		{
			name:        "Not JSON (plain text)",
			jsonContent: `just plain text`,
			wantRecords: nil,
			wantErr:     true,
			wantErrMsg:  "invalid character",
		},
		{
			name:        "Not JSON (XML)",
			jsonContent: `<tag>value</tag>`,
			wantRecords: nil,
			wantErr:     true,
			wantErrMsg:  "invalid character",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := createTempJSON(t, tc.jsonContent) // Use helper from common file
			reader := JSONReader{}
			gotRecords, err := reader.Read(filePath)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("Read() error = nil, want error containing %q", tc.wantErrMsg)
				}
				if tc.wantErrMsg != "" && !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("Read() error message = %q, want error containing %q", err.Error(), tc.wantErrMsg)
				}
				if gotRecords != nil {
					t.Errorf("Read() gotRecords = %v, want nil when error occurs", gotRecords)
				}
			} else {
				if err != nil {
					t.Fatalf("Read() returned unexpected error: %v", err)
				}
				// Use shared helper
				if !compareRecordsDeep(t, gotRecords, tc.wantRecords) {
					// compareRecordsDeep already logs details on failure
				}
			}
		})
	}

	t.Run("File Not Found", func(t *testing.T) {
		reader := JSONReader{}
		nonExistentPath := filepath.Join(t.TempDir(), "non_existent_file.json")
		_, err := reader.Read(nonExistentPath)
		if err == nil {
			t.Fatalf("Read() for non-existent file returned nil error, want error")
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("Read() error type = %T, want os.ErrNotExist", err)
		}
	})
}

// --- Test JSONWriter ---

func TestJSONWriter_Write(t *testing.T) {
	records := []map[string]interface{}{
		{"col_a": "value1", "col_b": 100, "col_c": true},
		{"col_a": "value2", "col_b": nil, "col_d": []int{1, 2}},
	}

	testCases := []struct {
		name        string
		records     []map[string]interface{}
		setupDir    bool
		expectDir   string
		wantContent string
		wantErr     bool
		wantErrMsg  string
	}{
		{
			name:    "Write valid records",
			records: records,
			wantContent: `[
  {
    "col_a": "value1",
    "col_b": 100,
    "col_c": true
  },
  {
    "col_a": "value2",
    "col_b": null,
    "col_d": [
      1,
      2
    ]
  }
]
`,
			wantErr: false,
		},
		{
			name:        "Write empty record slice",
			records:     []map[string]interface{}{},
			wantContent: "[]\n",
			wantErr:     false,
		},
		{
			name:        "Write nil record slice",
			records:     nil,
			wantContent: "[]\n",
			wantErr:     false,
		},
		{
			name:      "Write with directory creation",
			records:   records[:1],
			setupDir:  true,
			expectDir: "nested_dir",
			wantContent: `[
  {
    "col_a": "value1",
    "col_b": 100,
    "col_c": true
  }
]
`,
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			filePath := filepath.Join(tmpDir, "output.json")
			if tc.setupDir {
				filePath = filepath.Join(tmpDir, tc.expectDir, "output.json")
			}

			writer := JSONWriter{}
			err := writer.Write(tc.records, filePath)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("Write() error = nil, want error containing %q", tc.wantErrMsg)
				}
				if tc.wantErrMsg != "" && !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("Write() error message = %q, want error containing %q", err.Error(), tc.wantErrMsg)
				}
			} else {
				if err != nil {
					t.Fatalf("Write() returned unexpected error: %v", err)
				}

				if tc.expectDir != "" {
					dirPath := filepath.Join(tmpDir, tc.expectDir)
					if _, statErr := os.Stat(dirPath); os.IsNotExist(statErr) {
						t.Errorf("Expected directory %s was not created", dirPath)
					}
				}

				contentBytes, readErr := os.ReadFile(filePath)
				if readErr != nil {
					t.Fatalf("Failed to read back output file %s: %v", filePath, readErr)
				}
				gotContent := string(contentBytes)
				if gotContent != tc.wantContent {
					t.Errorf("Write() file content mismatch:\ngot:\n%s\nwant:\n%s", gotContent, tc.wantContent)
				}
			}
		})
	}

	t.Run("Directory Creation Failure", func(t *testing.T) {
		tmpDir := t.TempDir()
		conflictingFilePath := filepath.Join(tmpDir, "nested_dir")
		if err := os.WriteFile(conflictingFilePath, []byte("i am a file"), 0644); err != nil {
			t.Fatalf("Failed to create conflicting file: %v", err)
		}
		filePath := filepath.Join(conflictingFilePath, "output.json")
		writer := JSONWriter{}
		err := writer.Write(records[:1], filePath)
		if err == nil {
			t.Fatalf("Write() did not return error when directory creation should fail")
		}
		if !strings.Contains(err.Error(), "create directory") {
			t.Errorf("Write() error message %q does not indicate directory creation failure", err.Error())
		}
	})

}

func TestJSONWriter_Close(t *testing.T) {
	writer := JSONWriter{}
	err := writer.Close()
	if err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Errorf("Close() second call returned unexpected error: %v", err)
	}
}
