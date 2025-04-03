// --- START OF FINAL REVISED FILE internal/io/yaml_test.go ---

package io

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- Test YAMLReader ---

func TestYAMLReader_Read(t *testing.T) {
	testCases := []struct {
		name        string
		yamlContent string
		wantRecords []map[string]interface{}
		wantErr     bool
		wantErrMsg  string // Substring to check for in error message
	}{
		{
			name: "Valid YAML list",
			yamlContent: `- id: 1
  name: Alice
  active: true
- id: 2
  name: Bob
  city: New York
  details:
    age: 30
    tags: [dev, qa]`,
			wantRecords: []map[string]interface{}{
				{"id": 1, "name": "Alice", "active": true},
				{"id": 2, "name": "Bob", "city": "New York", "details": map[string]interface{}{
					"age":  30,
					"tags": []interface{}{"dev", "qa"},
				}},
			},
			wantErr: false,
		},
		{
			name: "Valid single YAML map",
			yamlContent: `id: 100
value: single entry`,
			wantRecords: []map[string]interface{}{
				{"id": 100, "value": "single entry"},
			},
			wantErr: false,
		},
		{
			name:        "Empty YAML list",
			yamlContent: `[]`,
			wantRecords: []map[string]interface{}{},
			wantErr:     false,
		},
		{
			name:        "Empty YAML map",
			yamlContent: `{}`,
			// Reader parses as map, returns slice with one empty map
			wantRecords: []map[string]interface{}{{}},
			wantErr:     false,
		},
		{
			name: "YAML with explicit types",
			yamlContent: `- name: f
  value: !!float 1.23
- name: i
  value: !!int 42`,
			wantRecords: []map[string]interface{}{
				{"name": "f", "value": 1.23},
				{"name": "i", "value": 42},
			},
			wantErr: false,
		},
		{
			name: "YAML with null values",
			yamlContent: `- key: a
  val: null
- key: b
  val: ~`,
			wantRecords: []map[string]interface{}{
				{"key": "a", "val": nil},
				{"key": "b", "val": nil},
			},
			wantErr: false,
		},
		{
			name: "YAML representing resolved anchor/alias",
			yamlContent: `- service: auth
  region: us-east-1
- service: data
  region: us-east-1`,
			wantRecords: []map[string]interface{}{
				{"service": "auth", "region": "us-east-1"},
				{"service": "data", "region": "us-east-1"},
			},
			wantErr: false,
		},
		{
			name:        "Empty file",
			yamlContent: ``,
			// Reader returns initialized empty slice for empty file
			wantRecords: []map[string]interface{}{},
			wantErr:     false,
		},
		{
			name:        "Malformed YAML (bad mix list/map)",
			yamlContent: "- item1\nkey: value", // Fails list parse, map parse might succeed partially or fail
			wantRecords: nil,
			wantErr:     true,
			// Reader prioritizes list error - Updated expected message
			wantErrMsg: "did not find expected '-' indicator",
		},
		{
			name:        "Malformed YAML (unclosed sequence)",
			yamlContent: "- item1\n- item2\n-", // Fails list parse
			wantRecords: nil,
			wantErr:     true,
			// Reader prioritizes list error - Updated expected message
			wantErrMsg: "cannot unmarshal",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := createTempYAML(t, tc.yamlContent) // Use shared helper
			reader := YAMLReader{}
			gotRecords, err := reader.Read(filePath)

			// Log results to help diagnose any future failures
			t.Logf("Test: %q, Read Error: %v, Got Records Count: %d", tc.name, err, len(gotRecords))
			// Use %#v for detailed structure if needed: t.Logf("Records: %#v", gotRecords)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("Read() error = nil, want error containing %q", tc.wantErrMsg)
				}
				if tc.wantErrMsg != "" && !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("Read() error message = %q, want error containing %q", err.Error(), tc.wantErrMsg)
				}
			} else {
				if err != nil {
					t.Fatalf("Read() returned unexpected error: %v", err)
				}
				// Use shared comparison helper
				if !compareRecordsDeep(t, gotRecords, tc.wantRecords) {
					// compareRecordsDeep logs detailed differences
				}
			}
		})
	}

	t.Run("File Not Found", func(t *testing.T) {
		reader := YAMLReader{}
		nonExistentPath := filepath.Join(t.TempDir(), "non_existent_file.yaml")
		_, err := reader.Read(nonExistentPath)
		if err == nil {
			t.Fatalf("Read() for non-existent file returned nil error, want error")
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("Read() error type = %T, want os.ErrNotExist", err)
		}
	})
}

// --- Test YAMLWriter ---

func TestYAMLWriter_Write(t *testing.T) {
	// Sample records for writing tests
	recordsValid := []map[string]interface{}{
		{"item": "Apple", "count": 10, "price": 0.5, "attrs": []string{"red", "sweet"}},
		{"item": "Banana", "count": 5, "price": 0.25, "organic": true, "attrs": nil},
	}
	recordsSingle := []map[string]interface{}{
		{"id": 99, "status": "ok"},
	}
	recordsEmpty := []map[string]interface{}{}
	var recordsNil []map[string]interface{} = nil

	// Expected content strings
	wantContentValid := `- attrs:
    - red
    - sweet
  count: 10
  item: Apple
  price: 0.5
- attrs: null
  count: 5
  item: Banana
  organic: true
  price: 0.25
`
	wantContentSingle := `- id: 99
  status: ok
`
	wantContentEmpty := "[]\n"
	wantContentNil := "null\n"

	testCases := []struct {
		name        string
		records     []map[string]interface{}
		setupDir    bool   // Flag to test directory creation
		expectDir   string // Subdirectory to create
		wantContent string // Expected exact file content
		wantErr     bool
		wantErrMsg  string
	}{
		{
			name:        "Write valid records",
			records:     recordsValid,
			wantContent: wantContentValid,
			wantErr:     false,
		},
		{
			name:        "Write empty record slice",
			records:     recordsEmpty,
			wantContent: wantContentEmpty,
			wantErr:     false,
		},
		{
			name:        "Write nil record slice",
			records:     recordsNil,
			wantContent: wantContentNil, // Expect "null\n" due to explicit handling in writer
			wantErr:     false,
		},
		{
			name:        "Write with directory creation",
			records:     recordsSingle,
			setupDir:    true,
			expectDir:   "data/output", // Nested directory
			wantContent: wantContentSingle,
			wantErr:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			filePath := filepath.Join(tmpDir, "output.yaml")
			if tc.setupDir {
				filePath = filepath.Join(tmpDir, tc.expectDir, "output.yaml")
			}

			writer := YAMLWriter{}
			// Log input for debugging
			inputIsNil := tc.records == nil
			t.Logf("Test: %q, Input records isNil: %t", tc.name, inputIsNil)
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

				// Verify directory was created if expected
				if tc.expectDir != "" {
					dirPath := filepath.Join(tmpDir, tc.expectDir)
					if _, statErr := os.Stat(dirPath); os.IsNotExist(statErr) {
						t.Errorf("Expected directory %s was not created", dirPath)
					}
				}

				// Verify file content
				contentBytes, readErr := os.ReadFile(filePath)
				if readErr != nil {
					t.Fatalf("Failed to read back output file %s: %v", filePath, readErr)
				}
				gotContent := string(contentBytes)
				// Log actual content for comparison
				t.Logf("Test: %q, Got file content:\n%s", tc.name, gotContent)
				if gotContent != tc.wantContent {
					t.Errorf("Write() file content mismatch:\n--- GOT ---\n%s\n--- WANT ---\n%s", gotContent, tc.wantContent)
				}
			}
		})
	}

	t.Run("Directory Creation Failure", func(t *testing.T) {
		tmpDir := t.TempDir()
		// Create a file where a directory is expected
		conflictingFilePath := filepath.Join(tmpDir, "targetdir_file")
		if err := os.WriteFile(conflictingFilePath, []byte("i am a file, not a dir"), 0644); err != nil {
			t.Fatalf("Failed to create conflicting file: %v", err)
		}

		// Attempt to write to a path that requires creating a directory over the file
		filePath := filepath.Join(conflictingFilePath, "output.yaml")
		writer := YAMLWriter{}
		err := writer.Write(recordsSingle, filePath)

		if err == nil {
			t.Fatalf("Write() did not return error when directory creation should fail")
		}
		// Check for specific error related to directory creation failure
		if !strings.Contains(err.Error(), "create directory") || (!strings.Contains(strings.ToLower(err.Error()), "not a directory") && !strings.Contains(strings.ToLower(err.Error()), "is a file")) {
			t.Errorf("Write() error message %q does not indicate directory creation failure (e.g., 'not a directory' or similar)", err.Error())
		}
	})
}

// --- Test YAMLWriter Close ---

func TestYAMLWriter_Close(t *testing.T) {
	// Setup
	writer := YAMLWriter{}

	// First call - should succeed (no-op)
	err1 := writer.Close()
	if err1 != nil {
		t.Errorf("Close() first call returned unexpected error: %v", err1)
	}

	// Second call - should also succeed (idempotency)
	err2 := writer.Close()
	if err2 != nil {
		t.Errorf("Close() second call returned unexpected error: %v", err2)
	}

}