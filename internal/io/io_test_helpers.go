package io

import (
	"os"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3" // Use yaml for error output consistency
)

// Helper to create a temporary file with specific content.
// nolint:unused // Used by sibling test files (csv_test.go, etc.)
func createTempFile(t *testing.T, content string, pattern string) string {
	t.Helper()
	tempFile, err := os.CreateTemp(t.TempDir(), pattern)
	if err != nil {
		t.Fatalf("Failed to create temp file (pattern: %s): %v", pattern, err)
	}
	filePath := tempFile.Name()
	_, err = tempFile.WriteString(content)
	if err != nil {
		// Attempt to close before failing, but don't check close error here
		_ = tempFile.Close()
		t.Fatalf("Failed to write to temp file %s: %v", filePath, err)
	}
	err = tempFile.Close()
	if err != nil {
		t.Fatalf("Failed to close temp file %s: %v", filePath, err)
	}
	return filePath
}

// Helper to create a temporary JSON file for testing.
// nolint:unused // Used by json_test.go
func createTempJSON(t *testing.T, content string) string {
	t.Helper()
	return createTempFile(t, content, "test_*.json")
}

// Helper to create a temporary YAML file for testing.
// nolint:unused // Used by yaml_test.go
func createTempYAML(t *testing.T, content string) string {
	t.Helper()
	return createTempFile(t, content, "test_*.yaml")
}

// Helper to compare slices of maps, useful for checking JSON/YAML/DB read results.
// Uses reflect.DeepEqual for value comparison. Order matters here.
// Uses YAML marshalling for more readable diffs on error.
// nolint:unused // Used by sibling test files (csv_test.go, etc.)
func compareRecordsDeep(t *testing.T, got, want []map[string]interface{}) bool {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		// Provide more detail on mismatch using YAML
		gotYAML, errGot := yaml.Marshal(got)
		if errGot != nil {
			t.Errorf("Failed to marshal 'got' records for comparison: %v", errGot)
			// Fallback to basic print if marshal fails
			t.Errorf("Record mismatch (order matters):\ngot:\n%#v\nwant:\n%#v", got, want)
			return false
		}
		wantYAML, errWant := yaml.Marshal(want)
		if errWant != nil {
			t.Errorf("Failed to marshal 'want' records for comparison: %v", errWant)
			// Fallback to basic print if marshal fails
			t.Errorf("Record mismatch (order matters):\ngot:\n%s\nwant:\n%#v", string(gotYAML), want)
			return false
		}

		t.Errorf("Record mismatch (order matters):\n--- GOT ---\n%s\n--- WANT ---\n%s", string(gotYAML), string(wantYAML))
		return false
	}
	return true
}
