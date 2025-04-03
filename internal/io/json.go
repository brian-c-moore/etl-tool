package io

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"etl-tool/internal/logging"
)

// JSONReader implements the InputReader interface for JSON files.
type JSONReader struct{}

// Read loads data from a JSON file specified by filePath.
// The JSON file is expected to contain an array of objects, but will
// gracefully handle a single top-level object as well.
// Returns a slice of maps representing the records, or an error.
func (jr *JSONReader) Read(filePath string) ([]map[string]interface{}, error) {
	logging.Logf(logging.Debug, "JSONReader reading file: %s", filePath)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("JSONReader failed to read file '%s': %w", filePath, err)
	}

	var records []map[string]interface{}
	// Attempt to unmarshal the JSON data into the slice of maps (array expected).
	if err := json.Unmarshal(data, &records); err != nil {
		// If array unmarshal fails, check if it's potentially a single JSON object.
		var singleRecord map[string]interface{}
		if errSingle := json.Unmarshal(data, &singleRecord); errSingle == nil {
			logging.Logf(logging.Debug, "JSON input file '%s' contains a single JSON object, processing as one record.", filePath)
			return []map[string]interface{}{singleRecord}, nil // Return slice containing the single object
		}
		// If it's neither an array nor a single object, return the original array unmarshal error.
		// Enhance error message for clarity.
		return nil, fmt.Errorf("JSONReader failed to unmarshal JSON from '%s' as array or single object: %w", filePath, err)
	}

	logging.Logf(logging.Debug, "JSONReader successfully loaded %d records from %s", len(records), filePath)
	return records, nil
}

// JSONWriter implements the OutputWriter interface for JSON files.
// The Write operation is self-contained and does not require a separate Close call.
type JSONWriter struct{}

// Write saves the provided records as a JSON array to the specified filePath.
// It marshals the data with indentation for readability. Ensures the output directory exists.
// Returns an error if marshaling or file writing fails.
func (jw *JSONWriter) Write(records []map[string]interface{}, filePath string) error {
	logging.Logf(logging.Debug, "JSONWriter writing %d records to file: %s", len(records), filePath)

	// Ensure the output directory exists.
	dir := filepath.Dir(filePath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("JSONWriter failed to create directory for '%s': %w", filePath, err)
		}
	}

	// Marshal the slice of maps into a JSON byte array with indentation.
	// Handle the case of empty records slice specifically.
	var data []byte
	var err error
	if len(records) == 0 {
		logging.Logf(logging.Debug, "JSONWriter: No records provided, writing empty JSON array '[]' to %s", filePath)
		data = []byte("[]\n") // Write an empty JSON array explicitly. Add newline for consistency.
	} else {
		data, err = json.MarshalIndent(records, "", "  ") // Use two spaces for indentation.
		if err != nil {
			return fmt.Errorf("JSONWriter failed to marshal records to JSON: %w", err)
		}
		// Add a trailing newline for POSIX compatibility / easier diffing.
		data = append(data, '\n')
	}

	// Write the JSON data to the specified file.
	// os.WriteFile handles file creation, truncation, and closing internally.
	err = os.WriteFile(filePath, data, 0644) // Use standard file permissions.
	if err != nil {
		return fmt.Errorf("JSONWriter failed to write file '%s': %w", filePath, err)
	}

	logging.Logf(logging.Debug, "JSONWriter successfully wrote %d records to %s", len(records), filePath)
	return nil
}

// Close implements the OutputWriter interface. For JSONWriter, this is a no-op
// as os.WriteFile handles file closing internally within the Write method.
func (jw *JSONWriter) Close() error {
	logging.Logf(logging.Debug, "JSONWriter Close called (no-op).")
	return nil
}