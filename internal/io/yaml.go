package io

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"etl-tool/internal/logging"
	"gopkg.in/yaml.v3"
)

// YAMLReader implements the InputReader interface for YAML files.
type YAMLReader struct{}

// Read loads data from a YAML file specified by filePath.
func (yr *YAMLReader) Read(filePath string) ([]map[string]interface{}, error) {
	logging.Logf(logging.Debug, "YAMLReader reading file: %s", filePath)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("YAMLReader failed to read file '%s': %w", filePath, err)
	}

	// No explicit empty check needed here anymore, let Unmarshal handle it.

	// Attempt 1: Unmarshal as a list (primary expectation)
	var records []map[string]interface{} // Starts as nil slice
	errList := yaml.Unmarshal(data, &records)
	logging.Logf(logging.Debug, "YAMLReader: Attempt 1 (List) Unmarshal Error: %v, Records after attempt: %#v (isNil: %t)", errList, records, records == nil)

	if errList == nil {
		// --- FIX for Empty File/Null Input ---
		// If Unmarshal succeeds but result is still nil (happens for empty input or literal "null"),
		// return an initialized empty slice for consistency.
		if records == nil {
			logging.Logf(logging.Debug, "YAMLReader: Unmarshal as list resulted in nil slice (likely empty/null input), returning initialized empty slice.")
			return []map[string]interface{}{}, nil
		}
		// --- End FIX ---
		// Otherwise, success as a non-nil list
		logging.Logf(logging.Debug, "YAMLReader successfully loaded %d records (list format) from %s", len(records), filePath)
		return records, nil
	}

	// Attempt 2: Unmarshal as a single map (fallback)
	var singleRecord map[string]interface{}
	errMap := yaml.Unmarshal(data, &singleRecord)
	logging.Logf(logging.Debug, "YAMLReader: Attempt 2 (Map) Unmarshal Error: %v", errMap)

	if errMap == nil {
		// Success as map
		logging.Logf(logging.Debug, "YAML input file '%s' contains a single map object, processing as one record.", filePath)
		// Check if the single map is nil (e.g. input was "{}")
		if singleRecord == nil {
			// Represent empty map as a single empty map in the slice
			return []map[string]interface{}{{}}, nil
		}
		return []map[string]interface{}{singleRecord}, nil
	}

	// --- FINAL REVISED ERROR RETURN ---
	// If both attempts failed, prioritize the error from the *first* attempt (list).
	logging.Logf(logging.Warning, "YAMLReader failed to unmarshal as list (%v) or map (%v) from '%s'. Returning list error.", errList, errMap, filePath)
	return nil, fmt.Errorf("YAMLReader failed to unmarshal YAML from '%s': %w", filePath, errList)
}

// YAMLWriter implements the OutputWriter interface for YAML files.
type YAMLWriter struct{}

// Write saves the provided records as a YAML list (sequence of maps) to the specified filePath.
func (yw *YAMLWriter) Write(records []map[string]interface{}, filePath string) error {
	recordCount := 0
	// --- Add explicit logging to see the exact input ---
	isNilInput := records == nil
	if !isNilInput {
		recordCount = len(records)
	}
	logging.Logf(logging.Debug, "YAMLWriter Write called. Input records isNil: %t, Len (if not nil): %d. Target: %s", isNilInput, recordCount, filePath)
	// --- End logging ---

	dir := filepath.Dir(filePath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("YAMLWriter failed to create directory for '%s': %w", filePath, err)
		}
	}

	var dataToWrite []byte
	var writeErr error // Error for the final WriteFile

	// --- FINAL REVISED: Explicit nil vs empty handling ---
	if isNilInput { // Use the variable checked above
		// Handle nil slice: Write literal "null\n"
		logging.Logf(logging.Debug, "YAMLWriter: Writing literal 'null\\n' for nil input.")
		dataToWrite = []byte("null\n")
		// Ensure no encoder logic runs for nil input
	} else {
		// Handle non-nil slices (including empty): Use encoder
		var buf bytes.Buffer
		encoder := yaml.NewEncoder(&buf)
		encoder.SetIndent(2)

		encodeErr := encoder.Encode(records) // Encode the actual slice ([] -> [], non-empty -> list)
		if encodeErr == nil {
			encodeErr = encoder.Close()
		}

		if encodeErr != nil {
			// If encoding fails, return that specific error
			return fmt.Errorf("YAMLWriter failed to marshal records to YAML: %w", encodeErr)
		}
		// Only assign from buffer if encoding succeeded
		dataToWrite = buf.Bytes()
		logging.Logf(logging.Debug, "YAMLWriter: Encoded non-nil slice (len %d). Output bytes: %d", len(records), len(dataToWrite))
	}
	// --- End FINAL REVISED ---

	// Write the prepared data (either "null\n" or encoded YAML)
	logging.Logf(logging.Debug, "YAMLWriter: Writing %d bytes to file %s", len(dataToWrite), filePath)
	writeErr = os.WriteFile(filePath, dataToWrite, 0644)
	if writeErr != nil {
		return fmt.Errorf("YAMLWriter failed to write file '%s': %w", filePath, writeErr)
	}

	logging.Logf(logging.Debug, "YAMLWriter successfully wrote data for %d records to %s", recordCount, filePath)
	return nil
}

// Close implements the OutputWriter interface. For YAMLWriter, this is a no-op
func (yw *YAMLWriter) Close() error {
	logging.Logf(logging.Debug, "YAMLWriter Close called (no-op).")
	return nil
}