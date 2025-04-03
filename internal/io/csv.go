package io

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"etl-tool/internal/logging"
)

// CSVReader implements the InputReader interface for CSV files.
// It supports configurable delimiters and comment characters.
type CSVReader struct {
	Delimiter   rune // Field delimiter (e.g., ',', '\t').
	CommentChar rune // Character indicating a comment line (e.g., '#'). 0 disables.
}

// NewCSVReader creates a CSVReader with options derived from SourceConfig.
func NewCSVReader(delimiter, commentChar string) (*CSVReader, error) {
	var delim rune = ',' // Default delimiter
	var comment rune     // Default comment (0 / disabled)

	if delimiter != "" {
		if utf8.RuneCountInString(delimiter) != 1 {
			return nil, fmt.Errorf("invalid delimiter '%s': must be a single character", delimiter)
		}
		delim = []rune(delimiter)[0]
	}

	if commentChar != "" {
		if utf8.RuneCountInString(commentChar) != 1 {
			return nil, fmt.Errorf("invalid comment character '%s': must be a single character or empty", commentChar)
		}
		comment = []rune(commentChar)[0]
	}

	return &CSVReader{
		Delimiter:   delim,
		CommentChar: comment,
	}, nil
}

// Read loads data from a CSV file, applying configured options.
func (cr *CSVReader) Read(filePath string) ([]map[string]interface{}, error) {
	logging.Logf(logging.Debug, "CSVReader reading file: %s (Delimiter: '%c', Comment: '%c')", filePath, cr.Delimiter, cr.CommentChar)

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("CSVReader failed to open file '%s': %w", filePath, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = cr.Delimiter
	if cr.CommentChar != 0 {
		reader.Comment = cr.CommentChar
	}
	reader.FieldsPerRecord = -1 // Allow variable number of fields initially

	allRows, err := reader.ReadAll()
	if err != nil {
		if parseErr, ok := err.(*csv.ParseError); ok {
			return nil, fmt.Errorf("CSVReader parse error in '%s' on line %d, column %d: %w", filePath, parseErr.Line, parseErr.Column, parseErr.Err)
		}
		return nil, fmt.Errorf("CSVReader failed to read rows from '%s': %w", filePath, err)
	}

	// Ensure an empty, non-nil slice is returned if no header or no data rows exist
	if len(allRows) < 2 { // Changed condition to < 2 to handle header-only case
		if len(allRows) == 0 {
			logging.Logf(logging.Warning, "CSV file '%s' is empty or contains no data", filePath)
		} else { // len(allRows) == 1
			logging.Logf(logging.Warning, "CSV file '%s' contains only a header row", filePath)
		}
		return []map[string]interface{}{}, nil // Return initialized empty slice
	}

	headers := allRows[0]
	numHeaders := len(headers)
	headerSet := make(map[string]int) // Stores count of each header
	validHeaderIndices := make(map[int]string) // Map column index to valid header name

	for i, h := range headers {
		header := strings.TrimSpace(h)
		if header == "" {
			logging.Logf(logging.Warning, "CSVReader: Empty header found in column %d of file '%s'; this column will be skipped", i+1, filePath)
			continue // Skip empty headers
		}
		headerSet[header]++
		if headerSet[header] > 1 {
			logging.Logf(logging.Warning, "CSVReader: Duplicate header '%s' found at column %d in file '%s'; data for this header name will represent the last occurring column", header, i+1, filePath)
		}
		validHeaderIndices[i] = header // Store mapping from original index to valid header
	}

	if len(validHeaderIndices) == 0 {
		logging.Logf(logging.Warning, "CSVReader: No valid headers found in file '%s'; returning empty dataset", filePath)
		return []map[string]interface{}{}, nil // Return initialized empty slice
	}

	records := make([]map[string]interface{}, 0, len(allRows)-1)
	for i, row := range allRows[1:] {
		rowNum := i + 2 // 1-based row number in the file (including header)
		// Check column count against the original number of headers read
		if len(row) != numHeaders {
			logging.Logf(logging.Warning, "CSVReader: Row %d in '%s' has %d fields, expected %d based on header count; skipping row. Data: %v", rowNum, filePath, len(row), numHeaders, row)
			continue
		}

		rec := make(map[string]interface{})
		for colIdx, value := range row {
			// Use only columns that had a valid header
			if headerName, ok := validHeaderIndices[colIdx]; ok {
				rec[headerName] = value // Assign value using the valid header name
			}
		}
		// Ensure all valid headers (from headerSet keys) are present, even if row was short
		// Note: Skipping rows with incorrect field count makes this less critical, but good practice
		for header := range headerSet {
			if _, exists := rec[header]; !exists && header != "" { // Ensure key exists, skip adding empty header key
				rec[header] = ""
			}
		}
		records = append(records, rec)
	}

	logging.Logf(logging.Debug, "CSVReader successfully loaded %d records from %s", len(records), filePath)
	return records, nil
}

// CSVWriter implements the OutputWriter interface for CSV files.
// It buffers writes and requires Close() to be called to finalize the file.
type CSVWriter struct {
	Delimiter     rune // Field delimiter to use for writing.
	filePath      string
	mu            sync.Mutex
	file          *os.File
	writer        *csv.Writer
	headers       []string // Store headers determined after first write batch
	headerWritten bool
}

// NewCSVWriter creates a CSVWriter, deferring file opening until the first Write call.
func NewCSVWriter(delimiter string) (*CSVWriter, error) {
	var delim rune = ','
	if delimiter != "" {
		if utf8.RuneCountInString(delimiter) != 1 {
			return nil, fmt.Errorf("invalid delimiter '%s': must be a single character", delimiter)
		}
		delim = []rune(delimiter)[0]
	}
	return &CSVWriter{
		Delimiter: delim,
		// File path, file handle, writer, headers, headerWritten are initialized in Write
	}, nil
}

// Write saves the provided records to the CSV file.
// The file is opened on the first call to Write. Headers are determined from *all* records
// in the first batch and written once. Subsequent calls use the initially determined headers.
// The file is created even if the first batch is empty.
// Data is buffered; call Close() to ensure all data is written and the file is closed.
func (cw *CSVWriter) Write(records []map[string]interface{}, filePath string) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	// Initialize file and writer on first write call only
	if cw.writer == nil {
		logging.Logf(logging.Debug, "CSVWriter initializing for first write to file: %s (Delimiter: '%c')", filePath, cw.Delimiter)
		cw.filePath = filePath // Store file path for subsequent calls and error messages

		// Ensure directory exists
		dir := filepath.Dir(filePath)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("CSVWriter failed to create directory for '%s': %w", filePath, err)
			}
		}

		// Create or truncate the file (even if records slice is empty on first call)
		f, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("CSVWriter failed to create file '%s': %w", filePath, err)
		}
		cw.file = f
		cw.writer = csv.NewWriter(f)
		cw.writer.Comma = cw.Delimiter
		cw.headerWritten = false // Header not written yet

		// If the first call has no records, the file is created empty, and we return.
		// The header will be determined and written on the *next* non-empty Write call.
		if len(records) == 0 {
			logging.Logf(logging.Debug, "CSVWriter: First Write call has 0 records. Created empty file '%s'", filePath)
			return nil
		}
	} else if cw.filePath != filePath {
		// Handle case where Write is called again with a different file path (error)
		// This prevents accidentally writing subsequent batches to a different file
		// without closing the first one.
		// Remove trailing punctuation from error message per ST1005.
		return fmt.Errorf("CSVWriter already initialized for file '%s', cannot write to different file '%s'; close the writer first", cw.filePath, filePath)
	}

	// Handle case where subsequent write calls have no records
	if len(records) == 0 {
		// If writer is already initialized, we just do nothing for this call.
		logging.Logf(logging.Debug, "CSVWriter: Write called with 0 records; no data written in this call")
		return nil
	}

	// Determine and write headers if not already done (during the first non-empty write)
	if !cw.headerWritten {
		// Determine headers by collecting all unique keys from the *current batch*
		headerSet := make(map[string]struct{})
		for _, rec := range records {
			for k := range rec {
				headerSet[k] = struct{}{}
			}
		}
		// Convert set to slice and sort for consistent order
		cw.headers = make([]string, 0, len(headerSet))
		for k := range headerSet {
			cw.headers = append(cw.headers, k)
		}
		sort.Strings(cw.headers)

		logging.Logf(logging.Debug, "CSVWriter determined headers from first batch: %v", cw.headers)
		if err := cw.writer.Write(cw.headers); err != nil {
			// Close the file handle on header write error to prevent leaving it open
			cw.cleanupResources() // Use helper to close file handle
			return fmt.Errorf("CSVWriter failed to write header to '%s': %w", cw.filePath, err)
		}
		// Check for immediate error after writing header
		if err := cw.writer.Error(); err != nil {
			cw.cleanupResources() // Use helper to close file handle
			return fmt.Errorf("CSVWriter error after writing header to '%s': %w", cw.filePath, err)
		}
		cw.headerWritten = true
	}

	// Write data rows using the established headers
	for i, rec := range records {
		row := make([]string, len(cw.headers))
		for j, header := range cw.headers {
			// Lookup value based on established header order
			if val, ok := rec[header]; ok && val != nil {
				row[j] = fmt.Sprintf("%v", val) // Use fmt.Sprintf for consistent string conversion
			} else {
				row[j] = "" // Empty string for nil or missing values
			}
		}
		if err := cw.writer.Write(row); err != nil {
			// Error might be recoverable, but report and stop for this batch
			// Do not close the file handle here, allow Close() to handle it
			return fmt.Errorf("CSVWriter failed to write data row %d to '%s': %w", i+1, cw.filePath, err)
		}
		// Check for potential asynchronous errors after each write
		if err := cw.writer.Error(); err != nil {
			// Do not close the file handle here
			return fmt.Errorf("CSVWriter error after writing data row %d to '%s': %w", i+1, cw.filePath, err)
		}
	}

	logging.Logf(logging.Debug, "CSVWriter successfully wrote %d records to buffer for %s", len(records), cw.filePath)
	// Data is buffered, actual write to disk happens on Flush (called in Close)
	return nil
}

// cleanupResources closes the file handle if it's open. Used internally on error.
func (cw *CSVWriter) cleanupResources() {
	if cw.file != nil {
		cw.file.Close()
		cw.file = nil
	}
	// Reset writer state as well
	cw.writer = nil
	cw.headerWritten = false
	cw.headers = nil
	// Keep filePath for potential error messages in Close()
}

// Close flushes any buffered data to the underlying file and closes the file resource.
// It should be called once after all Write calls are complete. It is safe to call multiple times.
func (cw *CSVWriter) Close() error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	// Check if writer was ever initialized and file opened
	if cw.writer == nil || cw.file == nil {
		// If filePath is set, it means Write was called but maybe with 0 records initially.
		// If not, it means Write was never called or failed very early.
		if cw.filePath != "" {
			logging.Logf(logging.Debug, "CSVWriter Close called, but writer/file not initialized (likely no non-empty Write calls for '%s')", cw.filePath)
		} else {
			logging.Logf(logging.Debug, "CSVWriter Close called, but writer/file never initialized (no Write calls?)")
		}
		return nil // Nothing to close
	}

	var firstErr error
	logging.Logf(logging.Debug, "CSVWriter closing file: %s", cw.filePath)

	// Flush the csv.Writer buffer
	cw.writer.Flush()
	errFlush := cw.writer.Error() // Check for errors during flush
	if errFlush != nil {
		firstErr = fmt.Errorf("CSVWriter flush error on close for '%s': %w", cw.filePath, errFlush)
		logging.Logf(logging.Error, "%v", firstErr) // Log the flush error
	}

	// Close the underlying file handle
	errClose := cw.file.Close()
	if errClose != nil {
		closeErr := fmt.Errorf("CSVWriter file close error for '%s': %w", cw.filePath, errClose)
		logging.Logf(logging.Error, "%v", closeErr) // Log the close error
		if firstErr == nil {
			firstErr = closeErr // Capture the close error only if flush was successful
		}
	}

	// Mark resources as closed regardless of errors during close
	cw.file = nil
	cw.writer = nil
	cw.headerWritten = false
	cw.headers = nil

	if firstErr == nil {
		logging.Logf(logging.Debug, "CSVWriter closed successfully: %s", cw.filePath)
	}
	return firstErr // Return the first error encountered during close
}

// --- Error Writer ---

// CSVErrorWriter implements the ErrorWriter interface, writing errors to a CSV file.
type CSVErrorWriter struct {
	filePath string
	writer   *csv.Writer
	file     *os.File
	headers  []string
	mu       sync.Mutex
	headerWritten bool
	closed   bool // Flag to track if Close has been called
}

// NewCSVErrorWriter creates a writer for logging record processing errors.
// The file is opened in append mode.
func NewCSVErrorWriter(filePath string) (*CSVErrorWriter, error) {
	dir := filepath.Dir(filePath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("CSVErrorWriter failed to create directory for '%s': %w", filePath, err)
		}
	}
	// Open file in append mode
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("CSVErrorWriter failed to open/create file '%s': %w", filePath, err)
	}

	writer := csv.NewWriter(f)
	// Use standard comma delimiter for error files
	writer.Comma = ','

	return &CSVErrorWriter{
		filePath:      filePath,
		file:          f,
		writer:        writer,
		headers:       nil, // Headers determined on first write
		headerWritten: false,
		closed:        false, // Initially not closed
	}, nil
}

// Write appends a record and its associated error to the CSV error file.
// Headers are written only if the file is empty or doesn't exist yet.
// Returns an error if called after Close() or if writing fails.
func (cew *CSVErrorWriter) Write(record map[string]interface{}, processError error) error {
	cew.mu.Lock()
	defer cew.mu.Unlock()

	// Check if the writer has been closed
	if cew.closed {
		// Return a specific error when trying to write to a closed writer.
		return errors.New("CSVErrorWriter: write called on closed writer")
	}
	// Double check if resources are nil just in case (although `closed` should cover it)
	if cew.writer == nil || cew.file == nil {
		// This state indicates an issue, possibly closed without setting the flag, or failed init.
		return errors.New("CSVErrorWriter: writer or file handle is nil (unexpected state)")
	}


	// Check if we need to determine and potentially write headers
	if !cew.headerWritten {
		// Stat the file *inside the lock* to get accurate size check
		fileInfo, err := cew.file.Stat()
		// Write header if file is new or empty
		writeHeader := err != nil || fileInfo.Size() == 0

		// Determine headers from this record + error column
		headers := make([]string, 0, len(record)+1)
		for k := range record {
			headers = append(headers, k)
		}
		sort.Strings(headers) // Consistent order for record fields
		headers = append(headers, "etl_error_message") // Add error column header
		cew.headers = headers

		if writeHeader {
			logging.Logf(logging.Debug, "Writing header to error file '%s': %v", cew.filePath, cew.headers)
			if err := cew.writer.Write(cew.headers); err != nil {
				// This is potentially serious, as subsequent rows might be misaligned
				return fmt.Errorf("CSVErrorWriter failed to write header to '%s': %w", cew.filePath, err)
			}
			// Flush immediately after writing header to ensure it's written
			cew.writer.Flush()
			if err := cew.writer.Error(); err != nil {
				return fmt.Errorf("CSVErrorWriter error after flushing header to '%s': %w", cew.filePath, err)
			}
		}
		cew.headerWritten = true // Mark header as handled (written or confirmed existing)
	}

	// Ensure headers are determined before writing row
	if cew.headers == nil {
		// This should not happen if logic above is correct, but safeguard
		return fmt.Errorf("CSVErrorWriter internal error: headers not determined before writing row")
	}

	// Create the row data based on determined headers
	row := make([]string, len(cew.headers))
	for i, header := range cew.headers {
		if header == "etl_error_message" {
			if processError != nil {
				row[i] = processError.Error()
			} else {
				row[i] = "" // Should not happen, but handle nil error case
			}
		} else {
			// Lookup value based on established header order
			if val, ok := record[header]; ok && val != nil {
				row[i] = fmt.Sprintf("%v", val) // Consistent string conversion
			} else {
				row[i] = "" // Empty string for nil or missing original field
			}
		}
	}

	// Write the error row
	if err := cew.writer.Write(row); err != nil {
		return fmt.Errorf("CSVErrorWriter failed to write error row to '%s': %w", cew.filePath, err)
	}
	// Flush after each write to ensure errors are persisted quickly
	cew.writer.Flush()
	if err := cew.writer.Error(); err != nil {
		return fmt.Errorf("CSVErrorWriter error after flushing error row to '%s': %w", cew.filePath, err)
	}

	return nil
}

// Close flushes any buffered error data and closes the underlying file.
// Marks the writer as closed to prevent subsequent writes. Safe to call multiple times.
func (cew *CSVErrorWriter) Close() error {
	cew.mu.Lock()
	defer cew.mu.Unlock()

	// Check if already closed or never properly initialized
	if cew.closed || cew.writer == nil || cew.file == nil {
		logging.Logf(logging.Debug, "CSVErrorWriter Close called, but writer already closed or not initialized")
		return nil // Already closed or nothing to close
	}

	var firstErr error
	logging.Logf(logging.Debug, "CSVErrorWriter closing file: %s", cew.filePath)

	// Flush the writer buffer - this might error if the file was closed prematurely
	cew.writer.Flush()
	errFlush := cew.writer.Error()
	if errFlush != nil {
		// Check if it's specifically a "bad file descriptor" or similar OS error
		// due to the file handle being closed.
		firstErr = fmt.Errorf("CSVErrorWriter flush error on close for '%s': %w", cew.filePath, errFlush)
		logging.Logf(logging.Error, "%v", firstErr) // Log flush error
	}

	// Close the file handle (safe to call close on an already closed file, returns error)
	errClose := cew.file.Close()
	if errClose != nil {
		// Log the close error, but prioritize the flush error if it happened
		closeErr := fmt.Errorf("CSVErrorWriter file close error for '%s': %w", cew.filePath, errClose)
		logging.Logf(logging.Error, "%v", closeErr)
		if firstErr == nil {
			// If Flush() succeeded but Close() failed (e.g., file handle was already closed by the test),
			// this is the error we want to return.
			firstErr = closeErr
		}
	}

	// Mark as closed and release resources
	cew.closed = true
	cew.file = nil
	cew.writer = nil
	cew.headerWritten = false // Reset state
	cew.headers = nil

	if firstErr == nil {
		logging.Logf(logging.Debug, "CSVErrorWriter closed successfully: %s", cew.filePath)
	}
	return firstErr // Return the first error encountered
}