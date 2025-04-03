package io

// InputReader defines the interface for reading data from various sources.
type InputReader interface {
	// Read extracts data from the source specified by the pathOrQuery argument.
	// For file-based readers, this is the file path (potentially expanded).
	// For database readers, this argument might be ignored if the query is pre-configured.
	// Returns a slice of maps, where each map represents a record, or an error.
	Read(pathOrQuery string) ([]map[string]interface{}, error)
}

// OutputWriter defines the interface for writing data to various destinations.
type OutputWriter interface {
	// Write sends the processed records to the destination specified by the pathOrTable argument.
	// For file-based writers, this is the output file path (potentially expanded).
	// For database writers, this argument might be ignored if the table is pre-configured.
	// Returns an error if writing fails.
	Write(records []map[string]interface{}, pathOrTable string) error

	// Close handles any necessary cleanup operations for the writer, such as
	// flushing buffers, closing file handles, or releasing network connections.
	// Implementations should be idempotent (safe to call multiple times).
	// Returns an error if closing fails (e.g., error during buffer flush).
	Close() error
}

// ErrorWriter defines the interface for writing records that failed during processing.
type ErrorWriter interface {
	// Write records the problematic input record (or partially transformed record)
	// along with the specific processing error encountered.
	Write(record map[string]interface{}, processError error) error

	// Close ensures any buffered error data is flushed and resources (like files) are released.
	// Implementations should be idempotent.
	Close() error
}