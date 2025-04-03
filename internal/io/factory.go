
package io

import (
	"fmt"
	"strings"

	"etl-tool/internal/config"
	"etl-tool/internal/logging"
)

// NewInputReader creates and returns an appropriate InputReader based on the source configuration.
func NewInputReader(cfg config.SourceConfig, dbConnStr string) (InputReader, error) {
	sourceType := strings.ToLower(cfg.Type)
	logging.Logf(logging.Debug, "Creating input reader for type: %s", sourceType)

	switch sourceType {
	case config.SourceTypeJSON:
		return &JSONReader{}, nil
	case config.SourceTypeCSV:
		// Capture and return potential error from NewCSVReader
		reader, err := NewCSVReader(cfg.Delimiter, cfg.CommentChar)
		if err != nil {
			// Wrap the error for context
			return nil, fmt.Errorf("failed to create CSV reader: %w", err)
		}
		return reader, nil // Return the reader only if no error occurred
	case config.SourceTypeXLSX:
		// Assuming NewXLSXReader doesn't return errors currently,
		// but could be modified similarly if it did.
		return NewXLSXReader(cfg.SheetName, cfg.SheetIndex), nil
	case config.SourceTypeXML:
		// Assuming NewXMLReader doesn't return errors currently.
		return NewXMLReader(cfg.XMLRecordTag), nil
	case config.SourceTypeYAML: // Added YAML case
		return &YAMLReader{}, nil
	case config.SourceTypePostgres:
		if dbConnStr == "" {
			return nil, fmt.Errorf("database connection string (-db or DB_CREDENTIALS) is required for source type 'postgres'")
		}
		if cfg.Query == "" {
			return nil, fmt.Errorf("query is required in source config for type 'postgres'")
		}
		// Assuming NewPostgresReader doesn't return errors currently.
		return NewPostgresReader(dbConnStr, cfg.Query), nil
	default:
		return nil, fmt.Errorf("unsupported source type '%s'", cfg.Type)
	}
}

// NewOutputWriter creates and returns an appropriate OutputWriter based on the destination configuration.
func NewOutputWriter(cfg config.DestinationConfig, dbConnStr string) (OutputWriter, error) {
	destType := strings.ToLower(cfg.Type)
	logging.Logf(logging.Debug, "Creating output writer for type: %s", destType)

	switch destType {
	case config.DestinationTypePostgres:
		if dbConnStr == "" {
			return nil, fmt.Errorf("database connection string (-db or DB_CREDENTIALS) is required for destination type 'postgres'")
		}
		if cfg.TargetTable == "" {
			return nil, fmt.Errorf("target_table is required in destination config for type 'postgres'")
		}
		// Assuming NewPostgresWriter doesn't return errors currently.
		return NewPostgresWriter(dbConnStr, cfg.TargetTable, cfg.Loader), nil
	case config.DestinationTypeCSV:
		// Capture and return potential error from NewCSVWriter
		writer, err := NewCSVWriter(cfg.Delimiter)
		if err != nil {
			// Wrap the error for context
			return nil, fmt.Errorf("failed to create CSV writer: %w", err)
		}
		return writer, nil // Return the writer only if no error occurred
	case config.DestinationTypeXLSX:
		// Assuming NewXLSXWriter doesn't return errors currently.
		return NewXLSXWriter(cfg.SheetName), nil
	case config.DestinationTypeXML:
		// Assuming NewXMLWriter doesn't return errors currently.
		return NewXMLWriter(cfg.XMLRecordTag, cfg.XMLRootTag), nil
	case config.DestinationTypeJSON:
		return &JSONWriter{}, nil
	case config.DestinationTypeYAML: // Added YAML case
		return &YAMLWriter{}, nil
	default:
		return nil, fmt.Errorf("unsupported destination type '%s'", cfg.Type)
	}
}
