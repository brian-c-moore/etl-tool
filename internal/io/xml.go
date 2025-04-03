// --- START OF CORRECTED FILE internal/io/xml.go ---
package io

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"etl-tool/internal/config"
	"etl-tool/internal/logging"
)

// XMLReader implements the InputReader interface for XML files.
// It expects a relatively flat structure where repeating elements specified
// by recordTag contain simple key-value fields.
// It reads the character data within field tags, including nested tags' data flattened.
type XMLReader struct {
	recordTag string
}

// NewXMLReader creates a new XMLReader.
func NewXMLReader(recordTag string) *XMLReader {
	tag := recordTag
	if tag == "" {
		tag = config.DefaultXMLRecordTag // Use default from config constants
	}
	return &XMLReader{
		recordTag: tag,
	}
}

// Read loads data from an XML file using a streaming decoder.
// It parses elements matching recordTag into map[string]interface{} records.
func (xr *XMLReader) Read(filePath string) ([]map[string]interface{}, error) {
	logging.Logf(logging.Debug, "XMLReader reading file: %s (Record Tag: '%s')", filePath, xr.recordTag)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("XMLReader failed to open file '%s': %w", filePath, err)
	}
	defer file.Close()

	decoder := xml.NewDecoder(file)
	var records []map[string]interface{} // Keep nil until first record found
	var currentRecord map[string]interface{}
	var currentFieldElement *xml.StartElement // The field element (e.g., <name>, <details>)
	var elementDepth int = 0                 // Track depth to handle nested elements within fields correctly
	var elementValue strings.Builder

	firstTokenRead := false // Flag to check if we successfully read at least one token

	for {
		token, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				// If EOF occurs before reading ANY token, it's an empty file error.
				if !firstTokenRead {
					return nil, fmt.Errorf("XMLReader error decoding token in '%s': %w", filePath, io.EOF)
				}
				// Otherwise, EOF is the normal end condition after processing tokens.
				break
			}
			// Return other decoding errors directly
			return nil, fmt.Errorf("XMLReader error decoding token in '%s': %w", filePath, err)
		}
		firstTokenRead = true // Mark that we've read at least one token

		switch se := token.(type) {
		case xml.StartElement:
			if elementDepth == 0 && se.Name.Local == xr.recordTag { // Start of a NEW record
				currentRecord = make(map[string]interface{})
				currentFieldElement = nil // Reset field tracking
				elementValue.Reset()
				elementDepth++ // Enter record level
			} else if elementDepth == 1 && currentRecord != nil { // Start of a FIELD within the record
				currentFieldElement = &se // Store the field's start element
				elementValue.Reset()      // Prepare to capture its value
				elementDepth++            // Enter field level
			} else if elementDepth > 1 { // Nested element within a field
				elementDepth++ // Just track depth
			}
			// Ignore elements outside of a record tag or deeper nesting levels for now

		case xml.CharData:
			// Append character data ONLY if we are inside a field (depth > 1)
			if elementDepth > 1 && currentRecord != nil && currentFieldElement != nil {
				elementValue.Write(se)
			}

		case xml.EndElement:
			if elementDepth > 0 { // Only process end tags if we are inside root/record/field
				elementDepth-- // Decrement depth regardless of tag type

				// Check if this is the end of the RECORD element (depth goes from 1 to 0)
				if elementDepth == 0 && se.Name.Local == xr.recordTag {
					if currentRecord != nil {
						if records == nil { // Initialize slice on first actual record
							records = make([]map[string]interface{}, 0)
						}
						records = append(records, currentRecord) // Add completed record
					}
					currentRecord = nil // Reset state
					currentFieldElement = nil
					elementValue.Reset()
				} else if elementDepth == 1 && currentRecord != nil && currentFieldElement != nil && se.Name.Local == currentFieldElement.Name.Local {
					// End of a FIELD element (depth goes from 2 to 1)
					fieldName := currentFieldElement.Name.Local
					// Assign accumulated character data (trimmed) to the field in the current record
					value := strings.TrimSpace(elementValue.String())
					currentRecord[fieldName] = value
					// Reset field tracking for the next field within the same record
					currentFieldElement = nil
					elementValue.Reset()
				}
				// Ignore other end elements (like nested ones within fields or the root)
			}
		}
	}

	// If loop finished and records is still nil (e.g., <root/> or <root></root>), initialize it
	if records == nil {
		records = make([]map[string]interface{}, 0)
	}

	logging.Logf(logging.Info, "XMLReader successfully loaded %d records from %s", len(records), filePath)
	return records, nil
}

// --- XML Writer ---

// XMLWriter implements the OutputWriter interface for XML files.
// It generates a flat XML structure with a specified root element and
// repeating record elements containing simple key-value fields.
// It does not currently support writing XML attributes or nested structures.
type XMLWriter struct {
	recordTag string
	rootTag   string
}

// NewXMLWriter creates a new XMLWriter.
func NewXMLWriter(recordTag, rootTag string) *XMLWriter {
	recTag := recordTag
	rtTag := rootTag
	if recTag == "" {
		recTag = config.DefaultXMLRecordTag // Use default from config constants
	}
	if rtTag == "" {
		rtTag = config.DefaultXMLRootTag // Use default from config constants
	}
	return &XMLWriter{
		recordTag: recTag,
		rootTag:   rtTag,
	}
}

// Write saves the provided records as an XML structure to the specified file.
// Uses an encoder with indentation for readability.
func (xw *XMLWriter) Write(records []map[string]interface{}, filePath string) error {
	logging.Logf(logging.Debug, "XMLWriter writing %d records to file: %s (Root: <%s>, Record: <%s>)", len(records), filePath, xw.rootTag, xw.recordTag)

	// Ensure output directory exists
	dir := filepath.Dir(filePath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("XMLWriter failed to create directory for '%s': %w", filePath, err)
		}
	}

	// Create or truncate the output file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("XMLWriter failed to create file '%s': %w", filePath, err)
	}
	// Ensure file is closed reliably
	defer file.Close()

	// Write standard XML header
	if _, err = file.WriteString(xml.Header); err != nil {
		return fmt.Errorf("XMLWriter failed to write XML header to '%s': %w", filePath, err)
	}

	// Create an XML encoder with indentation
	encoder := xml.NewEncoder(file)
	encoder.Indent("", "  ") // Use two spaces for indentation

	// Encode the root element start tag
	rootStartElem := xml.StartElement{Name: xml.Name{Local: xw.rootTag}}
	if err := encoder.EncodeToken(rootStartElem); err != nil {
		return fmt.Errorf("XMLWriter failed to encode root start element <%s>: %w", xw.rootTag, err)
	}

	// Define the record start element (reused)
	recordStartElem := xml.StartElement{Name: xml.Name{Local: xw.recordTag}}

	// Iterate through records and encode each one
	// Ranging over a nil slice is safe and does nothing, so the nil check is removed.
	for i, rec := range records {
		// Encode record start tag
		if err := encoder.EncodeToken(recordStartElem); err != nil {
			return fmt.Errorf("XMLWriter failed to encode record start element <%s> for record %d: %w", xw.recordTag, i, err)
		}

		// Sort keys for consistent field order within each record
		keys := make([]string, 0, len(rec))
		for k := range rec {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Encode each key-value pair as a field element
		for _, key := range keys {
			value := rec[key]
			// Convert value to string; handle nil as empty string
			stringValue := ""
			if value != nil {
				stringValue = fmt.Sprintf("%v", value)
			}

			fieldElement := xml.StartElement{Name: xml.Name{Local: key}}
			// Encode field start tag
			if err := encoder.EncodeToken(fieldElement); err != nil {
				return fmt.Errorf("XMLWriter failed to encode field start element <%s> for record %d: %w", key, i, err)
			}
			// Encode field value (character data) - Encoder handles escaping
			if err := encoder.EncodeToken(xml.CharData(stringValue)); err != nil {
				return fmt.Errorf("XMLWriter failed to encode field value for <%s> for record %d: %w", key, i, err)
			}
			// Encode field end tag
			if err := encoder.EncodeToken(fieldElement.End()); err != nil {
				return fmt.Errorf("XMLWriter failed to encode field end element </%s> for record %d: %w", key, i, err)
			}
		}

		// Encode record end tag
		if err := encoder.EncodeToken(recordStartElem.End()); err != nil {
			return fmt.Errorf("XMLWriter failed to encode record end element </%s> for record %d: %w", xw.recordTag, i, err)
		}
	} // End of for range loop

	// Encode the root element end tag
	if err := encoder.EncodeToken(rootStartElem.End()); err != nil {
		return fmt.Errorf("XMLWriter failed to encode root end element </%s>: %w", xw.rootTag, err)
	}

	// Flush the encoder buffer to the file
	if err := encoder.Flush(); err != nil {
		return fmt.Errorf("XMLWriter failed to flush encoder for file '%s': %w", filePath, err)
	}

	// Add a final newline for POSIX compatibility / aesthetics
	if _, err = file.WriteString("\n"); err != nil {
		// Non-fatal warning if writing newline fails
		logging.Logf(logging.Warning, "XMLWriter failed to write final newline to '%s': %v", filePath, err)
	}

	logging.Logf(logging.Info, "XMLWriter successfully wrote %d records to %s", len(records), filePath)
	return nil
}

// Close implements the OutputWriter interface. For XMLWriter, this is a no-op
// as the file handle is managed within the Write method using defer file.Close().
func (xw *XMLWriter) Close() error {
	logging.Logf(logging.Debug, "XMLWriter Close called (no-op).")
	return nil
}
