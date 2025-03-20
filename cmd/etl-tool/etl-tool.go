package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
	"gopkg.in/yaml.v3"
)

///////////////////////////////////////////////////////////////////////////////
// Utility Functions
///////////////////////////////////////////////////////////////////////////////

// expandEnvUniversal expands environment variables in both Unix ($VAR) and Windows (%VAR%) formats.
func expandEnvUniversal(s string) string {
	result := os.ExpandEnv(s)
	re := regexp.MustCompile(`%([A-Za-z0-9_]+)%`)
	result = re.ReplaceAllStringFunc(result, func(match string) string {
		varName := match[1 : len(match)-1]
		if value, ok := os.LookupEnv(varName); ok {
			return value
		}
		return ""
	})
	return result
}

///////////////////////////////////////////////////////////////////////////////
// Configuration Types
///////////////////////////////////////////////////////////////////////////////

/*
	Configuration Types:
	- ETLConfig: Defines the overall configuration for the ETL process.
	- SourceConfig: Specifies input source details (file path, query, etc.).
	- DestinationConfig: Specifies output destination details (target table, file, loader options, etc.).
	- MappingRule: Defines a transformation rule mapping a source field to a target field.
	- DedupConfig: Provides deduplication settings based on specific keys.
	- LoaderConfig: Configuration for custom SQL-based loading, including preload and postload commands.
*/
type ETLConfig struct {
	Source      SourceConfig      `yaml:"source"`
	Destination DestinationConfig `yaml:"destination"`
	Mappings    []MappingRule     `yaml:"mappings"`
	Dedup       *DedupConfig      `yaml:"dedup,omitempty"`
}

type SourceConfig struct {
	Type  string `yaml:"type"`           // Supported types: "json", "csv", "xlsx", "xml", "postgres"
	File  string `yaml:"file"`           // For file-based sources
	Query string `yaml:"query,omitempty"` // For PostgreSQL input source
}

type DestinationConfig struct {
	Type        string        `yaml:"type"`                  // Supported types: "postgres", "csv", "xlsx", "xml", "json"
	TargetTable string        `yaml:"target_table,omitempty"`// For PostgreSQL destination
	File        string        `yaml:"file,omitempty"`        // For file-based outputs
	Loader      *LoaderConfig `yaml:"loader,omitempty"`      // Loader configuration for PostgreSQL
}

type MappingRule struct {
	Source    string                 `yaml:"source"`              // Source field name
	Target    string                 `yaml:"target"`              // Target field name
	Transform string                 `yaml:"transform,omitempty"` // Transformation function to apply
	Params    map[string]interface{} `yaml:"params,omitempty"`    // Parameters for the transformation
}

type DedupConfig struct {
	Keys []string `yaml:"keys"` // Fields used to create a composite key for deduplication
}

type LoaderConfig struct {
	Mode      string   `yaml:"mode"`                // Loader mode, e.g., "sql"
	Command   string   `yaml:"command,omitempty"`   // Custom SQL command for each record
	Preload   []string `yaml:"preload,omitempty"`   // Commands to execute before loading records
	Postload  []string `yaml:"postload,omitempty"`  // Commands to execute after loading records
	BatchSize int      `yaml:"batch_size,omitempty"`// Batch size for loading records in batches
}

///////////////////////////////////////////////////////////////////////////////
// Transformation Functions
///////////////////////////////////////////////////////////////////////////////

/*
	Transformation Functions:
	Each function receives a value, a record, and a parameter map. They perform
	appropriate transformations (e.g., converting epoch to date, case conversion,
	and regex extraction) and return the transformed value.
*/
type TransformFunc func(value interface{}, record map[string]interface{}, params map[string]interface{}) interface{}

var transformRegistry = map[string]TransformFunc{
	"epochToDate":    epochToDate,
	"calculateAge":   calculateAge,
	"regexExtract":   regexExtract,
	"trim":           trim,
	"toUpperCase":    toUpperCase,
	"toLowerCase":    toLowerCase,
	"branch":         branchTransform,
	"dateConvert":    dateConvert,
	"toInt":          toInt,
	"toFloat":        toFloat,
	"toBool":         toBool,
	"toString":       toString,
	"replaceAll":     replaceAll,
	"substring":      substring,
	"coalesce":       coalesceTransform,
	"multiDateConvert": multiDateConvert,
}

func epochToDate(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("epochToDate called with value=%v", value), "debug")
	switch v := value.(type) {
	case float64:
		t := time.Unix(int64(v), 0)
		return t.Format("2006-01-02")
	case int64:
		t := time.Unix(v, 0)
		return t.Format("2006-01-02")
	case string:
		if epoch, err := strconv.ParseInt(v, 10, 64); err == nil {
			t := time.Unix(epoch, 0)
			return t.Format("2006-01-02")
		}
	}
	return value
}

func calculateAge(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("calculateAge called with value=%v", value), "debug")
	var epoch int64
	switch v := value.(type) {
	case float64:
		epoch = int64(v)
	case int64:
		epoch = v
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			epoch = parsed
		} else {
			return nil
		}
	default:
		return nil
	}
	firstObserved := time.Unix(epoch, 0)
	now := time.Now()
	if now.Before(firstObserved) {
		return 0
	}
	return int(now.Sub(firstObserved).Hours() / 24)
}

func regexExtract(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("regexExtract called with value=%v, params=%+v", value, params), "debug")
	var s string
	switch v := value.(type) {
	case string:
		s = v
	default:
		return nil
	}
	var pattern string
	if p, ok := params["pattern"]; ok {
		pattern = fmt.Sprintf("%v", p)
	} else {
		transformStr, ok := params["transformStr"].(string)
		if ok {
			parts := strings.SplitN(transformStr, ":", 2)
			if len(parts) == 2 {
				pattern = parts[1]
			}
		}
	}
	if pattern == "" {
		return nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		log.Printf("Invalid regex pattern '%s': %v", pattern, err)
		return nil
	}
	matches := re.FindStringSubmatch(s)
	if len(matches) >= 2 {
		return matches[1]
	}
	return nil
}

func trim(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("trim called with value=%v", value), "debug")
	if s, ok := value.(string); ok {
		return strings.TrimSpace(s)
	}
	return value
}

func toUpperCase(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("toUpperCase called with value=%v", value), "debug")
	if s, ok := value.(string); ok {
		return strings.ToUpper(s)
	}
	return value
}

func toLowerCase(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("toLowerCase called with value=%v", value), "debug")
	if s, ok := value.(string); ok {
		return strings.ToLower(s)
	}
	return value
}

func branchTransform(value interface{}, record map[string]interface{}, params map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("branchTransform start, value=%v, record=%+v, params=%+v", value, record, params), "debug")
	branchesIface, ok := params["branches"]
	if !ok {
		debugLog("branchTransform: no branches provided; returning original value", "debug")
		return value
	}
	branchesSlice, ok := branchesIface.([]interface{})
	if !ok {
		log.Println("Invalid branches parameter; expected an array")
		return value
	}
	exprParams := make(map[string]interface{})
	for k, v := range record {
		exprParams[k] = v
	}
	// Include the local 'value' in the expression parameters.
	exprParams["value"] = value

	for _, br := range branchesSlice {
		brMap, ok := br.(map[string]interface{})
		if !ok {
			continue
		}
		condRaw, condExists := brMap["condition"]
		if !condExists {
			continue
		}
		condition := fmt.Sprintf("%v", condRaw)
		condition = strings.TrimSpace(condition)

		expression, err := govaluate.NewEvaluableExpression(condition)
		if err != nil {
			log.Printf("branchTransform: failed to parse condition '%s': %v", condition, err)
			continue
		}
		result, err := expression.Evaluate(exprParams)
		if err != nil {
			log.Printf("branchTransform: failed to evaluate condition '%s': %v", condition, err)
			continue
		}
		boolResult, isBool := result.(bool)
		if isBool && boolResult {
			if branchVal, hasVal := brMap["value"]; hasVal {
				debugLog(fmt.Sprintf("branchTransform matched condition '%s'; returning branch value: %v", condition, branchVal), "debug")
				return branchVal
			}
		}
	}
	debugLog(fmt.Sprintf("branchTransform: no condition matched; returning original value: %v", value), "debug")
	return value
}

func dateConvert(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("dateConvert called with value=%v, params=%+v", value, params), "debug")
	inputFormat, _ := params["inputFormat"].(string)
	outputFormat, _ := params["outputFormat"].(string)
	strVal, ok := value.(string)
	if !ok || (inputFormat == "" && outputFormat == "") {
		return value
	}
	if inputFormat != "" && outputFormat != "" {
		t, err := time.Parse(inputFormat, strVal)
		if err != nil {
			log.Printf("dateConvert: parse error: %v", err)
			return value
		}
		return t.Format(outputFormat)
	}
	if outputFormat != "" && inputFormat == "" {
		t, err := time.Parse(time.RFC3339, strVal)
		if err != nil {
			log.Printf("dateConvert: parse error (RFC3339 fallback): %v", err)
			return value
		}
		return t.Format(outputFormat)
	}
	if inputFormat != "" && outputFormat == "" {
		t, err := time.Parse(inputFormat, strVal)
		if err != nil {
			log.Printf("dateConvert: parse error: %v", err)
			return value
		}
		return t.Format(time.RFC3339)
	}
	return value
}

func toInt(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("toInt called with value=%v", value), "debug")
	switch v := value.(type) {
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return i
		} else {
			log.Printf("Warning: toInt conversion failed for value '%v'; returning nil", v)
			return nil
		}
	}
	log.Printf("Warning: toInt conversion received unsupported type '%T'; returning nil", value)
	return nil
}

func toFloat(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("toFloat called with value=%v", value), "debug")
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return f
		} else {
			log.Printf("Warning: toFloat conversion failed for value '%v'; returning nil", v)
		}
	}
	log.Printf("Warning: toFloat conversion received unsupported type '%T'; returning nil", value)
	return nil
}

func toBool(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("toBool called with value=%v", value), "debug")
	switch v := value.(type) {
	case bool:
		return v
	case string:
		lower := strings.ToLower(strings.TrimSpace(v))
		if lower == "true" || lower == "1" || lower == "yes" {
			return true
		} else if lower == "false" || lower == "0" || lower == "no" {
			return false
		} else {
			log.Printf("Warning: toBool conversion unrecognized string value '%v'; returning nil", v)
			return nil
		}
	default:
		log.Printf("Warning: toBool conversion received unsupported type '%T'; returning nil", value)
		return nil
	}
}

func toString(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("toString called with value=%v", value), "debug")
	return fmt.Sprintf("%v", value)
}

func replaceAll(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("replaceAll called with value=%v, params=%+v", value, params), "debug")
	strVal, ok := value.(string)
	if !ok {
		return value
	}
	oldVal, _ := params["old"].(string)
	newVal, _ := params["new"].(string)
	if oldVal == "" {
		return strVal
	}
	return strings.ReplaceAll(strVal, oldVal, newVal)
}

func substring(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("substring called with value=%v, params=%+v", value, params), "debug")
	strVal, ok := value.(string)
	if !ok {
		return value
	}
	start, _ := parseParamAsInt(params["start"])
	length, _ := parseParamAsInt(params["length"])
	if start < 0 || start >= len(strVal) {
		return ""
	}
	if length <= 0 || start+length > len(strVal) {
		return strVal[start:]
	}
	return strVal[start : start+length]
}

func parseParamAsInt(v interface{}) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	case string:
		n, err := strconv.Atoi(val)
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func coalesceTransform(_ interface{}, record map[string]interface{}, params map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("coalesceTransform called, record=%+v, params=%+v", record, params), "debug")
	fieldsIface, ok := params["fields"].([]interface{})
	if !ok {
		return nil
	}
	for _, f := range fieldsIface {
		if keyStr, isStr := f.(string); isStr {
			if val, found := record[keyStr]; found {
				if val != nil && val != "" {
					return val
				}
			}
			// Return the key if it is not empty
			if keyStr != "" {
				return keyStr
			}
		} else {
			if f != nil && f != "" {
				return f
			}
		}
	}
	return nil
}

func multiDateConvert(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	debugLog(fmt.Sprintf("multiDateConvert called with value=%v, params=%+v", value, params), "debug")
	strVal, ok := value.(string)
	if !ok {
		return value
	}
	rawFormats, ok := params["formats"].([]interface{})
	if !ok || len(rawFormats) == 0 {
		return value
	}
	var formats []string
	for _, f := range rawFormats {
		if fs, isStr := f.(string); isStr {
			formats = append(formats, fs)
		}
	}
	outputFmt, _ := params["outputFormat"].(string)

	for _, f := range formats {
		t, err := time.Parse(f, strVal)
		if err == nil {
			if outputFmt != "" {
				return t.Format(outputFmt)
			}
			return t.Format(time.RFC3339)
		}
	}
	return strVal
}

///////////////////////////////////////////////////////////////////////////////
// Input Readers
///////////////////////////////////////////////////////////////////////////////

/*
	Input Readers:
	The InputReader interface defines a method for reading data from a specified source.
	The following implementations support JSON, CSV, XLSX, XML files, as well as PostgreSQL queries.
*/
type InputReader interface {
	Read(filePath string) ([]map[string]interface{}, error)
}

// JSONReader reads JSON files.
type JSONReader struct{}

// Read reads and unmarshals a JSON file into a slice of maps.
func (jr *JSONReader) Read(filePath string) ([]map[string]interface{}, error) {
	debugLog(fmt.Sprintf("JSONReader reading filePath=%s", filePath), "debug")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	debugLog(fmt.Sprintf("JSONReader loaded %d records", len(records)), "debug")
	return records, nil
}

// CSVReader reads CSV files.
type CSVReader struct{}

// Read reads a CSV file and maps each row to a record using the header row.
func (cr *CSVReader) Read(filePath string) ([]map[string]interface{}, error) {
	debugLog(fmt.Sprintf("CSVReader reading filePath=%s", filePath), "debug")
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	all, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(all) < 1 {
		return nil, fmt.Errorf("CSV file is empty")
	}
	headers := all[0]
	var records []map[string]interface{}
	for _, row := range all[1:] {
		rec := make(map[string]interface{})
		for i, value := range row {
			if i < len(headers) {
				rec[headers[i]] = value
			}
		}
		records = append(records, rec)
	}
	debugLog(fmt.Sprintf("CSVReader loaded %d records", len(records)), "debug")
	return records, nil
}

// XLSXReader reads Excel (XLSX) files.
type XLSXReader struct{}

// Read opens the first sheet of an XLSX file and reads rows into a slice of maps.
func (xr *XLSXReader) Read(filePath string) ([]map[string]interface{}, error) {
	debugLog(fmt.Sprintf("XLSXReader reading filePath=%s", filePath), "debug")
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sheetName := f.GetSheetName(0)
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, err
	}
	if len(rows) < 1 {
		return nil, fmt.Errorf("XLSX file is empty")
	}
	headers := rows[0]
	var records []map[string]interface{}
	for _, row := range rows[1:] {
		rec := make(map[string]interface{})
		for i, cell := range row {
			if i < len(headers) {
				rec[headers[i]] = cell
			}
		}
		records = append(records, rec)
	}
	debugLog(fmt.Sprintf("XLSXReader loaded %d records", len(records)), "debug")
	return records, nil
}

// XMLReader reads XML files.
type XMLReader struct{}

// XMLField represents a field in an XML record.
type XMLField struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

// XMLRecord represents a record in XML.
type XMLRecord struct {
	Fields []XMLField `xml:",any"`
}

// XMLRecords represents the root element containing XML records.
type XMLRecords struct {
	Records []XMLRecord `xml:"record"`
}

// Read unmarshals an XML file into a slice of maps.
func (xr *XMLReader) Read(filePath string) ([]map[string]interface{}, error) {
	debugLog(fmt.Sprintf("XMLReader reading filePath=%s", filePath), "debug")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var xmlRecs XMLRecords
	if err := xml.Unmarshal(data, &xmlRecs); err != nil {
		return nil, err
	}
	var records []map[string]interface{}
	for _, rec := range xmlRecs.Records {
		m := make(map[string]interface{})
		for _, field := range rec.Fields {
			m[field.XMLName.Local] = field.Value
		}
		records = append(records, m)
	}
	debugLog(fmt.Sprintf("XMLReader loaded %d records", len(records)), "debug")
	return records, nil
}

// PostgresReader reads data from a PostgreSQL database using a provided query.
type PostgresReader struct {
	connStr string // Connection string is supplied via CLI or environment.
	query   string // SQL query to execute.
}

// Read executes the configured query and returns the result as a slice of maps.
func (pr *PostgresReader) Read(_ string) ([]map[string]interface{}, error) {
	debugLog(fmt.Sprintf("PostgresReader reading query=%s", pr.query), "debug")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, expandEnvUniversal(pr.connStr))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, pr.query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query '%s': %w", pr.query, err)
	}
	defer rows.Close()

	var records []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		fieldDescs := rows.FieldDescriptions()
		rowMap := make(map[string]interface{}, len(values))
		for i, fd := range fieldDescs {
			colName := string(fd.Name)
			rowMap[colName] = values[i]
		}
		records = append(records, rowMap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	debugLog(fmt.Sprintf("PostgresReader loaded %d records from query", len(records)), "debug")
	return records, nil
}

// NewInputReader returns an appropriate InputReader implementation based on the source configuration.
func NewInputReader(cfg SourceConfig, globalDBConn string) InputReader {
	switch strings.ToLower(cfg.Type) {
	case "json":
		return &JSONReader{}
	case "csv":
		return &CSVReader{}
	case "xlsx":
		return &XLSXReader{}
	case "xml":
		return &XMLReader{}
	case "postgres":
		// Use the global database connection string.
		finalConnStr := globalDBConn
		return &PostgresReader{
			connStr: finalConnStr,
			query:   cfg.Query,
		}
	default:
		log.Printf("Unsupported source type '%s'; defaulting to JSON reader.", cfg.Type)
		return &JSONReader{}
	}
}

///////////////////////////////////////////////////////////////////////////////
// Output Writers
///////////////////////////////////////////////////////////////////////////////

/*
	Output Writers:
	The OutputWriter interface defines a method for writing a slice of records to a destination.
	The following implementations support PostgreSQL, CSV, XLSX, XML, and JSON outputs.
*/
type OutputWriter interface {
	Write(records []map[string]interface{}) error
}

// PostgresWriter writes records to a PostgreSQL table.
type PostgresWriter struct {
	connStr     string
	targetTable string
	loader      *LoaderConfig
}

// Write writes records to PostgreSQL using either the COPY method or custom SQL commands.
func (pw *PostgresWriter) Write(records []map[string]interface{}) error {
	debugLog(fmt.Sprintf("PostgresWriter Write called with %d records, targetTable=%s", len(records), pw.targetTable), "debug")
	if pw.loader != nil && strings.ToLower(pw.loader.Mode) == "sql" {
		return loadWithCustomSQL(pw.connStr, pw.targetTable, pw.loader, records)
	}
	return loadUsingCopy(pw.connStr, pw.targetTable, records)
}

// CSVWriter writes records to a CSV file.
type CSVWriter struct {
	filePath string
}

// Write writes the records to a CSV file.
func (cw *CSVWriter) Write(records []map[string]interface{}) error {
	debugLog(fmt.Sprintf("CSVWriter Write called with %d records, filePath=%s", len(records), cw.filePath), "debug")
	f, err := os.Create(cw.filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	defer writer.Flush()
	if len(records) == 0 {
		return nil
	}
	var headers []string
	for k := range records[0] {
		headers = append(headers, k)
	}
	if err := writer.Write(headers); err != nil {
		return err
	}
	for _, rec := range records {
		var row []string
		for _, header := range headers {
			row = append(row, fmt.Sprintf("%v", rec[header]))
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// XLSXWriter writes records to an Excel (XLSX) file.
type XLSXWriter struct {
	filePath string
}

// Write writes the records to an XLSX file.
func (xw *XLSXWriter) Write(records []map[string]interface{}) error {
	debugLog(fmt.Sprintf("XLSXWriter Write called with %d records, filePath=%s", len(records), xw.filePath), "debug")
	f := excelize.NewFile()
	sheetName := f.GetSheetName(0)
	if len(records) == 0 {
		if err := f.SaveAs(xw.filePath); err != nil {
			return err
		}
		return nil
	}
	var headers []string
	for k := range records[0] {
		headers = append(headers, k)
	}
	if err := f.SetSheetRow(sheetName, "A1", &headers); err != nil {
		return err
	}
	for i, rec := range records {
		var row []interface{}
		for _, header := range headers {
			row = append(row, rec[header])
		}
		cell, _ := excelize.CoordinatesToCellName(1, i+2)
		if err := f.SetSheetRow(sheetName, cell, &row); err != nil {
			return err
		}
	}
	if err := f.SaveAs(xw.filePath); err != nil {
		return err
	}
	return nil
}

// XMLWriter writes records to an XML file.
type XMLWriter struct {
	filePath string
}

// XMLFieldOutput represents a field in an XML output record.
type XMLFieldOutput struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

// XMLRecordOutput represents an XML record.
type XMLRecordOutput struct {
	XMLName xml.Name         `xml:"record"`
	Fields  []XMLFieldOutput `xml:",any"`
}

// XMLRecordsOutput represents the root XML element.
type XMLRecordsOutput struct {
	XMLName xml.Name          `xml:"records"`
	Records []XMLRecordOutput `xml:"record"`
}

// Write writes the records as XML to a file.
func (xw *XMLWriter) Write(records []map[string]interface{}) error {
	debugLog(fmt.Sprintf("XMLWriter Write called with %d records, filePath=%s", len(records), xw.filePath), "debug")
	var xmlRecords []XMLRecordOutput
	for _, rec := range records {
		var fields []XMLFieldOutput
		for k, v := range rec {
			fields = append(fields, XMLFieldOutput{
				XMLName: xml.Name{Local: k},
				Value:   fmt.Sprintf("%v", v),
			})
		}
		xmlRecords = append(xmlRecords, XMLRecordOutput{
			Fields: fields,
		})
	}
	output := XMLRecordsOutput{
		Records: xmlRecords,
	}
	data, err := xml.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(xw.filePath, data, 0644)
}

// JSONWriter writes records to a JSON file.
type JSONWriter struct {
	filePath string
}

// Write marshals the records to JSON and writes them to a file.
func (jw *JSONWriter) Write(records []map[string]interface{}) error {
	debugLog(fmt.Sprintf("JSONWriter Write called with %d records, filePath=%s", len(records), jw.filePath), "debug")
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(jw.filePath, data, 0644)
}

// NewOutputWriter returns an appropriate OutputWriter based on the destination configuration.
func NewOutputWriter(dest DestinationConfig, connStr string) OutputWriter {
	switch strings.ToLower(dest.Type) {
	case "postgres":
		return &PostgresWriter{
			connStr:     connStr,
			targetTable: dest.TargetTable,
			loader:      dest.Loader,
		}
	case "csv":
		return &CSVWriter{
			filePath: dest.File,
		}
	case "xlsx":
		return &XLSXWriter{
			filePath: dest.File,
		}
	case "xml":
		return &XMLWriter{
			filePath: dest.File,
		}
	case "json":
		return &JSONWriter{
			filePath: dest.File,
		}
	default:
		log.Printf("Unsupported destination type '%s'; defaulting to PostgreSQL writer.", dest.Type)
		return &PostgresWriter{
			connStr:     connStr,
			targetTable: dest.TargetTable,
			loader:      dest.Loader,
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
// Deduplication Utility
///////////////////////////////////////////////////////////////////////////////

// dedupRecords removes duplicate records based on a composite key derived from the specified keys.
func dedupRecords(records []map[string]interface{}, keys []string) []map[string]interface{} {
	debugLog(fmt.Sprintf("dedupRecords called with %d records, keys=%v", len(records), keys), "debug")
	seen := make(map[string]bool)
	var result []map[string]interface{}
	for _, rec := range records {
		var compositeKeyParts []string
		for _, key := range keys {
			if val, ok := rec[key]; ok {
				compositeKeyParts = append(compositeKeyParts, fmt.Sprintf("%v", val))
			} else {
				compositeKeyParts = append(compositeKeyParts, "")
			}
		}
		compositeKey := strings.Join(compositeKeyParts, "|")
		if !seen[compositeKey] {
			seen[compositeKey] = true
			result = append(result, rec)
		}
	}
	return result
}

///////////////////////////////////////////////////////////////////////////////
// PostgreSQL Loaders
///////////////////////////////////////////////////////////////////////////////

/*
	PostgreSQL Loaders:
	- loadUsingCopy: Loads records using the PostgreSQL COPY protocol.
	- loadWithCustomSQL: Loads records using custom SQL commands (supports batching).
*/

func loadUsingCopy(connStr string, table string, records []map[string]interface{}) error {
	debugLog(fmt.Sprintf("loadUsingCopy called, table=%s, recordCount=%d", table, len(records)), "debug")
	if len(records) == 0 {
		debugLog("No records to load; returning early.", "debug")
		return nil
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, expandEnvUniversal(connStr))
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)

	// Validate that the destination table contains the expected columns.
	schemaCols, err := getPostgresColumns(ctx, conn, table)
	if err != nil {
		return fmt.Errorf("failed to retrieve columns for table '%s': %w", table, err)
	}
	debugLog(fmt.Sprintf("Table '%s' has columns: %v", table, schemaCols), "debug")

	var columns []string
	for k := range records[0] {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	for _, col := range columns {
		if _, found := schemaCols[col]; !found {
			return fmt.Errorf("column '%s' not found in destination table '%s'", col, table)
		}
	}
	debugLog(fmt.Sprintf("Final columns to COPY: %v", columns), "debug")

	var rows [][]interface{}
	for _, rec := range records {
		row := make([]interface{}, len(columns))
		for i, col := range columns {
			row[i] = rec[col]
		}
		rows = append(rows, row)
	}

	copyCount, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{table},
		columns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy from failed: %w", err)
	}
	logMessage(fmt.Sprintf("Successfully inserted %d rows into %s.", copyCount, table), "info")
	return nil
}

func getPostgresColumns(ctx context.Context, conn *pgx.Conn, table string) (map[string]bool, error) {
	debugLog(fmt.Sprintf("Retrieving columns for table '%s'", table), "debug")
	q := `
SELECT column_name
FROM information_schema.columns
WHERE table_name = $1
ORDER BY ordinal_position
`
	rows, err := conn.Query(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colMap := make(map[string]bool)
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		colMap[colName] = true
	}
	return colMap, nil
}

func loadWithCustomSQL(connStr string, table string, loader *LoaderConfig, records []map[string]interface{}) error {
	debugLog(fmt.Sprintf("loadWithCustomSQL called, table=%s, recordCount=%d", table, len(records)), "debug")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, expandEnvUniversal(connStr))
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)

	for _, cmd := range loader.Preload {
		debugLog(fmt.Sprintf("Executing preload command: %s", cmd), "debug")
		if _, err := conn.Exec(ctx, cmd); err != nil {
			return fmt.Errorf("preload command failed: %w", err)
		}
	}
	batchSize := loader.BatchSize
	if batchSize <= 0 {
		for _, rec := range records {
			var params []interface{}
			for _, v := range rec {
				params = append(params, v)
			}
			if _, err := conn.Exec(ctx, loader.Command, params...); err != nil {
				return fmt.Errorf("failed to execute custom command: %w", err)
			}
		}
	} else {
		totalRecords := len(records)
		for i := 0; i < totalRecords; i += batchSize {
			end := i + batchSize
			if end > totalRecords {
				end = totalRecords
			}
			batch := &pgx.Batch{}
			batchRecords := records[i:end]

			for _, rec := range batchRecords {
				var params []interface{}
				for _, v := range rec {
					params = append(params, v)
				}
				batch.Queue(loader.Command, params...)
			}

			br := conn.SendBatch(ctx, batch)
			for range batchRecords {
				_, execErr := br.Exec()
				if execErr != nil {
					_ = br.Close()
					return fmt.Errorf("batch command failed: %w", execErr)
				}
			}
			if err := br.Close(); err != nil {
				return fmt.Errorf("batch close failed: %w", err)
			}
		}
	}
	for _, cmd := range loader.Postload {
		debugLog(fmt.Sprintf("Executing postload command: %s", cmd), "debug")
		if _, err := conn.Exec(ctx, cmd); err != nil {
			return fmt.Errorf("postload command failed: %w", err)
		}
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// Main Function
///////////////////////////////////////////////////////////////////////////////

/*
	Main Function:
	The entry point for the ETL tool. It parses command-line flags, reads the configuration,
	performs extraction, transformation, deduplication, and then writes the processed data to the destination.
*/
func main() {
	configFile := flag.String("config", "config/etl-config.yaml", "YAML configuration file for ETL tool")
	flagInputFile := flag.String("input", "", "Optional: override the input file path from config")
	dbConnStr := flag.String("db", "", "PostgreSQL connection string (optional, can use DB_CREDENTIALS env var)")
	logLevel := flag.String("loglevel", "info", "Logging level (none, info, debug)")
	flag.Parse()

	setLoggingLevel(*logLevel)
	logMessage("Starting ETL tool...", "info")

	cfgData, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}
	var config ETLConfig
	if err := yaml.Unmarshal(cfgData, &config); err != nil {
		log.Fatalf("Failed to parse YAML config: %v", err)
	}

	if *flagInputFile != "" {
		config.Source.File = *flagInputFile
		debugLog(fmt.Sprintf("Overriding input file path with: %s", *flagInputFile), "debug")
	}

	if config.Source.File == "" && strings.ToLower(config.Source.Type) != "postgres" {
		log.Fatalf("No input file specified (either via config or -input). For type=postgres, a query must be provided in the config.")
	}

	// Build the final DB connection string if needed.
	finalDBConn := *dbConnStr
	if finalDBConn == "" {
		finalDBConn = os.Getenv("DB_CREDENTIALS")
	}

	// Extraction: Read input data.
	inputReader := NewInputReader(config.Source, finalDBConn)
	records, err := inputReader.Read(config.Source.File)
	if err != nil {
		log.Fatalf("Failed to read input data: %v", err)
	}
	logMessage(fmt.Sprintf("Loaded %d records from input source.", len(records)), "info")

	// Transformation: Process each record.
	for i, rec := range records {
		records[i] = processRecord(rec, config.Mappings)
		debugLog(fmt.Sprintf("[DEBUG] Final record after processRecord: %v", records[i]), "debug")
	}

	// Deduplication: Remove duplicate records if configured.
	if config.Dedup != nil && len(config.Dedup.Keys) > 0 {
		origCount := len(records)
		records = dedupRecords(records, config.Dedup.Keys)
		logMessage(fmt.Sprintf("Deduplicated records: %d -> %d", origCount, len(records)), "info")
	}

	// Loading: Write the processed data to the destination.
	writer := NewOutputWriter(config.Destination, finalDBConn)
	if err := writer.Write(records); err != nil {
		log.Fatalf("Failed to write data: %v", err)
	}
	logMessage("Data loaded successfully.", "info")
}

// processRecord applies the mapping rules to transform an input record into an output record.
func processRecord(original map[string]interface{}, mappings []MappingRule) map[string]interface{} {
	debugLog(fmt.Sprintf("processRecord called with original=%+v", original), "debug")
	targetRecord := make(map[string]interface{})

	for i, m := range mappings {
		var srcValue interface{}
		// Allow sequential mappings by first checking the target record.
		if val, exists := targetRecord[m.Source]; exists {
			srcValue = val
		} else if val2, ok := original[m.Source]; ok {
			srcValue = val2
		}
		if m.Params == nil {
			m.Params = make(map[string]interface{})
		}
		m.Params["transformStr"] = m.Transform

		var transformedValue interface{}
		if m.Transform != "" {
			parts := strings.SplitN(m.Transform, ":", 2)
			funcName := parts[0]
			tf, found := transformRegistry[funcName]
			if !found {
				log.Printf("Warning: transformation function '%s' not found; using source value", funcName)
				transformedValue = srcValue
			} else {
				debugLog(fmt.Sprintf("Applying transform '%s' on srcValue=%v", funcName, srcValue), "debug")
				transformedValue = tf(srcValue, targetRecord, m.Params)
			}
		} else {
			transformedValue = srcValue
		}
		targetRecord[m.Target] = transformedValue

		debugLog(fmt.Sprintf("[DEBUG] After mapping #%d (source='%s', target='%s', transform='%s'), partial record=%+v",
			i, m.Source, m.Target, m.Transform, targetRecord), "debug")
	}
	return targetRecord
}

///////////////////////////////////////////////////////////////////////////////
// Logging Functions
///////////////////////////////////////////////////////////////////////////////

// Global log level; 0=none, 1=info, 2=debug.
var globalLogLevel int

// setLoggingLevel sets the global log level.
func setLoggingLevel(level string) {
	switch strings.ToLower(level) {
	case "none":
		globalLogLevel = 0
	case "info":
		globalLogLevel = 1
	case "debug":
		globalLogLevel = 2
	default:
		globalLogLevel = 1
	}
}

// logMessage logs messages at the appropriate log level.
func logMessage(message, level string) {
	lvMap := map[string]int{"none": 0, "info": 1, "debug": 2}
	if lv, ok := lvMap[strings.ToLower(level)]; ok && lv <= globalLogLevel {
		log.Println(message)
	}
}

// debugLog logs debug messages if the log level is set to debug.
func debugLog(msg string, level string) {
	if strings.ToLower(level) == "debug" && globalLogLevel >= 2 {
		log.Println(msg)
	}
}
