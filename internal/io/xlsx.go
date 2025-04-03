// --- START OF CORRECTED FILE internal/io/xlsx.go ---
package io

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"etl-tool/internal/config"
	"etl-tool/internal/logging"
	"github.com/xuri/excelize/v2"
)

// XLSXReader implements the InputReader interface for Excel (.xlsx) files.
type XLSXReader struct {
	sheetName  string
	sheetIndex *int
}

// NewXLSXReader creates a new XLSXReader with sheet preferences.
func NewXLSXReader(sheetName string, sheetIndex *int) *XLSXReader {
	return &XLSXReader{
		sheetName:  sheetName,
		sheetIndex: sheetIndex,
	}
}

// Read loads data from the specified sheet (or default) of an Excel file.
func (xr *XLSXReader) Read(filePath string) ([]map[string]interface{}, error) {
	logging.Logf(logging.Debug, "XLSXReader reading file: %s (SheetName: '%s', SheetIndex: %v)", filePath, xr.sheetName, xr.sheetIndex)

	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("XLSXReader failed to open file '%s': %w", filePath, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			logging.Logf(logging.Error, "XLSXReader failed to close file '%s': %v", filePath, err)
		}
	}()

	targetSheetName := ""
	// Logic to determine targetSheetName remains the same...
	if xr.sheetName != "" {
		found := false
		for _, name := range f.GetSheetList() {
			if name == xr.sheetName {
				targetSheetName = xr.sheetName
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("XLSXReader: specified sheet name '%s' not found in '%s'", xr.sheetName, filePath)
		}
		logging.Logf(logging.Debug, "XLSXReader: Using specified sheet name '%s'", targetSheetName)
	} else if xr.sheetIndex != nil {
		targetSheetName = f.GetSheetName(*xr.sheetIndex)
		if targetSheetName == "" {
			sheetCount := len(f.GetSheetList())
			if *xr.sheetIndex >= sheetCount || *xr.sheetIndex < 0 {
				return nil, fmt.Errorf("XLSXReader: specified sheet index %d is out of bounds (0 to %d) in '%s'", *xr.sheetIndex, sheetCount-1, filePath)
			}
			return nil, fmt.Errorf("XLSXReader: could not get sheet name for valid index %d in '%s'", *xr.sheetIndex, filePath)
		}
		logging.Logf(logging.Debug, "XLSXReader: Using specified sheet index %d ('%s')", *xr.sheetIndex, targetSheetName)
	} else {
		activeSheetIndex := f.GetActiveSheetIndex()
		targetSheetName = f.GetSheetName(activeSheetIndex)
		if targetSheetName == "" {
			if activeSheetIndex != 0 {
				targetSheetName = f.GetSheetName(0)
			}
			if targetSheetName == "" {
				if len(f.GetSheetList()) == 0 {
					return nil, fmt.Errorf("XLSXReader: file '%s' contains no sheets", filePath)
				}
				return nil, fmt.Errorf("XLSXReader: could not determine a valid sheet to read in '%s'", filePath)
			}
			logging.Logf(logging.Debug, "XLSXReader: Using first sheet '%s' (index 0) as default", targetSheetName)
		} else {
			logging.Logf(logging.Debug, "XLSXReader: Using active sheet '%s' (index %d) as default", targetSheetName, activeSheetIndex)
		}
	}


	rows, err := f.GetRows(targetSheetName)
	if err != nil {
		return nil, fmt.Errorf("XLSXReader failed to get rows from sheet '%s' in '%s': %w", targetSheetName, filePath, err)
	}

	// --- MODIFIED: Initialize records slice directly ---
	// Ensure we always return an initialized slice, not nil, if no data rows are processed.
	records := make([]map[string]interface{}, 0)
	// --- END MODIFICATION ---

	if len(rows) < 1 {
		logging.Logf(logging.Warning, "XLSX sheet '%s' in '%s' is empty or contains no header row.", targetSheetName, filePath)
		return records, nil // Return initialized empty slice
	}

	// Header processing logic remains the same...
	rawHeaders := rows[0]
	lastIndexForHeader := make(map[string]int)
	headerNameForIndex := make(map[int]string)
	for i, h := range rawHeaders {
		trimmedHeader := strings.TrimSpace(h)
		headerNameForIndex[i] = trimmedHeader
		if trimmedHeader != "" {
			lastIndexForHeader[trimmedHeader] = i
		} else {
			logging.Logf(logging.Warning, "XLSXReader: Empty header found in column %d of sheet '%s'. This column's data will be ignored.", i+1, targetSheetName)
		}
	}
	validHeadersMap := make(map[string]bool)
	validHeadersOrdered := []string{}
	tempHeaders := make([]string, 0, len(lastIndexForHeader))
	for header := range lastIndexForHeader {
		tempHeaders = append(tempHeaders, header)
	}
	sort.Strings(tempHeaders)
	for _, header := range tempHeaders {
		validHeadersMap[header] = true
		validHeadersOrdered = append(validHeadersOrdered, header)
	}


	if len(validHeadersMap) == 0 {
		logging.Logf(logging.Warning, "XLSXReader: No valid headers found in the first row of sheet '%s'. Cannot process data.", targetSheetName)
		return records, nil // Return initialized empty slice
	}
	logging.Logf(logging.Debug, "XLSXReader: Using unique headers (last wins): %v", validHeadersOrdered)


	// Data row processing loop remains the same...
	for i, row := range rows[1:] { // This loop correctly handles len(rows) == 1 (no iterations)
		rowNum := i + 2
		rec := make(map[string]interface{}, len(validHeadersMap))
		for cellIdx := 0; cellIdx < len(row); cellIdx++ {
			headerName, indexHasHeader := headerNameForIndex[cellIdx]
			if indexHasHeader && headerName != "" && lastIndexForHeader[headerName] == cellIdx {
				cellValue := ""
				if cellIdx < len(row) {
					cellValue = row[cellIdx]
				}
				cellName, _ := excelize.CoordinatesToCellName(cellIdx+1, rowNum)
				cellDisplayValue, err := f.GetCellValue(targetSheetName, cellName)
				if err != nil {
					logging.Logf(logging.Warning, "XLSXReader: Failed to get calculated value for cell %s on sheet '%s': %v. Using raw value '%s'.", cellName, targetSheetName, err, cellValue)
					rec[headerName] = cellValue
				} else {
					rec[headerName] = cellDisplayValue
				}
			}
		}
		for _, headerName := range validHeadersOrdered {
			if _, exists := rec[headerName]; !exists {
				rec[headerName] = ""
			}
		}
		records = append(records, rec) // Append to the initialized slice
	}


	logging.Logf(logging.Info, "XLSXReader successfully loaded %d records from sheet '%s' in %s", len(records), targetSheetName, filePath)
	return records, nil // Return the (potentially empty) initialized slice
}

// --- XLSXWriter code remains the same as previous correction ---
// (Includes boolean casing fix)
// XLSXWriter implements the OutputWriter interface for Excel (.xlsx) files.
type XLSXWriter struct {
	sheetName string
}

// NewXLSXWriter creates a new XLSXWriter.
func NewXLSXWriter(sheetName string) *XLSXWriter {
	name := sheetName
	if name == "" {
		name = config.DefaultSheetName
	}
	return &XLSXWriter{
		sheetName: name,
	}
}

// Write saves the provided records to the specified sheet of an Excel file.
func (xw *XLSXWriter) Write(records []map[string]interface{}, filePath string) error {
	logging.Logf(logging.Debug, "XLSXWriter writing %d records to file: %s (Sheet: '%s')", len(records), filePath, xw.sheetName)

	dir := filepath.Dir(filePath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("XLSXWriter failed to create directory for '%s': %w", filePath, err)
		}
	}

	f := excelize.NewFile()
	defaultSheetName := config.DefaultSheetName
	targetSheetName := xw.sheetName
	targetSheetIndex := 0

	existingIndex, _ := f.GetSheetIndex(defaultSheetName)
	if existingIndex == -1 {
		index, err := f.NewSheet(defaultSheetName)
		if err != nil {
			return fmt.Errorf("XLSXWriter failed to create default sheet '%s': %w", defaultSheetName, err)
		}
		existingIndex = index
	}

	if targetSheetName != defaultSheetName {
		errRename := f.SetSheetName(defaultSheetName, targetSheetName)
		if errRename != nil {
			existingTargetIndex, _ := f.GetSheetIndex(targetSheetName)
			if existingTargetIndex != -1 {
				logging.Logf(logging.Warning, "XLSXWriter: Target sheet '%s' already exists. Using existing sheet.", targetSheetName)
				targetSheetIndex = existingTargetIndex
			} else {
				logging.Logf(logging.Warning, "XLSXWriter: Failed to rename default sheet to '%s' (%v), attempting to create new sheet.", targetSheetName, errRename)
				newIndex, errCreate := f.NewSheet(targetSheetName)
				if errCreate != nil {
					if strings.Contains(errCreate.Error(), "invalid character") || strings.Contains(errCreate.Error(), "the sheet can not contain any of the characters") {
						return fmt.Errorf("XLSXWriter failed to create target sheet '%s': %w", targetSheetName, errCreate)
					}
					return fmt.Errorf("XLSXWriter failed to create target sheet '%s': %w", targetSheetName, errCreate)
				}
				targetSheetIndex = newIndex
				defaultIndexCheck, _ := f.GetSheetIndex(defaultSheetName)
				if defaultIndexCheck != -1 {
					if delErr := f.DeleteSheet(defaultSheetName); delErr != nil {
						logging.Logf(logging.Warning, "XLSXWriter: Failed to delete original default sheet '%s' after creating '%s': %v", defaultSheetName, targetSheetName, delErr)
					}
				}
			}
		} else {
			targetSheetIndex = existingIndex
		}
	} else {
		targetSheetIndex = existingIndex
	}

	f.SetActiveSheet(targetSheetIndex)

	if len(records) == 0 {
		logging.Logf(logging.Info, "XLSXWriter: No records provided, saving empty file %s with sheet '%s'.", filePath, targetSheetName)
		if err := f.SaveAs(filePath); err != nil {
			return fmt.Errorf("XLSXWriter failed to save empty file '%s': %w", filePath, err)
		}
		return nil
	}

	var headers []string
	headerSet := make(map[string]struct{})
	for _, rec := range records {
		for k := range rec {
			if _, exists := headerSet[k]; !exists {
				headerSet[k] = struct{}{}
				headers = append(headers, k)
			}
		}
	}
	sort.Strings(headers)

	headerRowInterface := make([]interface{}, len(headers))
	for i, h := range headers {
		headerRowInterface[i] = h
	}
	if err := f.SetSheetRow(targetSheetName, "A1", &headerRowInterface); err != nil {
		return fmt.Errorf("XLSXWriter failed to write header row to sheet '%s': %w", targetSheetName, err)
	}

	for i, rec := range records {
		rowNum := i + 2
		rowData := make([]interface{}, len(headers))
		for j, header := range headers {
			value := rec[header]
			if bVal, ok := value.(bool); ok {
				rowData[j] = strconv.FormatBool(bVal)
			} else {
				rowData[j] = value
			}
		}

		startCell, err := excelize.CoordinatesToCellName(1, rowNum)
		if err != nil {
			return fmt.Errorf("XLSXWriter failed to calculate cell coordinates for row %d: %w", rowNum, err)
		}

		if err := f.SetSheetRow(targetSheetName, startCell, &rowData); err != nil {
			return fmt.Errorf("XLSXWriter failed to write data row %d (Excel row %d) to sheet '%s': %w", i+1, rowNum, targetSheetName, err)
		}
	}

	if err := f.SaveAs(filePath); err != nil {
		return fmt.Errorf("XLSXWriter failed to save file '%s': %w", filePath, err)
	}

	logging.Logf(logging.Info, "XLSXWriter successfully wrote %d data rows (plus header) to sheet '%s' in %s", len(records), targetSheetName, filePath)
	return nil
}

// Close implements the OutputWriter interface.
func (xw *XLSXWriter) Close() error {
	logging.Logf(logging.Debug, "XLSXWriter Close called (no-op).")
	return nil
}
