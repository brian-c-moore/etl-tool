// --- START OF CORRECTED FILE internal/io/xlsx_test.go ---
package io

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"etl-tool/internal/config" // For default sheet name constant
	"github.com/xuri/excelize/v2"
)

// --- Test Helpers for XLSX ---

// createTempXLSX creates a temporary XLSX file with specified data on a given sheet.
// data should be a slice of slices (e.g., [][]string or [][]interface{}), where the first inner slice is the header.
func createTempXLSX(t *testing.T, sheetName string, data [][]interface{}) string {
	t.Helper()
	f := excelize.NewFile()
	// Delete the default "Sheet1" if we are creating a differently named sheet first.
	// If sheetName is "Sheet1", this will fail harmlessly.
	if sheetName != config.DefaultSheetName {
		_ = f.DeleteSheet(config.DefaultSheetName) // Ignore error if it doesn't exist
	}

	// Create the target sheet
	index, err := f.NewSheet(sheetName)
	if err != nil {
		t.Fatalf("Failed to create sheet '%s': %v", sheetName, err)
	}
	f.SetActiveSheet(index) // Set the created sheet as active

	// Write data row by row
	for r, rowData := range data {
		startCell, err := excelize.CoordinatesToCellName(1, r+1) // A1, A2, ...
		if err != nil {
			t.Fatalf("Failed to get cell coordinates for row %d: %v", r+1, err)
		}
		// excelize needs []*interface{} or []interface{} for SetSheetRow
		// Convert rowData to []interface{} if it's not already
		interfaceRow := make([]interface{}, len(rowData))
		copy(interfaceRow, rowData) // Use copy instead of loop
		if err := f.SetSheetRow(sheetName, startCell, &interfaceRow); err != nil {
			t.Fatalf("Failed to set row %d on sheet '%s': %v", r+1, sheetName, err)
		}
	}

	// Save to temporary file
	tempDir := t.TempDir()
	tempFile, err := os.CreateTemp(tempDir, "test_*.xlsx")
	if err != nil {
		t.Fatalf("Failed to create temp file placeholder: %v", err)
	}
	filePath := tempFile.Name() // Assign filePath here
	if err := tempFile.Close(); err != nil { // Close the placeholder file
		t.Fatalf("Failed to close placeholder file: %v", err)
	}

	if err := f.SaveAs(filePath); err != nil {
		t.Fatalf("Failed to save temp XLSX file %s: %v", filePath, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Failed to close excelize file object: %v", err)
	}

	return filePath
}

// readXLSXFile reads back all rows from a specified sheet in an XLSX file.
func readXLSXFile(t *testing.T, filePath, sheetName string) [][]string {
	t.Helper()
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		// Check if the file just doesn't exist (e.g., writer test expected no creation)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatalf("Failed to open XLSX file %s: %v", filePath, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Logf("Warning: failed to close XLSX file %s: %v", filePath, err)
		}
	}()

	rows, err := f.GetRows(sheetName)
	if err != nil {
		// Check if the sheet doesn't exist
		_, sheetErr := f.GetSheetIndex(sheetName)
		if sheetErr != nil { // If GetSheetIndex also fails, sheet probably doesn't exist
			t.Logf("Sheet '%s' not found in %s during readback", sheetName, filePath)
			return nil // Return nil if sheet specifically doesn't exist
		}
		// Otherwise, it was some other error reading rows
		t.Fatalf("Failed to get rows from sheet '%s' in %s: %v", sheetName, filePath, err)
	}
	return rows
}

// --- Test XLSXReader ---

func TestNewXLSXReader(t *testing.T) {
	idxPtr := func(i int) *int { return &i }

	testCases := []struct {
		name          string
		sheetName     string
		sheetIndex    *int
		wantSheetName string
		wantSheetIndex *int
	}{
		{"No specific sheet", "", nil, "", nil},
		{"By Name", "DataSheet", nil, "DataSheet", nil},
		{"By Index", "", idxPtr(1), "", idxPtr(1)},
		{"Name takes precedence", "DataSheet", idxPtr(1), "DataSheet", idxPtr(1)},
		{"By Index 0", "", idxPtr(0), "", idxPtr(0)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := NewXLSXReader(tc.sheetName, tc.sheetIndex)
			if reader.sheetName != tc.wantSheetName {
				t.Errorf("reader.sheetName = %q, want %q", reader.sheetName, tc.wantSheetName)
			}
			// Compare pointers carefully
			if (reader.sheetIndex == nil && tc.wantSheetIndex != nil) ||
			   (reader.sheetIndex != nil && tc.wantSheetIndex == nil) ||
			   (reader.sheetIndex != nil && tc.wantSheetIndex != nil && *reader.sheetIndex != *tc.wantSheetIndex) {
				gotIdxStr := "nil"; if reader.sheetIndex != nil { gotIdxStr = fmt.Sprintf("%d", *reader.sheetIndex) }
				wantIdxStr := "nil"; if tc.wantSheetIndex != nil { wantIdxStr = fmt.Sprintf("%d", *tc.wantSheetIndex) }
				t.Errorf("reader.sheetIndex = %s, want %s", gotIdxStr, wantIdxStr)
			}
		})
	}
}

func TestXLSXReader_Read(t *testing.T) {
	intPtr := func(i int) *int { return &i }

	// Data for test files
	sheet1Data := [][]interface{}{
		{"ID", "Name", "Value"},
		{1, "Alice", 100.5},
		{2, "Bob", 200},
	}
	sheet2Data := [][]interface{}{
		{"Key", "Description"},
		{"A", "Item A"},
		{"B", "Item B"},
	}
	sheetWithEmptyHeader := [][]interface{}{
		{"ColA", "", "ColC"},
		{"ValA1", "Ignored1", "ValC1"},
		{"ValA2", "Ignored2", "ValC2"},
	}
	sheetWithDuplicateHeader := [][]interface{}{
		{"Header1", "Header2", "Header1"},
		{"DataA1", "DataB1", "DataC1"}, // Expect C1 for Header1
		{"DataA2", "DataB2", "DataC2"}, // Expect C2 for Header1
	}

	// Setup a multi-sheet file
	multiSheetFile := func(t *testing.T) string {
		t.Helper()
		f := excelize.NewFile()
		_, _ = f.NewSheet("Sheet1")
		f.SetActiveSheet(0)
		for r, row := range sheet1Data {
			startCell, _ := excelize.CoordinatesToCellName(1, r+1)
			interfaceRow := make([]interface{}, len(row))
			copy(interfaceRow, row)
			_ = f.SetSheetRow("Sheet1", startCell, &interfaceRow)
		}
		_, _ = f.NewSheet("Sheet2")
		for r, row := range sheet2Data {
			startCell, _ := excelize.CoordinatesToCellName(1, r+1)
			interfaceRow := make([]interface{}, len(row))
			copy(interfaceRow, row)
			_ = f.SetSheetRow("Sheet2", startCell, &interfaceRow)
		}

		tempDir := t.TempDir()
		tempFile, err := os.CreateTemp(tempDir, "multisheet_*.xlsx")
		if err != nil { t.Fatalf("Failed to create temp file placeholder: %v", err) }
		filePath := tempFile.Name()
		if err := tempFile.Close(); err != nil { t.Fatalf("Failed to close placeholder file: %v", err) }
		if err := f.SaveAs(filePath); err != nil { t.Fatalf("Failed to save temp XLSX file %s: %v", filePath, err) }
		if err := f.Close(); err != nil { t.Fatalf("Failed to close excelize file object: %v", err) }
		return filePath
	}

	testCases := []struct {
		name          string
		setupFile     func(t *testing.T) string // Function to create the test file
		sheetName     string
		sheetIndex    *int
		wantRecords   []map[string]interface{}
		wantErr       bool
		wantErrMsgSub string // Substring to check in error message
	}{
		{
			name:      "Read default sheet (Sheet1)",
			setupFile: func(t *testing.T) string { return createTempXLSX(t, "Sheet1", sheet1Data) },
			wantRecords: []map[string]interface{}{
				{"ID": "1", "Name": "Alice", "Value": "100.5"},
				{"ID": "2", "Name": "Bob", "Value": "200"},
			},
			wantErr: false,
		},
		{
			name:        "Read by sheet name (Sheet2)",
			setupFile:   multiSheetFile,
			sheetName:   "Sheet2",
			wantRecords: []map[string]interface{}{{"Key": "A", "Description": "Item A"}, {"Key": "B", "Description": "Item B"}},
			wantErr:     false,
		},
		{
			name:        "Read by sheet index (Sheet2 -> index 1)",
			setupFile:   multiSheetFile,
			sheetIndex:  intPtr(1),
			wantRecords: []map[string]interface{}{{"Key": "A", "Description": "Item A"}, {"Key": "B", "Description": "Item B"}},
			wantErr:     false,
		},
		{
			name:        "Read by name takes precedence",
			setupFile:   multiSheetFile,
			sheetName:   "Sheet1",
			sheetIndex:  intPtr(1),
			wantRecords: []map[string]interface{}{{"ID": "1", "Name": "Alice", "Value": "100.5"}, {"ID": "2", "Name": "Bob", "Value": "200"}},
			wantErr:     false,
		},
		{
			name:        "Empty sheet",
			setupFile:   func(t *testing.T) string { return createTempXLSX(t, "EmptySheet", [][]interface{}{}) },
			sheetName:   "EmptySheet",
			wantRecords: []map[string]interface{}{},
			wantErr:     false,
		},
		{
			name:        "Sheet with header only",
			setupFile:   func(t *testing.T) string { return createTempXLSX(t, "HeaderOnly", [][]interface{}{{"H1", "H2"}}) },
			sheetName:   "HeaderOnly",
			wantRecords: []map[string]interface{}{},
			wantErr:     false, // This test case should now pass
		},
		{
			name:          "Sheet name not found",
			setupFile:     multiSheetFile,
			sheetName:     "NonExistentSheet",
			wantRecords:   nil,
			wantErr:       true,
			wantErrMsgSub: "specified sheet name 'NonExistentSheet' not found",
		},
		{
			name:          "Sheet index out of bounds (positive)",
			setupFile:     multiSheetFile,
			sheetIndex:    intPtr(2),
			wantRecords:   nil,
			wantErr:       true,
			wantErrMsgSub: "sheet index 2 is out of bounds",
		},
		{
			name:          "Sheet index out of bounds (negative)",
			setupFile:     multiSheetFile,
			sheetIndex:    intPtr(-1),
			wantRecords:   nil,
			wantErr:       true,
			wantErrMsgSub: "sheet index -1 is out of bounds",
		},
		{
			name: "File with empty header column",
			setupFile: func(t *testing.T) string { return createTempXLSX(t, "Sheet1", sheetWithEmptyHeader) },
			wantRecords: []map[string]interface{}{
				{"ColA": "ValA1", "ColC": "ValC1"},
				{"ColA": "ValA2", "ColC": "ValC2"},
			},
			wantErr: false,
		},
		{
			name: "File with duplicate header column (last wins)",
			setupFile: func(t *testing.T) string { return createTempXLSX(t, "Sheet1", sheetWithDuplicateHeader) },
			// --- CORRECTED EXPECTATION ---
			wantRecords: []map[string]interface{}{
				{"Header1": "DataC1", "Header2": "DataB1"}, // Value for Header1 comes from 3rd column (DataC1)
				{"Header1": "DataC2", "Header2": "DataB2"}, // Value for Header1 comes from 3rd column (DataC2)
			},
			// --- END CORRECTION ---
			wantErr: false, // This test case should now pass
		},
		{
			name: "File with no valid headers",
			setupFile: func(t *testing.T) string { return createTempXLSX(t, "Sheet1", [][]interface{}{{"", ""}, {"a","b"}}) },
			wantRecords: []map[string]interface{}{},
			wantErr:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := tc.setupFile(t)
			reader := NewXLSXReader(tc.sheetName, tc.sheetIndex)
			gotRecords, err := reader.Read(filePath)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("Read() error = nil, want error containing %q", tc.wantErrMsgSub)
				}
				if tc.wantErrMsgSub != "" && !strings.Contains(err.Error(), tc.wantErrMsgSub) {
					t.Errorf("Read() error message = %q, want error containing %q", err.Error(), tc.wantErrMsgSub)
				}
			} else {
				if err != nil {
					t.Fatalf("Read() returned unexpected error: %v", err)
				}
				if !compareRecordsDeep(t, gotRecords, tc.wantRecords) {
					// compareRecordsDeep logs details on failure
				}
			}
		})
	}

	t.Run("File Not Found", func(t *testing.T) {
		reader := NewXLSXReader("", nil)
		nonExistentPath := filepath.Join(t.TempDir(), "non_existent_file.xlsx")
		_, err := reader.Read(nonExistentPath)
		if err == nil {
			t.Fatalf("Read() for non-existent file returned nil error, want error")
		}
		if !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "no such file or directory") {
			t.Errorf("Read() error type = %T, message = %q, want os.ErrNotExist or similar", err, err.Error())
		}
	})
}

// --- Test XLSXWriter ---

func TestNewXLSXWriter(t *testing.T) {
	testCases := []struct {
		name          string
		sheetName     string
		wantSheetName string
	}{
		{"Specific Name", "MyData", "MyData"},
		{"Empty Name Uses Default", "", config.DefaultSheetName},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := NewXLSXWriter(tc.sheetName)
			if writer.sheetName != tc.wantSheetName {
				t.Errorf("NewXLSXWriter(%q).sheetName = %q, want %q", tc.sheetName, writer.sheetName, tc.wantSheetName)
			}
		})
	}
}

func TestXLSXWriter_Write(t *testing.T) {
	records := []map[string]interface{}{
		{"Str": "Value1", "Int": 100, "Float": 12.34, "Bool": true, "Nil": nil},
		{"Str": "Value2", "Int": -50, "Float": -0.5, "Bool": false, "Extra": "extra data"},
	}
	wantHeaders := []string{"Bool", "Extra", "Float", "Int", "Nil", "Str"} // Correct sorted headers including 'Extra'
	// --- CORRECTED EXPECTED ROWS (Lowercase bools) ---
	wantRows := [][]string{
		{"true", "", "12.34", "100", "", "Value1"}, // Bool is "true", Extra missing in row 1 -> "", Nil -> ""
		{"false", "extra data", "-0.5", "-50", "", "Value2"}, // Bool is "false", Nil missing -> ""
	}
	// --- END CORRECTION ---

	testCases := []struct {
		name         string
		records      []map[string]interface{}
		sheetName    string
		setupDir     bool
		expectDir    string
		wantSheet    string
		wantHeaders  []string
		wantRows     [][]string
		wantErr      bool
		wantErrMsgSub string
	}{
		{
			name:        "Write valid records (default sheet)",
			records:     records,
			sheetName:   "",
			wantSheet:   config.DefaultSheetName,
			wantHeaders: wantHeaders,
			wantRows:    wantRows,
			wantErr:     false,
		},
		{
			name:        "Write valid records (custom sheet)",
			records:     records,
			sheetName:   "OutputData",
			wantSheet:   "OutputData",
			wantHeaders: wantHeaders,
			wantRows:    wantRows,
			wantErr:     false,
		},
		{
			name:        "Write empty record slice (creates empty file/sheet)", // Corrected description
			records:     []map[string]interface{}{},
			sheetName:   "EmptyTest",
			wantSheet:   "EmptyTest",
			wantHeaders: nil, // No headers if no records
			wantRows:    nil,
			wantErr:     false,
		},
		{
			name:        "Write nil record slice (creates empty file/sheet)",
			records:     nil,
			sheetName:   "NilTest",
			wantSheet:   "NilTest",
			wantHeaders: nil,
			wantRows:    nil,
			wantErr:     false,
		},
		{
			name:        "Write with directory creation",
			records:     records[:1], // Just the first record
			sheetName:   "SubdirData",
			setupDir:    true,
			expectDir:   "excel_output/nested",
			wantSheet:   "SubdirData",
			wantHeaders: []string{"Bool", "Float", "Int", "Nil", "Str"}, // Headers from first record only
			wantRows:    [][]string{{"true", "12.34", "100", "", "Value1"}},
			wantErr:     false,
		},
		{
			name:          "Write with invalid sheet name",
			records:       records,
			sheetName:     "Invalid:Sheet",
			wantSheet:     "Invalid:Sheet",
			wantHeaders:   nil,
			wantRows:      nil,
			wantErr:       true,
			wantErrMsgSub: "the sheet can not contain any of the characters", // Match specific excelize error part
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			filePath := filepath.Join(tmpDir, "output.xlsx")
			if tc.setupDir {
				filePath = filepath.Join(tmpDir, tc.expectDir, "output.xlsx")
			}

			writer := NewXLSXWriter(tc.sheetName)
			err := writer.Write(tc.records, filePath)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("Write() error = nil, want error containing %q", tc.wantErrMsgSub)
				}
				// --- CORRECTED ERROR SUBSTRING CHECK ---
				if tc.wantErrMsgSub != "" && !strings.Contains(err.Error(), tc.wantErrMsgSub) {
					t.Errorf("Write() error message = %q, want error containing %q", err.Error(), tc.wantErrMsgSub)
				}
				// --- END CORRECTION ---
				if _, statErr := os.Stat(filePath); statErr == nil {
					t.Logf("File %s was created despite Write error", filePath)
				}
				return
			}

			if err != nil {
				t.Fatalf("Write() returned unexpected error: %v", err)
			}

			if tc.expectDir != "" {
				dirPath := filepath.Join(tmpDir, tc.expectDir)
				if _, statErr := os.Stat(dirPath); os.IsNotExist(statErr) {
					t.Errorf("Expected directory %s was not created", dirPath)
				}
			}

			gotRows := readXLSXFile(t, filePath, tc.wantSheet)
			if gotRows == nil && len(tc.wantHeaders) == 0 && len(tc.wantRows) == 0 {
				t.Logf("File created, expected sheet '%s' is likely empty (as expected).", tc.wantSheet)
			} else if gotRows == nil {
				t.Fatalf("Failed to read back expected sheet '%s' from file %s", tc.wantSheet, filePath)
			} else if len(tc.wantHeaders) > 0 {
				if len(gotRows) < 1 {
					t.Fatalf("Expected header row %v, but got no rows", tc.wantHeaders)
				}
				// Don't sort tc.wantHeaders here, writer uses sorted unique keys from records
				if !reflect.DeepEqual(gotRows[0], tc.wantHeaders) {
					t.Errorf("Header mismatch:\ngot:  %v\nwant: %v", gotRows[0], tc.wantHeaders)
				}
				gotDataRows := [][]string{}
				if len(gotRows) > 1 {
					gotDataRows = gotRows[1:]
				}
				if !reflect.DeepEqual(gotDataRows, tc.wantRows) {
					t.Errorf("Data rows mismatch:\ngot:  %v\nwant: %v", gotDataRows, tc.wantRows)
				}
			} else if len(gotRows) > 0 {
				t.Errorf("Expected empty sheet '%s', but got %d rows.", tc.wantSheet, len(gotRows))
			}
		})
	}

	t.Run("Directory Creation Failure", func(t *testing.T) {
		tmpDir := t.TempDir()
		conflictingFilePath := filepath.Join(tmpDir, "output_dir_conflict")
		if err := os.WriteFile(conflictingFilePath, []byte("file content"), 0644); err != nil {
			t.Fatalf("Failed to create conflicting file: %v", err)
		}

		filePath := filepath.Join(conflictingFilePath, "output.xlsx")
		writer := NewXLSXWriter("Sheet1")
		err := writer.Write(records, filePath)

		if err == nil {
			t.Fatalf("Write() succeeded unexpectedly when directory creation should fail")
		}
		if !strings.Contains(err.Error(), "create directory") || (!strings.Contains(strings.ToLower(err.Error()), "not a directory") && !strings.Contains(strings.ToLower(err.Error()), "is a file")) {
			t.Errorf("Write() error message %q does not indicate directory creation failure", err.Error())
		}
	})
}

func TestXLSXWriter_Close(t *testing.T) {
	writer := NewXLSXWriter("TestSheet")
	err := writer.Close()
	if err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Errorf("Close() second call returned unexpected error: %v", err)
	}
}
