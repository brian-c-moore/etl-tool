package transform

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Helper function to compare results, handling potential errors and using DeepEqual.
// Returns true if the results match (including error state), false otherwise.
// Make sure resultsMatch function is also present in the file
func resultsMatch(t *testing.T, got, want interface{}) bool {
	t.Helper() // Mark this as a test helper

	gotErr, gotIsErr := got.(error)
	wantErr, wantIsErr := want.(error)

	if gotIsErr != wantIsErr {
		// One is an error, the other isn't - clearly different.
		t.Errorf("Error mismatch: got error = %v (%[1]T), want error = %v (%[2]T)", got, want)
		return false
	}

	if gotIsErr { // Both are errors, compare them
		// Basic error string comparison is often sufficient for these tests.
		if gotErr.Error() != wantErr.Error() {
			t.Errorf("Error message mismatch:\n got: %q\nwant: %q", gotErr.Error(), wantErr.Error())
			return false
		}
		return true // Errors match
	}

	// Neither are errors, compare values using DeepEqual
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Value mismatch:\n got: %#v (%T)\nwant: %#v (%T)", got, got, want, want)
		return false
	}

	return true // Values match
}

// TestApplyTransform tests the main function dispatcher.
func TestApplyTransform(t *testing.T) {
	// Mock record state for context-aware transforms if needed
	record := map[string]interface{}{"fieldA": 10, "fieldB": "hello"}

	testCases := []struct {
		name          string
		transformStr  string
		params        map[string]interface{}
		sourceValue   interface{}
		recordState   map[string]interface{}
		expectedValue interface{} // Use interface{} to allow error results
	}{
		{
			name:          "Known function lookup - toUpperCase",
			transformStr:  "toUpperCase",
			params:        nil,
			sourceValue:   "test me",
			recordState:   nil,
			expectedValue: "TEST ME",
		},
		{
			name:          "Known function lookup - toInt",
			transformStr:  "toInt",
			params:        nil,
			sourceValue:   "123",
			recordState:   nil,
			expectedValue: int64(123),
		},
		{
			name:          "Unknown function",
			transformStr:  "nonExistentFunction",
			params:        nil,
			sourceValue:   "some value",
			recordState:   nil,
			expectedValue: "some value", // Should return original value
		},
		{
			name:          "Empty transform string",
			transformStr:  "",
			params:        nil,
			sourceValue:   42,
			recordState:   nil,
			expectedValue: 42, // Should return original value
		},
		{
			name:          "Function with explicit params - replaceAll",
			transformStr:  "replaceAll",
			params:        map[string]interface{}{"old": "l", "new": "X"},
			sourceValue:   "hello world",
			recordState:   nil,
			expectedValue: "heXXo worXd",
		},
		{
			name:          "Function with shorthand param - regexExtract",
			transformStr:  "regexExtract:(\\d+)", // Shorthand pattern
			params:        nil,                   // No explicit params
			sourceValue:   "order_id: 12345",
			recordState:   nil,
			expectedValue: "12345",
		},
		{
			name:          "Shorthand param ignored if explicit exists",
			transformStr:  "regexExtract:ignored",                      // Shorthand ignored
			params:        map[string]interface{}{"pattern": `(\w+)$`}, // Explicit param takes precedence
			sourceValue:   "item_code=ABC",
			recordState:   nil,
			expectedValue: "ABC",
		},
		{
			name:          "Shorthand param for function without default key",
			transformStr:  "toUpperCase:ignored", // Shorthand ignored, logs warning
			params:        nil,
			sourceValue:   "test",
			recordState:   nil,
			expectedValue: "TEST", // Function still works correctly
		},
		{
			name:          "Validation function passes",
			transformStr:  "validateRequired",
			params:        nil,
			sourceValue:   "not empty",
			recordState:   nil,
			expectedValue: "not empty", // Returns original value on success
		},
		{
			name:          "Validation function fails",
			transformStr:  "validateRequired",
			params:        nil,
			sourceValue:   "", // Empty string fails validation
			recordState:   nil,
			expectedValue: errors.New("required string value is empty or whitespace"), // Returns error
		},
		{
			name:          "Strict function passes",
			transformStr:  "mustToInt",
			params:        nil,
			sourceValue:   "456",
			recordState:   nil,
			expectedValue: int64(456),
		},
		{
			name:          "Strict function fails",
			transformStr:  "mustToInt",
			params:        nil,
			sourceValue:   "abc",
			recordState:   nil,
			expectedValue: errors.New("mustToInt: conversion failed for input 'abc' (type string)"), // Returns error
		},
		{
			name:         "Context-aware transform - branch",
			transformStr: "branch",
			params: map[string]interface{}{
				"branches": []interface{}{
					map[string]interface{}{"condition": "inputValue == 'original'", "value": "MatchedInput"},
					map[string]interface{}{"condition": "fieldA > 5", "value": "high"},
					map[string]interface{}{"condition": "fieldA <= 5", "value": "low"},
				},
			},
			sourceValue:   "original",
			recordState:   record,
			expectedValue: "MatchedInput",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function under test
			gotValue := ApplyTransform(tc.transformStr, tc.params, tc.sourceValue, tc.recordState)

			// Use helper to compare results, handles errors correctly
			resultsMatch(t, gotValue, tc.expectedValue)
		})
	}
}

// TestEpochToDate tests the epochToDate transformation.
func TestEpochToDate(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect string or original value
	}{
		{name: "valid int epoch", input: int(1678886400), want: "2023-03-15"},
		{name: "valid int64 epoch", input: int64(0), want: "1970-01-01"},
		{name: "valid float epoch", input: float64(1678886400.5), want: "2023-03-15"},
		{name: "valid string epoch", input: "1678886400", want: "2023-03-15"},
		{name: "valid string float epoch", input: "1678886400.999", want: "2023-03-15"},
		{name: "unlikely low int", input: int(100), want: "1970-01-01"},
		{name: "unlikely high float", input: float64(5e12), want: "160413-09-10"},
		{name: "zero string", input: "0", want: "1970-01-01"},
		{name: "negative epoch", input: int64(-315619200), want: "1960-01-01"},
		{name: "invalid string", input: "not a number", want: "not a number"},
		{name: "empty string", input: "", want: ""},
		{name: "nil input", input: nil, want: nil},
		{name: "bool input", input: true, want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := epochToDate(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestMustEpochToDate tests the strict epochToDate transformation.
func TestMustEpochToDate(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect string or error
	}{
		{name: "valid int epoch", input: int(1678886400), want: "2023-03-15"},
		{name: "valid string epoch", input: "1678886400", want: "2023-03-15"},
		{name: "valid float epoch", input: 1678886400.5, want: "2023-03-15"},
		{name: "unlikely low int", input: int(100), want: "1970-01-01"},
		{name: "unlikely high float", input: float64(5e12), want: "160413-09-10"},
		{name: "invalid string", input: "not a number", want: errors.New("mustEpochToDate: could not parse input 'not a number' (type string) as epoch seconds")},
		{name: "nil input", input: nil, want: errors.New("mustEpochToDate: could not parse input '<nil>' (type <nil>) as epoch seconds")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := mustEpochToDate(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestCalculateAge tests the calculateAge transformation.
func TestCalculateAge(t *testing.T) {
	// Approx epochs for testing relative ages
	nowEpoch := time.Now().Unix()
	epoch10DaysAgo := nowEpoch - (10 * 24 * 60 * 60)
	epoch5YearsAgo := time.Now().AddDate(-5, 0, 0).Unix()
	epochFuture := nowEpoch + (30 * 24 * 60 * 60)
	veryOldEpoch := int64(-30000000000) // Approx year 1013

	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect int or nil
	}{
		{name: "10 days ago", input: epoch10DaysAgo, want: 10},
		{name: "approx 5 years ago", input: epoch5YearsAgo, want: 5 * 365}, // Approx, doesn't account for leaps precisely here
		{name: "future date", input: epochFuture, want: 0},
		{name: "zero epoch", input: int64(0), want: int(math.Floor(time.Since(time.Unix(0, 0)).Hours() / 24))}, // Age since 1970
		{name: "low int epoch", input: int(100), want: int(math.Floor(time.Since(time.Unix(100, 0)).Hours() / 24))},
		{name: "invalid string", input: "text", want: nil},
		{name: "nil input", input: nil, want: nil},
		{name: "very old epoch", input: veryOldEpoch, want: int(math.Floor(time.Since(time.Unix(veryOldEpoch, 0)).Hours() / 24))}, // Calculated dynamically
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := calculateAge(tc.input, nil, nil)

			// Special handling for approximate age
			if tc.name == "approx 5 years ago" {
				gotAge, ok := got.(int)
				if !ok {
					t.Fatalf("calculateAge(%v) = %T, want int", tc.input, got)
				}
				// Allow a small tolerance for leap years etc.
				// dynamically calculate expected age based on system time for robustness
				expectedAge := int(math.Floor(time.Since(time.Unix(epoch5YearsAgo, 0)).Hours() / 24.0))
				if gotAge < expectedAge-2 || gotAge > expectedAge+2 {
					t.Errorf("calculateAge(%v) = %d, want approx %d (within +/- 2 days)", tc.input, gotAge, expectedAge)
				}
			} else {
				// Use standard comparison for other cases
				resultsMatch(t, got, tc.want)
			}
		})
	}
}

// TestRegexExtract tests the regexExtract transformation.
func TestRegexExtract(t *testing.T) {
	testCases := []struct {
		name   string
		input  interface{}
		params map[string]interface{}
		want   interface{} // Expect string or nil
	}{
		{name: "simple match", input: "Order ID: 12345", params: map[string]interface{}{"pattern": `(\d+)`}, want: "12345"},
		{name: "match word", input: "Status: Active", params: map[string]interface{}{"pattern": `Status: (\w+)`}, want: "Active"},
		{name: "no match", input: "Order ID: ABC", params: map[string]interface{}{"pattern": `(\d+)`}, want: nil},
		{name: "no capture group", input: "Order ID: 123", params: map[string]interface{}{"pattern": `Order ID: \d+`}, want: nil}, // Matches but doesn't capture
		{name: "empty string input", input: "", params: map[string]interface{}{"pattern": `(\w)`}, want: nil},
		{name: "nil input", input: nil, params: map[string]interface{}{"pattern": `.`}, want: nil},
		{name: "non-string input", input: 123, params: map[string]interface{}{"pattern": `.`}, want: nil},
		{name: "missing pattern param", input: "abc", params: nil, want: nil},
		{name: "empty pattern param", input: "abc", params: map[string]interface{}{"pattern": ""}, want: nil},
		{name: "invalid pattern param (type)", input: "abc", params: map[string]interface{}{"pattern": 123}, want: nil},
		{name: "invalid regex syntax", input: "abc", params: map[string]interface{}{"pattern": `(`}, want: nil}, // Logs error internally
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := regexExtract(tc.input, nil, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestTrim tests the trim transformation.
func TestTrim(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{name: "leading/trailing spaces", input: "  hello world  ", want: "hello world"},
		{name: "leading spaces only", input: "  hello", want: "hello"},
		{name: "trailing spaces only", input: "world  ", want: "world"},
		{name: "spaces and tabs", input: "\t hello \t", want: "hello"},
		{name: "newlines", input: "\nhello\nworld\n", want: "hello\nworld"}, // Doesn't trim internal whitespace
		{name: "no spaces", input: "no_spaces", want: "no_spaces"},
		{name: "empty string", input: "", want: ""},
		{name: "whitespace only string", input: "   \t \n ", want: ""},
		{name: "non-string number", input: 123, want: 123},
		{name: "nil input", input: nil, want: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := trim(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestToUpper tests the toUpperCase transformation.
func TestToUpper(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{name: "lowercase string", input: "hello", want: "HELLO"},
		{name: "mixed case string", input: "HeLlO wOrLd", want: "HELLO WORLD"},
		{name: "uppercase string", input: "ALREADY UPPER", want: "ALREADY UPPER"},
		{name: "string with numbers/symbols", input: "Test 123!", want: "TEST 123!"},
		{name: "empty string", input: "", want: ""},
		{name: "non-string number", input: 123, want: 123}, // Returns unchanged
		{name: "non-string bool", input: true, want: true}, // Returns unchanged
		{name: "nil input", input: nil, want: nil},         // Returns unchanged
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := toUpperCase(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestToLower tests the toLowerCase transformation.
func TestToLower(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{name: "uppercase string", input: "HELLO", want: "hello"},
		{name: "mixed case string", input: "HeLlO wOrLd", want: "hello world"},
		{name: "lowercase string", input: "already lower", want: "already lower"},
		{name: "string with numbers/symbols", input: "TEST 123!", want: "test 123!"},
		{name: "empty string", input: "", want: ""},
		{name: "non-string number", input: 123, want: 123}, // Returns unchanged
		{name: "non-string bool", input: true, want: true}, // Returns unchanged
		{name: "nil input", input: nil, want: nil},         // Returns unchanged
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := toLowerCase(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestBranchTransform tests the branch transformation logic directly.
func TestBranchTransform(t *testing.T) {
	record := map[string]interface{}{
		"status":      "active",
		"amount":      100,
		"category":    "A",
		"maybeExists": nil,
	}
	originalValue := "original" // Value passed as input to transform

	testCases := []struct {
		name   string
		params map[string]interface{}
		record map[string]interface{}
		input  interface{}
		want   interface{} // Expect branch value or original input
	}{
		{
			name: "first condition matches",
			params: map[string]interface{}{"branches": []interface{}{
				map[string]interface{}{"condition": "status == 'active'", "value": "Branch1"},
				map[string]interface{}{"condition": "amount > 50", "value": "Branch2"},
			}},
			record: record, input: originalValue, want: "Branch1",
		},
		{
			name: "second condition matches",
			params: map[string]interface{}{"branches": []interface{}{
				map[string]interface{}{"condition": "status == 'inactive'", "value": "Branch1"},
				map[string]interface{}{"condition": "amount == 100", "value": "Branch2"},
			}},
			record: record, input: originalValue, want: "Branch2",
		},
		{
			name: "no condition matches",
			params: map[string]interface{}{"branches": []interface{}{
				map[string]interface{}{"condition": "status == 'inactive'", "value": "Branch1"},
				map[string]interface{}{"condition": "amount < 0", "value": "Branch2"},
			}},
			record: record, input: originalValue, want: originalValue, // Returns original input
		},
		{
			name: "condition uses input value",
			params: map[string]interface{}{"branches": []interface{}{
				map[string]interface{}{"condition": "inputValue == 'original'", "value": "MatchedInput"},
			}},
			record: record, input: originalValue, want: "MatchedInput",
		},
		{
			name: "condition evaluation error",
			params: map[string]interface{}{"branches": []interface{}{
				map[string]interface{}{"condition": "status + 1", "value": "WontMatch"}, // Type mismatch
			}},
			record: record, input: originalValue, want: originalValue, // Skips branch, returns original
		},
		{
			name: "invalid condition syntax",
			params: map[string]interface{}{"branches": []interface{}{
				map[string]interface{}{"condition": "status ==", "value": "WontMatch"}, // Syntax error
			}},
			record: record, input: originalValue, want: originalValue, // Skips branch, returns original (logs error)
		},
		{
			name:   "missing branches param",
			params: nil, record: record, input: originalValue, want: originalValue,
		},
		{
			name:   "branches param not array",
			params: map[string]interface{}{"branches": "not-an-array"}, record: record, input: originalValue, want: originalValue,
		},
		{
			name:   "branches array empty",
			params: map[string]interface{}{"branches": []interface{}{}}, record: record, input: originalValue, want: originalValue,
		},
		{
			name: "branch definition not map",
			params: map[string]interface{}{"branches": []interface{}{
				"not-a-map",
				map[string]interface{}{"condition": "amount == 100", "value": "Branch2"},
			}},
			record: record, input: originalValue, want: "Branch2", // Skips invalid branch, matches second
		},
		{
			name: "branch missing condition",
			params: map[string]interface{}{"branches": []interface{}{
				map[string]interface{}{"value": "NoCondition"},
				map[string]interface{}{"condition": "amount == 100", "value": "Branch2"},
			}},
			record: record, input: originalValue, want: "Branch2", // Skips invalid branch
		},
		{
			name: "branch missing value",
			params: map[string]interface{}{"branches": []interface{}{
				map[string]interface{}{"condition": "status == 'active'"}, // Missing value
				map[string]interface{}{"condition": "amount == 100", "value": "Branch2"},
			}},
			record: record, input: originalValue, want: "Branch2", // Skips invalid branch
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := branchTransform(tc.input, tc.record, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestDateConvert tests the permissive dateConvert transformation.
func TestDateConvert(t *testing.T) {
	tNow := time.Now() // Fixed time for consistent results
	tEpochZero := time.Unix(0, 0).UTC()

	testCases := []struct {
		name   string
		input  interface{}
		params map[string]interface{}
		want   interface{} // Expect string or original value
	}{
		{name: "RFC3339 to YYYY-MM-DD", input: "2023-03-15T10:30:00Z", params: map[string]interface{}{"outputFormat": "2006-01-02"}, want: "2023-03-15"},
		{name: "YYYY-MM-DD input to RFC3339 output", input: "2024-01-20", params: map[string]interface{}{"inputFormat": "2006-01-02"}, want: "2024-01-20T00:00:00Z"}, // Assumes UTC if no tz
		{name: "MM/DD/YYYY input to YYYYMMDD output", input: "05/25/2023", params: map[string]interface{}{"inputFormat": "01/02/2006", "outputFormat": "20060102"}, want: "20230525"},
		{name: "time.Time input to HH:MM output", input: tNow, params: map[string]interface{}{"outputFormat": "15:04"}, want: tNow.Format("15:04")},
		{name: "Default formats (RFC3339 in/out)", input: "2023-11-01T12:00:00Z", params: nil, want: "2023-11-01T12:00:00Z"},
		{name: "Fallback parse YYYY/MM/DD", input: "2023/04/01", params: map[string]interface{}{"outputFormat": "2006-01-02"}, want: "2023-04-01"}, // No input format provided
		{name: "Fallback parse MM/DD/YYYY", input: "12/31/2022", params: nil, want: "2022-12-31T00:00:00Z"},                                        // No input format provided
		{name: "Fallback parse YYYYMMDD", input: "20230101", params: map[string]interface{}{"outputFormat": "01/02/2006"}, want: "01/01/2023"},
		{name: "Specific format parse fails", input: "2023-03-15", params: map[string]interface{}{"inputFormat": "01/02/2006"}, want: "2023-03-15"}, // Returns original
		{name: "Fallback parse fails", input: "15-Mar-2023", params: nil, want: "15-Mar-2023"},                                                      // Returns original
		{name: "Nil input", input: nil, params: nil, want: nil},
		{name: "Non-string/non-time input", input: 123, params: nil, want: 123},
		{name: "Empty output format", input: "2023-01-01", params: map[string]interface{}{"inputFormat": "2006-01-02", "outputFormat": ""}, want: "2023-01-01T00:00:00Z"}, // Defaults output to RFC3339
		{name: "Zero time input", input: tEpochZero, params: map[string]interface{}{"outputFormat": "2006-01-02"}, want: "1970-01-01"},
		{name: "Invalid time string input", input: "invalid date", params: map[string]interface{}{"inputFormat": "2006-01-02"}, want: "invalid date"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := dateConvert(tc.input, nil, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestMustDateConvert tests the strict dateConvert transformation.
func TestMustDateConvert(t *testing.T) {
	testCases := []struct {
		name   string
		input  interface{}
		params map[string]interface{}
		want   interface{} // Expect string or error
	}{
		{name: "Valid conversion", input: "05/25/2023", params: map[string]interface{}{"inputFormat": "01/02/2006", "outputFormat": "2006-01-02"}, want: "2023-05-25"},
		{name: "Fallback parse success", input: "2023/04/01", params: map[string]interface{}{"outputFormat": "2006-01-02"}, want: "2023-04-01"},
		{name: "Specific format parse fails", input: "2023-03-15", params: map[string]interface{}{"inputFormat": "01/02/2006"}, want: errors.New("mustDateConvert: failed to parse '2023-03-15' with specified format '01/02/2006': parsing time \"2023-03-15\": month out of range")},
		{name: "Fallback parse fails", input: "15-Mar-2023", params: nil, want: errors.New("mustDateConvert: failed to parse '15-Mar-2023' with default format or common fallbacks")},
		{name: "Nil input", input: nil, params: nil, want: errors.New("mustDateConvert: input value is not a string or time.Time (type <nil>)")},
		{name: "Non-string/non-time input", input: 123, params: nil, want: errors.New("mustDateConvert: input value is not a string or time.Time (type int)")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := mustDateConvert(tc.input, nil, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestMultiDateConvert tests the permissive multiDateConvert transformation.
func TestMultiDateConvert(t *testing.T) {
	formats := []interface{}{"2006-01-02", "01/02/2006", "2006/01/02"}
	outputFmt := "20060102"

	testCases := []struct {
		name   string
		input  interface{}
		params map[string]interface{}
		want   interface{} // Expect string or original value
	}{
		{name: "First format matches", input: "2023-03-15", params: map[string]interface{}{"formats": formats, "outputFormat": outputFmt}, want: "20230315"},
		{name: "Second format matches", input: "04/01/2024", params: map[string]interface{}{"formats": formats, "outputFormat": outputFmt}, want: "20240401"},
		{name: "Third format matches", input: "2022/12/31", params: map[string]interface{}{"formats": formats, "outputFormat": outputFmt}, want: "20221231"},
		{name: "No format matches", input: "15 Mar 2023", params: map[string]interface{}{"formats": formats, "outputFormat": outputFmt}, want: "15 Mar 2023"}, // Returns original
		{name: "Nil input", input: nil, params: map[string]interface{}{"formats": formats, "outputFormat": outputFmt}, want: nil},
		{name: "Non-string input", input: 123, params: map[string]interface{}{"formats": formats, "outputFormat": outputFmt}, want: 123},
		{name: "Missing formats param", input: "2023-01-01", params: map[string]interface{}{"outputFormat": outputFmt}, want: "2023-01-01"},                           // Returns original
		{name: "Missing outputFormat param", input: "2023-01-01", params: map[string]interface{}{"formats": formats}, want: "2023-01-01"},                             // Returns original
		{name: "Empty formats array", input: "2023-01-01", params: map[string]interface{}{"formats": []interface{}{}, "outputFormat": outputFmt}, want: "2023-01-01"}, // Returns original
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := multiDateConvert(tc.input, nil, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestToInt tests the permissive toInt transformation.
func TestToInt(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect int64 or nil
	}{
		{name: "valid int string", input: "12345", want: int64(12345)},
		{name: "valid negative int string", input: "-50", want: int64(-50)},
		{name: "valid float string (whole)", input: "99.0", want: int64(99)},
		{name: "valid float string (negative whole)", input: "-100.000", want: int64(-100)},
		{name: "float string (fractional)", input: "99.5", want: nil}, // Fails conversion
		{name: "string with space", input: " 123 ", want: int64(123)},
		{name: "int input", input: int(789), want: int64(789)},
		{name: "int32 input", input: int32(-1), want: int64(-1)},
		{name: "uint64 input (fits)", input: uint64(1 << 30), want: int64(1 << 30)},
		{name: "uint64 input (overflow)", input: uint64(math.MaxUint64), want: nil}, // Overflows int64
		{name: "float32 input (whole)", input: float32(42.0), want: int64(42)},
		{name: "float64 input (whole)", input: float64(-10.0), want: int64(-10)},
		{name: "float input (fractional)", input: 3.14, want: nil}, // Fails conversion
		{name: "bool true", input: true, want: nil},                // Fails conversion
		{name: "bool false", input: false, want: nil},              // Fails conversion
		{name: "invalid string", input: "abc", want: nil},
		{name: "empty string", input: "", want: nil},
		{name: "nil input", input: nil, want: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := toInt(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestMustToInt tests the strict mustToInt transformation.
func TestMustToInt(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect int64 or error
	}{
		{name: "valid int string", input: "12345", want: int64(12345)},
		{name: "valid int", input: int(-50), want: int64(-50)},
		{name: "valid float string (whole)", input: "99.0", want: int64(99)},
		{name: "float string (fractional)", input: "99.5", want: errors.New("mustToInt: conversion failed for input '99.5' (type string)")},
		{name: "uint64 input (overflow)", input: uint64(math.MaxUint64), want: errors.New("mustToInt: conversion failed for input '18446744073709551615' (type uint64)")},
		{name: "float input (fractional)", input: 3.14, want: errors.New("mustToInt: conversion failed for input '3.14' (type float64)")},
		{name: "invalid string", input: "abc", want: errors.New("mustToInt: conversion failed for input 'abc' (type string)")},
		{name: "nil input", input: nil, want: errors.New("mustToInt: conversion failed for input '<nil>' (type <nil>)")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := mustToInt(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestToFloat tests the permissive toFloat transformation.
func TestToFloat(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect float64 or nil
	}{
		{name: "valid float string", input: "123.45", want: float64(123.45)},
		{name: "valid negative float string", input: "-0.5", want: float64(-0.5)},
		{name: "valid int string", input: "99", want: float64(99.0)},
		{name: "string with space", input: " 1.23 ", want: float64(1.23)},
		{name: "scientific notation string", input: "1.5e3", want: float64(1500.0)},
		{name: "int input", input: int(789), want: float64(789.0)},
		{name: "int32 input", input: int32(-1), want: float64(-1.0)},
		{name: "uint64 input", input: uint64(1 << 63), want: float64(1 << 63)}, // May lose precision but converts
		{name: "float32 input", input: float32(42.5), want: float64(42.5)},
		{name: "float64 input", input: float64(-10.1), want: float64(-10.1)},
		{name: "bool true", input: true, want: nil},   // Fails conversion
		{name: "bool false", input: false, want: nil}, // Fails conversion
		{name: "invalid string", input: "abc", want: nil},
		{name: "empty string", input: "", want: nil},
		{name: "nil input", input: nil, want: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := toFloat(tc.input, nil, nil)
			// Handle float comparison carefully if needed, but DeepEqual often works
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestMustToFloat tests the strict mustToFloat transformation.
func TestMustToFloat(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect float64 or error
	}{
		{name: "valid float string", input: "123.45", want: float64(123.45)},
		{name: "valid int", input: int(-50), want: float64(-50.0)},
		{name: "bool true", input: true, want: errors.New("mustToFloat: conversion failed for input 'true' (type bool)")},
		{name: "invalid string", input: "abc", want: errors.New("mustToFloat: conversion failed for input 'abc' (type string)")},
		{name: "nil input", input: nil, want: errors.New("mustToFloat: conversion failed for input '<nil>' (type <nil>)")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := mustToFloat(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestToBool tests the permissive toBool transformation.
func TestToBool(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect bool or nil
	}{
		// True values
		{name: "bool true", input: true, want: true},
		{name: "string true", input: "true", want: true},
		{name: "string TRUE", input: "TRUE", want: true},
		{name: "string t", input: "t", want: true},
		{name: "string T", input: "T", want: true},
		{name: "string 1", input: "1", want: true},
		{name: "string yes", input: "yes", want: true},
		{name: "string Y", input: "Y", want: true},
		{name: "int 1", input: 1, want: true},
		{name: "int -1", input: -1, want: true}, // Non-zero int is true
		{name: "float 1.0", input: 1.0, want: true},
		{name: "float -0.5", input: -0.5, want: true}, // Non-zero float is true
		// False values
		{name: "bool false", input: false, want: false},
		{name: "string false", input: "false", want: false},
		{name: "string FALSE", input: "FALSE", want: false},
		{name: "string f", input: "f", want: false},
		{name: "string F", input: "F", want: false},
		{name: "string 0", input: "0", want: false},
		{name: "string no", input: "no", want: false},
		{name: "string N", input: "N", want: false},
		{name: "string empty", input: "", want: false}, // Empty string is false
		{name: "int 0", input: 0, want: false},
		{name: "uint 0", input: uint(0), want: false},
		{name: "float 0.0", input: 0.0, want: false},
		{name: "nil input", input: nil, want: false}, // Nil is false
		// Ambiguous/Invalid -> nil
		{name: "string ambiguous maybe", input: "maybe", want: nil},
		{name: "string ambiguous whitespace", input: "  ", want: false}, // Whitespace trims to empty -> false
		{name: "map input", input: map[string]int{"a": 1}, want: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := toBool(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestMustToBool tests the strict mustToBool transformation.
func TestMustToBool(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect bool or error
	}{
		// True values
		{name: "bool true", input: true, want: true},
		{name: "string true", input: "true", want: true},
		{name: "string 1", input: "1", want: true},
		{name: "int 1", input: 1, want: true},
		// False values
		{name: "bool false", input: false, want: false},
		{name: "string false", input: "false", want: false},
		{name: "string 0", input: "0", want: false},
		{name: "int 0", input: 0, want: false},
		// Error cases
		{name: "nil input", input: nil, want: errors.New("mustToBool: input is nil")},
		{name: "string empty", input: "", want: errors.New("mustToBool: unrecognized or ambiguous string value ''")}, // Changed from false to error
		{name: "string ambiguous maybe", input: "maybe", want: errors.New("mustToBool: unrecognized or ambiguous string value 'maybe'")},
		{name: "map input", input: map[string]int{"a": 1}, want: errors.New("mustToBool: conversion received unsupported type 'map[string]int'")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := mustToBool(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestToString tests the toString transformation.
func TestToString(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{name: "string input", input: "hello", want: "hello"},
		{name: "int input", input: 123, want: "123"},
		{name: "float input", input: 3.14, want: "3.14"},
		{name: "bool true input", input: true, want: "true"},
		{name: "bool false input", input: false, want: "false"},
		{name: "nil input", input: nil, want: ""}, // Converts nil to empty string
		{name: "byte slice input", input: []byte("bytes"), want: "bytes"},
		{name: "empty byte slice", input: []byte{}, want: ""},
		{name: "time input", input: time.Date(2023, 3, 15, 10, 0, 0, 0, time.UTC), want: "2023-03-15 10:00:00 +0000 UTC"}, // Default time string format
		{name: "map input", input: map[string]int{"a": 1}, want: "map[a:1]"},                                              // Default map string format
		{name: "slice input", input: []int{1, 2}, want: "[1 2]"},                                                          // Default slice string format
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := toString(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestReplaceAll tests the replaceAll transformation.
func TestReplaceAll(t *testing.T) {
	testCases := []struct {
		name   string
		input  interface{}
		params map[string]interface{}
		want   interface{}
	}{
		{name: "simple replace", input: "hello world", params: map[string]interface{}{"old": "l", "new": "X"}, want: "heXXo worXd"},
		{name: "replace multiple", input: "ababab", params: map[string]interface{}{"old": "ab", "new": "c"}, want: "ccc"},
		{name: "replace with empty", input: "hello", params: map[string]interface{}{"old": "l", "new": ""}, want: "heo"},
		{name: "replace empty with char", input: "hello", params: map[string]interface{}{"old": "", "new": "-"}, want: "-h-e-l-l-o-"}, // Standard ReplaceAll behavior
		{name: "no occurrences", input: "hello", params: map[string]interface{}{"old": "z", "new": "X"}, want: "hello"},
		{name: "non-string input", input: 123, params: map[string]interface{}{"old": "1", "new": "9"}, want: 123}, // Returns original
		{name: "missing old param", input: "hello", params: map[string]interface{}{"new": "X"}, want: "hello"},    // Returns original
		{name: "missing new param", input: "hello", params: map[string]interface{}{"old": "l"}, want: "hello"},    // Returns original
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := replaceAll(tc.input, nil, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestSubstring tests the substring transformation.
func TestSubstring(t *testing.T) {
	testCases := []struct {
		name   string
		input  interface{}
		params map[string]interface{}
		want   interface{}
	}{
		{name: "simple substring", input: "hello world", params: map[string]interface{}{"start": 6, "length": 5}, want: "world"},
		{name: "substring from start", input: "hello world", params: map[string]interface{}{"start": 0, "length": 5}, want: "hello"},
		{name: "substring to end", input: "hello world", params: map[string]interface{}{"start": 6, "length": 10}, want: "world"}, // Length exceeds bounds
		{name: "length 1", input: "hello", params: map[string]interface{}{"start": 1, "length": 1}, want: "e"},
		{name: "multibyte runes", input: "你好世界", params: map[string]interface{}{"start": 2, "length": 2}, want: "世界"},
		{name: "start out of bounds", input: "hello", params: map[string]interface{}{"start": 10, "length": 1}, want: ""},
		{name: "negative start", input: "hello", params: map[string]interface{}{"start": -2, "length": 3}, want: "hel"}, // Treats negative start as 0
		{name: "zero length", input: "hello", params: map[string]interface{}{"start": 1, "length": 0}, want: ""},
		{name: "negative length", input: "hello", params: map[string]interface{}{"start": 1, "length": -1}, want: ""},
		{name: "non-string input", input: 123, params: map[string]interface{}{"start": 0, "length": 1}, want: 123},              // Returns original
		{name: "missing start", input: "hello", params: map[string]interface{}{"length": 1}, want: "hello"},                     // Returns original
		{name: "missing length", input: "hello", params: map[string]interface{}{"start": 1}, want: "hello"},                     // Returns original
		{name: "params wrong type", input: "hello", params: map[string]interface{}{"start": "a", "length": "b"}, want: "hello"}, // Returns original
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := substring(tc.input, nil, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestCoalesceTransform tests the coalesce transformation.
func TestCoalesceTransform(t *testing.T) {
	record := map[string]interface{}{
		"fieldA": nil,
		"fieldB": "", // Empty string
		"fieldC": "Value C",
		"fieldD": 0,
		"fieldE": false,
		"fieldF": "Value F",
	}

	testCases := []struct {
		name   string
		params map[string]interface{}
		record map[string]interface{}
		want   interface{} // Expect first non-nil/non-empty-string value, or nil
	}{
		{name: "first non-nil", params: map[string]interface{}{"fields": []interface{}{"fieldA", "fieldB", "fieldC"}}, record: record, want: "Value C"},
		{name: "includes non-string non-nil", params: map[string]interface{}{"fields": []interface{}{"fieldA", "fieldB", "fieldD"}}, record: record, want: 0},
		{name: "includes false", params: map[string]interface{}{"fields": []interface{}{"fieldA", "fieldB", "fieldE"}}, record: record, want: false},
		{name: "all are nil/empty string", params: map[string]interface{}{"fields": []interface{}{"fieldA", "fieldB"}}, record: record, want: nil},
		{name: "field not in record", params: map[string]interface{}{"fields": []interface{}{"missing", "fieldF"}}, record: record, want: "Value F"},
		{name: "empty fields array", params: map[string]interface{}{"fields": []interface{}{}}, record: record, want: nil},
		{name: "missing fields param", params: nil, record: record, want: nil},
		{name: "fields not array", params: map[string]interface{}{"fields": "not-array"}, record: record, want: nil},
		{name: "field name not string", params: map[string]interface{}{"fields": []interface{}{123, "fieldC"}}, record: record, want: "Value C"}, // Skips invalid field name
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Input value to coalesce is ignored
			got := coalesceTransform(nil, tc.record, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestValidateRequired tests the validateRequired validation.
func TestValidateRequired(t *testing.T) {
	testCases := []struct {
		name  string
		input interface{}
		want  interface{} // Expect original value or error
	}{
		{name: "valid string", input: "hello", want: "hello"},
		{name: "valid number", input: 123, want: 123},
		{name: "valid bool", input: false, want: false},
		{name: "nil input", input: nil, want: errors.New("required value is missing (nil)")},
		{name: "empty string", input: "", want: errors.New("required string value is empty or whitespace")},
		{name: "whitespace string", input: "   \t\n", want: errors.New("required string value is empty or whitespace")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateRequired(tc.input, nil, nil)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestValidateRegex tests the validateRegex validation.
func TestValidateRegex(t *testing.T) {
	testCases := []struct {
		name   string
		input  interface{}
		params map[string]interface{}
		want   interface{} // Expect original value or error
	}{
		{name: "valid match", input: "abc123xyz", params: map[string]interface{}{"pattern": `^\w+\d+\w+$`}, want: "abc123xyz"},
		{name: "no match", input: "abc_xyz", params: map[string]interface{}{"pattern": `^\w+\d+\w+$`}, want: errors.New(fmt.Sprintf("value %s does not match required pattern '^\\w+\\d+\\w+$'", strconv.Quote("abc_xyz")))},
		{name: "empty string match", input: "", params: map[string]interface{}{"pattern": `^$`}, want: ""},
		{name: "empty string no match", input: "", params: map[string]interface{}{"pattern": `.`}, want: errors.New(fmt.Sprintf("value %s does not match required pattern '.'", strconv.Quote("")))},
		{name: "non-string input passes", input: 123, params: map[string]interface{}{"pattern": `\d+`}, want: 123}, // Corrected: returns original value
		{name: "nil input passes", input: nil, params: map[string]interface{}{"pattern": `.`}, want: nil},             // Corrected: returns original value
		{name: "missing pattern", input: "abc", params: nil, want: errors.New("missing or empty 'pattern' string parameter for validateRegex")},
		{name: "empty pattern", input: "abc", params: map[string]interface{}{"pattern": ""}, want: errors.New("missing or empty 'pattern' string parameter for validateRegex")},
		{name: "invalid pattern syntax", input: "abc", params: map[string]interface{}{"pattern": `(`}, want: errors.New("invalid regex pattern '(': error parsing regexp: missing closing ): `(`")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateRegex(tc.input, nil, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestValidateNumericRange tests the validateNumericRange validation.
func TestValidateNumericRange(t *testing.T) {
	testCases := []struct {
		name   string
		input  interface{}
		params map[string]interface{}
		want   interface{} // Expect original value or error
	}{
		// Valid cases
		{name: "within range (int)", input: 50, params: map[string]interface{}{"min": 0, "max": 100}, want: 50},
		{name: "within range (float)", input: 50.5, params: map[string]interface{}{"min": 0.0, "max": 100.0}, want: 50.5},
		{name: "within range (string)", input: "50", params: map[string]interface{}{"min": "0", "max": "100"}, want: "50"},
		{name: "at min boundary", input: 0, params: map[string]interface{}{"min": 0, "max": 100}, want: 0},
		{name: "at max boundary", input: 100.0, params: map[string]interface{}{"min": 0.0, "max": 100.0}, want: 100.0},
		{name: "only min specified (valid)", input: 150, params: map[string]interface{}{"min": 100}, want: 150},
		{name: "only max specified (valid)", input: -50, params: map[string]interface{}{"max": 0}, want: -50},
		// Invalid cases
		{name: "below min (int)", input: -10, params: map[string]interface{}{"min": 0, "max": 100}, want: errors.New("value -10 is less than minimum allowed 0")},
		{name: "above max (float)", input: 100.1, params: map[string]interface{}{"min": 0.0, "max": 100.0}, want: errors.New("value 100.1 is greater than maximum allowed 100")},
		{name: "below min (only min specified)", input: 99, params: map[string]interface{}{"min": 100}, want: errors.New("value 99 is less than minimum allowed 100")},
		{name: "above max (only max specified)", input: 0.1, params: map[string]interface{}{"max": 0}, want: errors.New("value 0.1 is greater than maximum allowed 0")},
		{name: "non-numeric input passes", input: "abc", params: map[string]interface{}{"min": 0, "max": 100}, want: "abc"}, // Corrected: returns original value
		{name: "nil input passes", input: nil, params: map[string]interface{}{"min": 0}, want: nil},                         // Corrected: returns original value
		// Config errors
		{name: "missing min/max", input: 50, params: nil, want: errors.New("requires at least 'min' or 'max' parameter for validateNumericRange")},
		{name: "min not number", input: 50, params: map[string]interface{}{"min": "a", "max": 100}, want: errors.New("invalid 'min' parameter: 'a' is not a valid number")},
		{name: "max not number", input: 50, params: map[string]interface{}{"min": 0, "max": "b"}, want: errors.New("invalid 'max' parameter: 'b' is not a valid number")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateNumericRange(tc.input, nil, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestValidateAllowedValues tests the validateAllowedValues validation.
func TestValidateAllowedValues(t *testing.T) {
	allowedStrings := []interface{}{"apple", "banana", "cherry"}
	allowedInts := []interface{}{10, 20, 30}
	allowedMixed := []interface{}{"active", 1, true, nil}

	testCases := []struct {
		name   string
		input  interface{}
		params map[string]interface{}
		want   interface{} // Expect original value or error
	}{
		// Valid cases
		{name: "string found", input: "banana", params: map[string]interface{}{"values": allowedStrings}, want: "banana"},
		{name: "int found", input: 20, params: map[string]interface{}{"values": allowedInts}, want: 20},
		{name: "int as string found", input: "20", params: map[string]interface{}{"values": allowedInts}, want: "20"}, // Uses CompareValues, treats "20" == 20
		{name: "string as int found", input: 20, params: map[string]interface{}{"values": []interface{}{"10", "20", "30"}}, want: 20},
		{name: "mixed found (string)", input: "active", params: map[string]interface{}{"values": allowedMixed}, want: "active"},
		{name: "mixed found (int)", input: 1, params: map[string]interface{}{"values": allowedMixed}, want: 1},
		{name: "mixed found (bool)", input: true, params: map[string]interface{}{"values": allowedMixed}, want: true},
		{name: "mixed found (nil)", input: nil, params: map[string]interface{}{"values": allowedMixed}, want: nil},
		// Invalid cases
		{name: "string not found", input: "grape", params: map[string]interface{}{"values": allowedStrings}, want: errors.New("value 'grape' is not in the list of allowed values")},
		{name: "int not found", input: 25, params: map[string]interface{}{"values": allowedInts}, want: errors.New("value '25' is not in the list of allowed values")},
		{name: "type mismatch (int vs string list)", input: 10, params: map[string]interface{}{"values": allowedStrings}, want: errors.New("value '10' is not in the list of allowed values")},
		{name: "nil not in list", input: nil, params: map[string]interface{}{"values": allowedStrings}, want: errors.New("value '<nil>' is not in the list of allowed values")},
		// Config errors
		{name: "missing values param", input: "apple", params: nil, want: errors.New("missing 'values' array parameter for validateAllowedValues")},
		{name: "values not array", input: "apple", params: map[string]interface{}{"values": "not-an-array"}, want: errors.New("'values' parameter is not a non-empty array for validateAllowedValues")},
		{name: "values empty array", input: "apple", params: map[string]interface{}{"values": []interface{}{}}, want: errors.New("'values' parameter is not a non-empty array for validateAllowedValues")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateAllowedValues(tc.input, nil, tc.params)
			resultsMatch(t, got, tc.want)
		})
	}
}

// TestCompareValues tests the robust value comparison function.
func TestCompareValues(t *testing.T) {
	time1 := time.Now()
	time2 := time1.Add(time.Hour)

	testCases := []struct {
		name    string
		inputA  interface{}
		inputB  interface{}
		want    int  // -1 (a<b), 0 (a==b), 1 (a>b)
		wantErr bool // True if comparison should error
	}{
		// Numeric Comparisons
		{name: "int equal", inputA: 10, inputB: 10, want: 0, wantErr: false},
		{name: "int less", inputA: 5, inputB: 10, want: -1, wantErr: false},
		{name: "int greater", inputA: 15, inputB: 10, want: 1, wantErr: false},
		{name: "float equal", inputA: 3.14, inputB: 3.14, want: 0, wantErr: false},
		{name: "float less", inputA: 2.71, inputB: 3.14, want: -1, wantErr: false},
		{name: "float greater", inputA: 4.0, inputB: 3.14, want: 1, wantErr: false},
		{name: "int vs float equal", inputA: 10, inputB: 10.0, want: 0, wantErr: false},
		{name: "int vs float less", inputA: 9, inputB: 9.5, want: -1, wantErr: false},
		{name: "int vs float greater", inputA: 11, inputB: 10.5, want: 1, wantErr: false},
		{name: "string number vs int equal", inputA: "100", inputB: 100, want: 0, wantErr: false},
		{name: "string number vs float less", inputA: "99.9", inputB: 100.0, want: -1, wantErr: false},
		{name: "int vs string number greater", inputA: 101, inputB: "100.5", want: 1, wantErr: false},
		// String Comparisons
		{name: "string equal", inputA: "hello", inputB: "hello", want: 0, wantErr: false},
		{name: "string less", inputA: "apple", inputB: "banana", want: -1, wantErr: false},
		{name: "string greater", inputA: "world", inputB: "hello", want: 1, wantErr: false},
		{name: "empty strings", inputA: "", inputB: "", want: 0, wantErr: false},
		// Time Comparisons
		{name: "time equal", inputA: time1, inputB: time1, want: 0, wantErr: false},
		{name: "time less", inputA: time1, inputB: time2, want: -1, wantErr: false},
		{name: "time greater", inputA: time2, inputB: time1, want: 1, wantErr: false},
		// Bool Comparisons
		{name: "bool equal true", inputA: true, inputB: true, want: 0, wantErr: false},
		{name: "bool equal false", inputA: false, inputB: false, want: 0, wantErr: false},
		{name: "bool true > false", inputA: true, inputB: false, want: 1, wantErr: false},
		{name: "bool false < true", inputA: false, inputB: true, want: -1, wantErr: false},
		// Nil Comparisons
		{name: "nil equal", inputA: nil, inputB: nil, want: 0, wantErr: false},
		{name: "nil less than int", inputA: nil, inputB: 10, want: -1, wantErr: false},
		{name: "string greater than nil", inputA: "hello", inputB: nil, want: 1, wantErr: false},
		// Incompatible Type Comparisons
		{name: "int vs string", inputA: 10, inputB: "hello", want: 0, wantErr: true}, // Type mismatch error
		{name: "string vs int", inputA: "hello", inputB: 10, want: 0, wantErr: true},   // Type mismatch error
		{name: "bool vs int", inputA: true, inputB: 1, want: 0, wantErr: true},       // Type mismatch error
		{name: "time vs string", inputA: time1, inputB: "now", want: 0, wantErr: true},  // Type mismatch error
		{name: "map vs map (equal)", inputA: map[string]int{"a": 1}, inputB: map[string]int{"a": 1}, want: 0, wantErr: false}, // DeepEqual handles this
		{name: "map vs map (unequal)", inputA: map[string]int{"a": 1}, inputB: map[string]int{"a": 2}, want: 0, wantErr: true}, // Cannot determine order
		{name: "slice vs slice (equal)", inputA: []int{1, 2}, inputB: []int{1, 2}, want: 0, wantErr: false},                    // DeepEqual handles this
		{name: "slice vs slice (unequal)", inputA: []int{1, 2}, inputB: []int{1, 3}, want: 0, wantErr: true},                   // Cannot determine order
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CompareValues(tc.inputA, tc.inputB)

			if (err != nil) != tc.wantErr {
				t.Errorf("CompareValues(%v, %v) error presence mismatch: got error = %v, want error = %v", tc.inputA, tc.inputB, err, tc.wantErr)
			} else if !tc.wantErr && got != tc.want {
				t.Errorf("CompareValues(%v, %v) = %d, want %d", tc.inputA, tc.inputB, got, tc.want)
			}
		})
	}
}

// TestHashTransform tests the hash generation logic using canonical string representations.
func TestHashTransform(t *testing.T) {
	record := map[string]interface{}{
		"firstName": "John",
		"lastName":  "Doe",
		"id":        123,
		"city":      "Anytown",
		"password":  "secret",
		"amount":    123.45,
		"active":    true,
		"timestamp": time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	// Expected hashes recalculated using the valueToStringForHash logic:
	// Fields (sorted): active, amount, city, firstName, id, lastName, password, timestamp
	// Canonical String: true||123.45||Anytown||John||123||Doe||secret||2023-01-01T12:00:00Z
	expectedSHA256 := "486957b4de918ecd413fcab67ebf4a506d23c68eb1e326e2d53139d9561b0c1a"
	expectedSHA512 := "f17f7cf76cc3a718df1c73b13cc2bc0d2dc555839a780489a9498dbe736bb3e173997809c96a2c09bedbf9445be07fb6d0423f88af1e01a3ea16fa7aeb11c5f2"
	expectedMD5 := "51245b998bce45b14a42462766f7c092"

	// Fields (sorted): active, amount, city, firstName, id, lastName, missingField, password, timestamp
	// Canonical String: true||123.45||Anytown||John||123||Doe||<MISSING>||secret||2023-01-01T12:00:00Z
	expectedMissingSHA256 := "2ec620a1c5adb5f2b2193447967528f942ea0e4e9a22f4c3f56b690860a3c363"

	// Fields (sorted): active, amount, city, firstName, id, lastName, nilField, password, timestamp
	// Canonical String: true||123.45||Anytown||John||123||Doe||<NIL>||secret||2023-01-01T12:00:00Z
	expectedNilSHA256 := "60c14d53cdc9d846113bd571b1c0edaf22d884cfdc811e345650d2f18f2c3a49"

	testCases := []struct {
		name    string
		params  map[string]interface{}
		record  map[string]interface{}
		want    interface{} // Expected hash string or error
		fipsSet bool        // Whether to enable FIPS mode for this test
	}{
		{
			name: "SHA256_success",
			params: map[string]interface{}{
				"algorithm": "sha256",
				"fields":    []interface{}{"lastName", "id", "firstName", "city", "password", "amount", "active", "timestamp"}, // Unordered
			},
			record: record,
			want:   expectedSHA256,
		},
		{
			name: "SHA512_success",
			params: map[string]interface{}{
				"algorithm": "SHA512",
				"fields":    []interface{}{"active", "amount", "city", "firstName", "id", "lastName", "password", "timestamp"}, // Ordered
			},
			record: record,
			want:   expectedSHA512,
		},
		{
			name: "MD5_success_(FIPS_off)",
			params: map[string]interface{}{
				"algorithm": "md5",
				"fields":    []interface{}{"active", "amount", "city", "firstName", "id", "lastName", "password", "timestamp"}, // Ordered
			},
			record:  record,
			want:    expectedMD5,
			fipsSet: false,
		},
		{
			name: "MD5_fail_(FIPS_on)",
			params: map[string]interface{}{
				"algorithm": "md5",
				"fields":    []interface{}{"id"},
			},
			record:  record,
			want:    errors.New("hash algorithm 'md5' not allowed in FIPS mode"),
			fipsSet: true,
		},
		{
			name: "SHA256_success_(FIPS_on)",
			params: map[string]interface{}{
				"algorithm": "sha256",
				"fields":    []interface{}{"active", "amount", "city", "firstName", "id", "lastName", "password", "timestamp"}, // Ordered
			},
			record:  record,
			want:    expectedSHA256,
			fipsSet: true,
		},
		{
			name: "Missing_field_included",
			params: map[string]interface{}{
				"algorithm": "sha256",
				"fields":    []interface{}{"active", "amount", "city", "firstName", "id", "lastName", "missingField", "password", "timestamp"}, // Ordered + missing
			},
			record: record,
			want:   expectedMissingSHA256,
		},
		{
			name: "Nil_value_field_included",
			params: map[string]interface{}{
				"algorithm": "sha256",
				"fields":    []interface{}{"active", "amount", "city", "firstName", "id", "lastName", "nilField", "password", "timestamp"}, // Ordered + nil
			},
			record: func() map[string]interface{} {
				r := make(map[string]interface{})
				for k, v := range record {
					r[k] = v
				}
				r["nilField"] = nil
				return r
			}(),
			want: expectedNilSHA256,
		},
		{
			name: "Error_-_missing_algorithm",
			params: map[string]interface{}{
				"fields": []interface{}{"id"},
			},
			record: record,
			want:   errors.New("missing 'algorithm' parameter for hash transform"),
		},
		{
			name: "Error_-_missing_fields",
			params: map[string]interface{}{
				"algorithm": "sha256",
			},
			record: record,
			want:   errors.New("missing 'fields' parameter for hash transform"),
		},
		{
			name: "Error_-_fields_not_array",
			params: map[string]interface{}{
				"algorithm": "sha256",
				"fields":    "not-an-array",
			},
			record: record,
			want:   errors.New("'fields' parameter must be a non-empty array for hash transform"),
		},
		{
			name: "Error_-_fields_empty_array",
			params: map[string]interface{}{
				"algorithm": "sha256",
				"fields":    []interface{}{},
			},
			record: record,
			want:   errors.New("'fields' parameter must be a non-empty array for hash transform"),
		},
		{
			name: "Error_-_field_name_not_string",
			params: map[string]interface{}{
				"algorithm": "sha256",
				"fields":    []interface{}{"id", 123}, // Contains non-string
			},
			record: record,
			want:   errors.New("field name at index 1 is not a string for hash transform"),
		},
		{
			name: "Error_-_unknown_algorithm",
			params: map[string]interface{}{
				"algorithm": "sha1", // Not supported
				"fields":    []interface{}{"id"},
			},
			record: record,
			want:   errors.New("unsupported hash algorithm: sha1"),
		},
	}

	originalFIPS := IsFIPSMode()    // Store original FIPS state
	defer SetFIPSMode(originalFIPS) // Ensure FIPS state is reset after tests

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set FIPS mode for this specific test case
			SetFIPSMode(tc.fipsSet)
			// Reset FIPS mode after this subtest finishes
			t.Cleanup(func() { SetFIPSMode(originalFIPS) })

			// Call the function being tested
			// hashTransform ignores the first 'value' argument.
			got := hashTransform(nil, tc.record, tc.params)
			resultsMatch(t, got, tc.want) // Original assertion
		})
	}
}
