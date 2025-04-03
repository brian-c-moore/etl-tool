package processor

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"etl-tool/internal/config"
	etlio "etl-tool/internal/io" // Use aliased import for internal io package
	"etl-tool/internal/transform"
)

// --- Mocks and Helpers ---

// mockErrorWriter implements the etlio.ErrorWriter interface for testing purposes.
type mockErrorWriter struct {
	mu              sync.Mutex
	writeCalls      []struct {
		Record map[string]interface{}
		Err    error
	}
	closeCalls      int
	writeShouldFail bool
	closeShouldFail bool
}

func (m *mockErrorWriter) Write(record map[string]interface{}, processError error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	recordCopy := make(map[string]interface{})
	for k, v := range record {
		recordCopy[k] = v
	}
	m.writeCalls = append(m.writeCalls, struct { Record map[string]interface{}; Err error }{recordCopy, processError})
	if m.writeShouldFail {
		return errors.New("mock write error")
	}
	return nil
}

func (m *mockErrorWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	if m.closeShouldFail {
		return errors.New("mock close error")
	}
	return nil
}

func (m *mockErrorWriter) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCalls = nil
	m.closeCalls = 0
	m.writeShouldFail = false
	m.closeShouldFail = false
}

// canonicalMapString creates a consistent string representation of a map by sorting keys.
func canonicalMapString(m map[string]interface{}) string {
	if m == nil { return "<nil_map>" }; keys := make([]string, 0, len(m)); for k := range m { keys = append(keys, k) }; sort.Strings(keys); var sb strings.Builder; sb.WriteString("{")
	for i, k := range keys { fmt.Fprintf(&sb, "%q:%#v", k, m[k]); if i < len(keys)-1 { sb.WriteString(",") } }; sb.WriteString("}"); return sb.String()
}

// recordsEqualIgnoringOrder compares two slices of maps for equality, ignoring order.
func recordsEqualIgnoringOrder(got, want []map[string]interface{}) bool {
	if len(got) != len(want) { return false }; if len(got) == 0 { return true }; gotFreq := make(map[string]int); wantFreq := make(map[string]int)
	for _, r := range got { gotFreq[canonicalMapString(r)]++ }; for _, r := range want { wantFreq[canonicalMapString(r)]++ }; return reflect.DeepEqual(gotFreq, wantFreq)
}

// printRecordsDiff helps visualize differences between slices of maps for debugging.
func printRecordsDiff(t *testing.T, got, want []map[string]interface{}) {
	t.Helper(); gotStrings := make([]string, len(got)); wantStrings := make([]string, len(want))
	for i, r := range got { gotStrings[i] = canonicalMapString(r) }; for i, r := range want { wantStrings[i] = canonicalMapString(r) }
	sort.Strings(gotStrings); sort.Strings(wantStrings)
	t.Logf("GOT Records (%d):\n%s", len(got), strings.Join(gotStrings, "\n"))
	t.Logf("WANT Records (%d):\n%s", len(want), strings.Join(wantStrings, "\n"))
}


// TestNewProcessor validates the constructor's behavior, particularly default settings.
func TestNewProcessor(t *testing.T) {
	testCases := []struct {
		name                string
		mappings            []config.MappingRule
		dedupCfg            *config.DedupConfig
		errorHandling       *config.ErrorHandlingConfig
		errorWriter         etlio.ErrorWriter
		wantDedupStrategy   string
		wantErrorMode       string
		wantLogErrorDefault bool
	}{
		{ name: "Nil configs", mappings: []config.MappingRule{{Source: "a", Target: "b"}}, dedupCfg: nil, errorHandling: nil, errorWriter: nil, wantDedupStrategy: "", wantErrorMode: config.ErrorHandlingModeHalt, wantLogErrorDefault: false, },
		{ name: "Dedup with no strategy", mappings: nil, dedupCfg: &config.DedupConfig{ Keys: []string{"id"}, }, errorHandling: nil, errorWriter: nil, wantDedupStrategy: config.DefaultDedupStrategy, wantErrorMode: config.ErrorHandlingModeHalt, wantLogErrorDefault: false, },
		{ name: "Dedup with explicit strategy", mappings: nil, dedupCfg: &config.DedupConfig{ Keys: []string{"id"}, Strategy: config.DedupStrategyLast, }, errorHandling: nil, errorWriter: nil, wantDedupStrategy: config.DedupStrategyLast, wantErrorMode: config.ErrorHandlingModeHalt, wantLogErrorDefault: false, },
		{ name: "Error handling skip, logErrors nil", mappings: nil, dedupCfg: nil, errorHandling: &config.ErrorHandlingConfig{ Mode: config.ErrorHandlingModeSkip, }, errorWriter: nil, wantDedupStrategy: "", wantErrorMode: config.ErrorHandlingModeSkip, wantLogErrorDefault: true, },
		{ name: "Error handling skip, logErrors false", mappings: nil, dedupCfg: nil, errorHandling: &config.ErrorHandlingConfig{ Mode: config.ErrorHandlingModeSkip, LogErrors: func() *bool { b := false; return &b }(), }, errorWriter: nil, wantDedupStrategy: "", wantErrorMode: config.ErrorHandlingModeSkip, wantLogErrorDefault: false, },
		{ name: "Error handling halt, logErrors nil", mappings: nil, dedupCfg: nil, errorHandling: &config.ErrorHandlingConfig{ Mode: config.ErrorHandlingModeHalt, }, errorWriter: nil, wantDedupStrategy: "", wantErrorMode: config.ErrorHandlingModeHalt, wantLogErrorDefault: false, },
		{ name: "Error handling mode empty", mappings: nil, dedupCfg: nil, errorHandling: &config.ErrorHandlingConfig{}, errorWriter: nil, wantDedupStrategy: "", wantErrorMode: config.ErrorHandlingModeHalt, wantLogErrorDefault: false, },
		{ name: "With mock error writer", mappings: []config.MappingRule{{Source: "x", Target: "y"}}, dedupCfg: nil, errorHandling: nil, errorWriter: &mockErrorWriter{}, wantDedupStrategy: "", wantErrorMode: config.ErrorHandlingModeHalt, wantLogErrorDefault: false, },
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pInterface := NewProcessor(tc.mappings, tc.dedupCfg, tc.errorHandling, tc.errorWriter)
			p, ok := pInterface.(*processorImpl) // Keep the assertion to the concrete type
			if !ok { t.Fatalf("NewProcessor returned unexpected type %T", pInterface) }

			if !reflect.DeepEqual(p.mappings, tc.mappings) { t.Errorf("processorImpl mappings mismatch") }
			if tc.dedupCfg != nil {
				if p.dedupCfg == nil { t.Errorf("processorImpl dedupCfg is nil") } else if p.dedupCfg.Strategy != tc.wantDedupStrategy { t.Errorf("processorImpl dedup strategy mismatch: got %q, want %q", p.dedupCfg.Strategy, tc.wantDedupStrategy) }
			} else if p.dedupCfg != nil { t.Errorf("processorImpl dedupCfg is non-nil") }
			if p.errorHandling == nil { t.Fatalf("processorImpl errorHandling is nil") }
			if p.errorHandling.Mode != tc.wantErrorMode { t.Errorf("processorImpl error mode mismatch: got %q, want %q", p.errorHandling.Mode, tc.wantErrorMode) }

			// Corrected check for LogErrors handling nil tc.errorHandling
			var originalLogErrors *bool
			if tc.errorHandling != nil { // Check if input errorHandling was provided
				originalLogErrors = tc.errorHandling.LogErrors
			}

			if tc.wantLogErrorDefault {
				if p.errorHandling.LogErrors == nil || !*p.errorHandling.LogErrors {
					t.Errorf("processorImpl LogErrors: got %v, want true (defaulted)", p.errorHandling.LogErrors)
				}
			} else {
				// Check if the processor's LogErrors matches the original (or is appropriately nil)
				if (p.errorHandling.LogErrors == nil && originalLogErrors != nil) ||
				   (p.errorHandling.LogErrors != nil && originalLogErrors == nil) ||
				   (p.errorHandling.LogErrors != nil && originalLogErrors != nil && *p.errorHandling.LogErrors != *originalLogErrors) {
					wantValStr := "nil"; if originalLogErrors != nil { wantValStr = fmt.Sprintf("%t", *originalLogErrors) }
					gotValStr := "nil"; if p.errorHandling.LogErrors != nil { gotValStr = fmt.Sprintf("%t", *p.errorHandling.LogErrors) }
					t.Errorf("LogErrors mismatch: got %s, want %s", gotValStr, wantValStr)
				}
			}

			if p.errorWriter != tc.errorWriter { t.Errorf("processorImpl errorWriter mismatch") }
		})
	}
}


// TestProcessRecords tests the core processing logic.
func TestProcessRecords(t *testing.T) {
	originalFIPS := transform.IsFIPSMode(); defer transform.SetFIPSMode(originalFIPS); transform.SetFIPSMode(false)
	basicMappings := []config.MappingRule{ {Source: "id", Target: "output_id"}, {Source: "name", Target: "full_name", Transform: "toUpperCase"}, {Source: "value", Target: "numeric_value", Transform: "toInt"}, }
	validationMappings := []config.MappingRule{ {Source: "email", Target: "email", Transform: "validateRegex", Params: map[string]interface{}{"pattern": `\w+@\w+\.\w+`}}, {Source: "status", Target: "status", Transform: "validateRequired"}, {Source: "age", Target: "age", Transform: "validateNumericRange", Params: map[string]interface{}{"min": 0, "max": 120}}, }
	strictMappings := []config.MappingRule{ {Source: "id", Target: "id", Transform: "mustToInt"}, {Source: "amount", Target: "amount", Transform: "mustToFloat"}, }

	// --- CORRECTED Dedup Configs ---
	dedupConfigFirst := &config.DedupConfig{Keys: []string{"k"}, Strategy: config.DedupStrategyFirst}                                    // Changed "key" to "k"
	dedupConfigLast := &config.DedupConfig{Keys: []string{"k"}, Strategy: config.DedupStrategyLast}                                     // Changed "key" to "k"
	dedupConfigMin := &config.DedupConfig{Keys: []string{"k"}, Strategy: config.DedupStrategyMin, StrategyField: "v"}                    // Changed "key" to "k", "value" to "v"
	dedupConfigMax := &config.DedupConfig{Keys: []string{"k"}, Strategy: config.DedupStrategyMax, StrategyField: "timestamp"}           // Changed "key" to "k". StrategyField "timestamp" is correct due to mapping.
	dedupConfigMultiKey := &config.DedupConfig{Keys: []string{"k1", "k2"}, Strategy: config.DedupStrategyFirst} // Keys "k1", "k2" are correct for the test data.
	// --- End CORRECTIONS ---

	trueVal := true; falseVal := false
	errorHandlingHalt := &config.ErrorHandlingConfig{Mode: config.ErrorHandlingModeHalt}
	errorHandlingSkipLog := &config.ErrorHandlingConfig{Mode: config.ErrorHandlingModeSkip, LogErrors: &trueVal}
	errorHandlingSkipNoLog := &config.ErrorHandlingConfig{Mode: config.ErrorHandlingModeSkip, LogErrors: &falseVal}
	mockWriter := &mockErrorWriter{}

	testCases := []struct {
		name           string; mappings []config.MappingRule; dedupCfg *config.DedupConfig; errorHandling *config.ErrorHandlingConfig; useErrorWriter bool; writerSetup func(*mockErrorWriter); inputRecords []map[string]interface{}; wantRecords []map[string]interface{}; wantErr bool; wantErrMsg string; wantErrorCount int64; wantWriteCalls int; checkWrites func(*testing.T, *mockErrorWriter)
	}{
		{ name: "Basic mapping success", mappings: basicMappings, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"id": "1", "name": "Test One", "value": "100", "extra": "ignored"}, {"id": "2", "name": "Test two", "value": 200.0}, }, wantRecords: []map[string]interface{}{ {"output_id": "1", "full_name": "TEST ONE", "numeric_value": int64(100)}, {"output_id": "2", "full_name": "TEST TWO", "numeric_value": int64(200)}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Mapping chain success", mappings: []config.MappingRule{ {Source: "raw_value", Target: "temp_int", Transform: "toInt"}, {Source: "temp_int", Target: "final_value"}, }, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"raw_value": "55"}, }, wantRecords: []map[string]interface{}{ {"temp_int": int64(55), "final_value": int64(55)}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Mapping source missing (uses nil)", mappings: []config.MappingRule{ {Source: "missing_field", Target: "output", Transform: "toUpperCase"}, }, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"id": 1}, }, wantRecords: []map[string]interface{}{ {"output": nil}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Validation pass", mappings: validationMappings, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, }, wantRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Validation fail (Halt Mode)", mappings: validationMappings, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "invalid-email", "status": "active", "age": 40}, }, wantRecords: nil, wantErr: true, wantErrMsg: "error processing record 1 (halting): validation failed for rule #0 ('email' -> 'email', transform: 'validateRegex'): value \"invalid-email\" does not match required pattern", wantErrorCount: 1, wantWriteCalls: 0, }, // Shortened err msg check
		{ name: "Validation fail (Skip Mode - Log)", mappings: validationMappings, errorHandling: errorHandlingSkipLog, useErrorWriter: false, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "good@email.org", "status": "", "age": 40}, {"email": "ok@domain.net", "status": "active", "age": 150}, {"email": "final@test.io", "status": "active", "age": 50}, }, wantRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "final@test.io", "status": "active", "age": 50}, }, wantErr: false, wantErrorCount: 2, wantWriteCalls: 0, },
		{ name: "Validation fail (Skip Mode - No Log)", mappings: validationMappings, errorHandling: errorHandlingSkipNoLog, useErrorWriter: false, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "invalid", "status": "active", "age": 40}, }, wantRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, }, wantErr: false, wantErrorCount: 1, wantWriteCalls: 0, },
		{ name: "Validation fail (Skip Mode - With Error Writer)", mappings: validationMappings, errorHandling: errorHandlingSkipLog, useErrorWriter: true, writerSetup: nil, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "invalid", "status": "active", "age": 40}, {"email": "ok@domain.net", "status": "ok", "age": -5}, }, wantRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, }, wantErr: false, wantErrorCount: 2, wantWriteCalls: 2, checkWrites: func(t *testing.T, mw *mockErrorWriter) { if len(mw.writeCalls) != 2 { t.Fatalf("W#!=2") }; if !reflect.DeepEqual(mw.writeCalls[0].Record["email"], "invalid") {t.Error("W0 rec")}; if !strings.Contains(mw.writeCalls[0].Err.Error(),"validateRegex") {t.Error("W0 err")}; if !reflect.DeepEqual(mw.writeCalls[1].Record["email"], "ok@domain.net") {t.Error("W1 rec")}; if !strings.Contains(mw.writeCalls[1].Err.Error(),"validateNumericRange") {t.Error("W1 err")} }, },
		{ name: "Validation fail (Skip Mode - Error Writer Fails)", mappings: validationMappings, errorHandling: errorHandlingSkipLog, useErrorWriter: true, writerSetup: func(m *mockErrorWriter) { m.writeShouldFail = true }, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "invalid", "status": "active", "age": 40}, }, wantRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, }, wantErr: false, wantErrorCount: 1, wantWriteCalls: 1, checkWrites: func(t *testing.T, mw *mockErrorWriter) { if len(mw.writeCalls) != 1 || !reflect.DeepEqual(mw.writeCalls[0].Record["email"], "invalid") { t.Errorf("Expected write fail for 'invalid'") } }, },
		{ name: "Strict transform fail (Halt Mode)", mappings: strictMappings, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"id": "10", "amount": "50.5"}, {"id": "twenty", "amount": "60.0"}, }, wantRecords: nil, wantErr: true, wantErrMsg: "mustToInt: conversion failed for input 'twenty'", wantErrorCount: 1, wantWriteCalls: 0, }, // Shortened err msg check
		{ name: "Strict transform fail (Skip Mode)", mappings: strictMappings, errorHandling: errorHandlingSkipLog, useErrorWriter: false, inputRecords: []map[string]interface{}{ {"id": "10", "amount": "50.5"}, {"id": "20", "amount": "sixty"}, {"id": "30", "amount": "70.7"}, }, wantRecords: []map[string]interface{}{ {"id": int64(10), "amount": float64(50.5)}, {"id": int64(30), "amount": float64(70.7)}, }, wantErr: false, wantErrorCount: 1, wantWriteCalls: 0, },
		{ name: "Deduplication (First)", mappings: []config.MappingRule{{Source: "k",Target:"k"},{Source:"v",Target:"v"}}, dedupCfg: dedupConfigFirst, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"k":"A", "v":1},{"k":"B", "v":2},{"k":"A", "v":3},{"k":"C", "v":4},{"k":"B", "v":5}, }, wantRecords: []map[string]interface{}{ {"k":"A", "v":1},{"k":"B", "v":2},{"k":"C", "v":4}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Deduplication (Last)", mappings: []config.MappingRule{{Source: "k",Target:"k"},{Source:"v",Target:"v"}}, dedupCfg: dedupConfigLast, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"k":"A", "v":1},{"k":"B", "v":2},{"k":"A", "v":3},{"k":"C", "v":4},{"k":"B", "v":5}, }, wantRecords: []map[string]interface{}{ {"k":"A", "v":3},{"k":"B", "v":5},{"k":"C", "v":4}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Deduplication (Min)", mappings: []config.MappingRule{{Source: "k",Target:"k"},{Source:"v",Target:"v"}}, dedupCfg: dedupConfigMin, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"k":"A","v":10},{"k":"B","v":200},{"k":"A","v":5},{"k":"C","v":40},{"k":"B","v":50},{"k":"A","v":15}, }, wantRecords: []map[string]interface{}{ {"k":"A","v":5},{"k":"B","v":50},{"k":"C","v":40}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Deduplication (Max - Time)", mappings: []config.MappingRule{{Source: "k",Target:"k"},{Source:"t",Target:"timestamp"}}, dedupCfg: dedupConfigMax, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"k":"A","t":time.Date(2023,1,1,10,0,0,0,time.UTC)},{"k":"B","t":time.Date(2023,1,1,11,0,0,0,time.UTC)},{"k":"A","t":time.Date(2023,1,1,12,0,0,0,time.UTC)},{"k":"C","t":time.Date(2023,1,1,9,0,0,0,time.UTC)},{"k":"B","t":time.Date(2023,1,1,10,30,0,0,time.UTC)}, }, wantRecords: []map[string]interface{}{ {"k":"A","timestamp":time.Date(2023,1,1,12,0,0,0,time.UTC)},{"k":"B","timestamp":time.Date(2023,1,1,11,0,0,0,time.UTC)},{"k":"C","timestamp":time.Date(2023,1,1,9,0,0,0,time.UTC)}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Deduplication (Multi-key)", mappings: []config.MappingRule{{Source:"k1",Target:"k1"},{Source:"k2",Target:"k2"},{Source:"v",Target:"v"}}, dedupCfg: dedupConfigMultiKey, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"k1":"A","k2":1,"v":"first"},{"k1":"A","k2":2,"v":"second"},{"k1":"B","k2":1,"v":"third"},{"k1":"A","k2":1,"v":"fourth"},{"k1":"B","k2":2,"v":"fifth"}, }, wantRecords: []map[string]interface{}{ {"k1":"A","k2":1,"v":"first"},{"k1":"A","k2":2,"v":"second"},{"k1":"B","k2":1,"v":"third"},{"k1":"B","k2":2,"v":"fifth"}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Empty input records", mappings: basicMappings, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{}, wantRecords: []map[string]interface{}{}, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "No mappings defined", mappings: []config.MappingRule{}, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"id": 1}}, wantRecords: []map[string]interface{}{ {} }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockWriter.reset(); var writerForProcessor etlio.ErrorWriter
			if tc.useErrorWriter { writerForProcessor = mockWriter; if tc.writerSetup != nil { tc.writerSetup(mockWriter) } }
			p := NewProcessor(tc.mappings, tc.dedupCfg, tc.errorHandling, writerForProcessor) // Returns Processor interface
			gotRecords, gotErr := p.ProcessRecords(tc.inputRecords)
			gotErrorCount := p.GetErrorCount() // Use interface method
			gotWriteCalls := len(mockWriter.writeCalls) // Use corrected field name

			if tc.wantErr { if gotErr == nil { t.Fatalf("Err=nil, want %q", tc.wantErrMsg) }; if tc.wantErrMsg != "" && !strings.Contains(gotErr.Error(), tc.wantErrMsg) { t.Errorf("Err mismatch:\ngot: %q\nwant: %q", gotErr.Error(), tc.wantErrMsg) }
			} else { if gotErr != nil { t.Fatalf("Unexpected err: %v", gotErr) } }
			if !tc.wantErr || (tc.errorHandling != nil && tc.errorHandling.Mode == config.ErrorHandlingModeSkip) { if !recordsEqualIgnoringOrder(gotRecords, tc.wantRecords) { t.Errorf("Records mismatch:"); printRecordsDiff(t, gotRecords, tc.wantRecords) } }
			if gotErrorCount != tc.wantErrorCount { t.Errorf("Error count = %d, want %d", gotErrorCount, tc.wantErrorCount) }
			if tc.useErrorWriter { if gotWriteCalls != tc.wantWriteCalls { t.Errorf("Writer calls = %d, want %d", gotWriteCalls, tc.wantWriteCalls) }; if tc.checkWrites != nil { tc.checkWrites(t, mockWriter) }
			} else { if gotWriteCalls > 0 { t.Errorf("Writer calls = %d, want 0", gotWriteCalls) } }
		})
	}
}
