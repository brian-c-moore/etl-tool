package processor

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	// "time" // Still not used

	"etl-tool/internal/config"
	etlio "etl-tool/internal/io" // Use aliased import for internal io package
	"etl-tool/internal/transform"
)

// --- Mocks and Helpers ---
type mockErrorWriter struct { mu sync.Mutex; writeCalls []struct { Record map[string]interface{}; Err error }; closeCalls int; writeShouldFail, closeShouldFail bool }
func (m *mockErrorWriter) Write(record map[string]interface{}, processError error) error { m.mu.Lock(); defer m.mu.Unlock(); recordCopy := make(map[string]interface{}); for k, v := range record { recordCopy[k] = v }; m.writeCalls = append(m.writeCalls, struct { Record map[string]interface{}; Err error }{recordCopy, processError}); if m.writeShouldFail { return errors.New("mock write error") }; return nil }
func (m *mockErrorWriter) Close() error { m.mu.Lock(); defer m.mu.Unlock(); m.closeCalls++; if m.closeShouldFail { return errors.New("mock close error") }; return nil }
func (m *mockErrorWriter) reset() { m.mu.Lock(); defer m.mu.Unlock(); m.writeCalls = nil; m.closeCalls = 0; m.writeShouldFail = false; m.closeShouldFail = false }
func canonicalMapString(m map[string]interface{}) string { if m == nil { return "<nil_map>" }; keys := make([]string, 0, len(m)); for k := range m { keys = append(keys, k) }; sort.Strings(keys); var sb strings.Builder; sb.WriteString("{"); for i, k := range keys { fmt.Fprintf(&sb, "%q:%#v", k, m[k]); if i < len(keys)-1 { sb.WriteString(",") } }; sb.WriteString("}"); return sb.String() }
func recordsEqualIgnoringOrder(got, want []map[string]interface{}) bool { if len(got) != len(want) { return false }; if len(got) == 0 { return true }; gotFreq := make(map[string]int); wantFreq := make(map[string]int); for _, r := range got { gotFreq[canonicalMapString(r)]++ }; for _, r := range want { wantFreq[canonicalMapString(r)]++ }; return reflect.DeepEqual(gotFreq, wantFreq) }
func printRecordsDiff(t *testing.T, got, want []map[string]interface{}) { t.Helper(); gotStrings := make([]string, len(got)); wantStrings := make([]string, len(want)); for i, r := range got { gotStrings[i] = canonicalMapString(r) }; for i, r := range want { wantStrings[i] = canonicalMapString(r) }; sort.Strings(gotStrings); sort.Strings(wantStrings); t.Logf("GOT Records (%d):\n%s", len(got), strings.Join(gotStrings, "\n")); t.Logf("WANT Records (%d):\n%s", len(want), strings.Join(wantStrings, "\n")) }

// TestNewProcessor validates the constructor's behavior, particularly default settings.
func TestNewProcessor(t *testing.T) { boolPtr := func(b bool) *bool { return &b }; testCases := []struct { name string; mappings []config.MappingRule; flatteningCfg *config.FlatteningConfig; dedupCfg *config.DedupConfig; errorHandling *config.ErrorHandlingConfig; errorWriter etlio.ErrorWriter; wantDedupStrategy string; wantErrorMode string; wantLogErrorDefault bool; wantFlattenIncParent *bool; wantFlattenErrNonList *bool }{ { name: "Nil configs", mappings: []config.MappingRule{{Source: "a", Target: "b"}}, flatteningCfg: nil, dedupCfg: nil, errorHandling: nil, errorWriter: nil, wantDedupStrategy: "", wantErrorMode: config.ErrorHandlingModeHalt, wantLogErrorDefault: false, wantFlattenIncParent: nil, wantFlattenErrNonList: nil, }, { name: "Dedup with no strategy", mappings: nil, flatteningCfg: nil, dedupCfg: &config.DedupConfig{Keys: []string{"id"}}, errorHandling: nil, errorWriter: nil, wantDedupStrategy: config.DefaultDedupStrategy, wantErrorMode: config.ErrorHandlingModeHalt, wantLogErrorDefault: false, wantFlattenIncParent: nil, wantFlattenErrNonList: nil, }, { name: "Error handling skip, logErrors nil", mappings: nil, flatteningCfg: nil, dedupCfg: nil, errorHandling: &config.ErrorHandlingConfig{Mode: config.ErrorHandlingModeSkip}, errorWriter: nil, wantDedupStrategy: "", wantErrorMode: config.ErrorHandlingModeSkip, wantLogErrorDefault: true, }, { name: "Flattening config defaults", mappings: []config.MappingRule{}, flatteningCfg: &config.FlatteningConfig{ SourceField: "list", TargetField: "item", }, dedupCfg: nil, errorHandling: nil, errorWriter: nil, wantErrorMode: config.ErrorHandlingModeHalt, wantFlattenIncParent: boolPtr(true), wantFlattenErrNonList: boolPtr(false), }, { name: "Flattening config explicit", mappings: []config.MappingRule{}, flatteningCfg: &config.FlatteningConfig{ SourceField: "list", TargetField: "item", IncludeParent: boolPtr(false), ErrorOnNonList: boolPtr(true), }, dedupCfg: nil, errorHandling: nil, errorWriter: nil, wantErrorMode: config.ErrorHandlingModeHalt, wantFlattenIncParent: boolPtr(false), wantFlattenErrNonList: boolPtr(true), }, }; for _, tc := range testCases { t.Run(tc.name, func(t *testing.T) { pInterface := NewProcessor(tc.mappings, tc.flatteningCfg, tc.dedupCfg, tc.errorHandling, tc.errorWriter); p, ok := pInterface.(*processorImpl); if !ok { t.Fatalf("NewProcessor returned unexpected type %T", pInterface) }; if !reflect.DeepEqual(p.mappings, tc.mappings) { t.Errorf("processorImpl mappings mismatch") }; if tc.flatteningCfg != nil { if p.flatteningCfg == nil { t.Errorf("processorImpl flatteningCfg is nil, want non-nil") } else { if p.flatteningCfg.SourceField != tc.flatteningCfg.SourceField { t.Errorf("Flatten SourceField mismatch") }; if p.flatteningCfg.TargetField != tc.flatteningCfg.TargetField { t.Errorf("Flatten TargetField mismatch") }; if !reflect.DeepEqual(p.flatteningCfg.IncludeParent, tc.wantFlattenIncParent) { t.Errorf("Flatten IncludeParent mismatch: got %v, want %v", p.flatteningCfg.IncludeParent, tc.wantFlattenIncParent) }; if !reflect.DeepEqual(p.flatteningCfg.ErrorOnNonList, tc.wantFlattenErrNonList) { t.Errorf("Flatten ErrorOnNonList mismatch: got %v, want %v", p.flatteningCfg.ErrorOnNonList, tc.wantFlattenErrNonList) }; if p.flatteningCfg.ConditionField != tc.flatteningCfg.ConditionField { t.Errorf("Flatten ConditionField mismatch") }; if p.flatteningCfg.ConditionValue != tc.flatteningCfg.ConditionValue { t.Errorf("Flatten ConditionValue mismatch") } } } else if p.flatteningCfg != nil { t.Errorf("processorImpl flatteningCfg is non-nil, want nil") }; if tc.dedupCfg != nil { if p.dedupCfg == nil { t.Errorf("processorImpl dedupCfg is nil") } else if p.dedupCfg.Strategy != tc.wantDedupStrategy { t.Errorf("processorImpl dedup strategy mismatch: got %q, want %q", p.dedupCfg.Strategy, tc.wantDedupStrategy) } } else if p.dedupCfg != nil { t.Errorf("processorImpl dedupCfg is non-nil") }; if p.errorHandling == nil { t.Fatalf("processorImpl errorHandling is nil") }; if p.errorHandling.Mode != tc.wantErrorMode { t.Errorf("processorImpl error mode mismatch: got %q, want %q", p.errorHandling.Mode, tc.wantErrorMode) }; var originalLogErrors *bool; if tc.errorHandling != nil { originalLogErrors = tc.errorHandling.LogErrors }; if tc.wantLogErrorDefault { if p.errorHandling.LogErrors == nil || !*p.errorHandling.LogErrors { t.Errorf("processorImpl LogErrors: got %v, want true (defaulted)", p.errorHandling.LogErrors) } } else { if !reflect.DeepEqual(p.errorHandling.LogErrors, originalLogErrors) { t.Errorf("LogErrors mismatch: got %v, want %v", p.errorHandling.LogErrors, originalLogErrors) } }; if p.errorWriter != tc.errorWriter { t.Errorf("processorImpl errorWriter mismatch") } }) } }

// TestProcessRecords tests the core processing logic including flattening.
func TestProcessRecords(t *testing.T) {
	originalFIPS := transform.IsFIPSMode(); defer transform.SetFIPSMode(originalFIPS); transform.SetFIPSMode(false)
	basicMappings := []config.MappingRule{ {Source: "id", Target: "output_id"}, {Source: "name", Target: "full_name", Transform: "toUpperCase"}, {Source: "value", Target: "numeric_value", Transform: "toInt"}, }
	validationMappings := []config.MappingRule{ {Source: "email", Target: "email", Transform: "validateRegex", Params: map[string]interface{}{"pattern": `\w+@\w+\.\w+`}}, {Source: "status", Target: "status", Transform: "validateRequired"}, {Source: "age", Target: "age", Transform: "validateNumericRange", Params: map[string]interface{}{"min": 0, "max": 120}}, }

	dedupConfigFirst := &config.DedupConfig{Keys: []string{"k"}, Strategy: config.DedupStrategyFirst}

	// Flattening Configs
	boolPtr := func(b bool) *bool { return &b }
	flattenSimple := &config.FlatteningConfig{ SourceField: "items", TargetField: "item", IncludeParent: boolPtr(true), ErrorOnNonList: boolPtr(false), }
	flattenNested := &config.FlatteningConfig{ SourceField: "details.addresses", TargetField: "address", IncludeParent: boolPtr(true), }
	flattenNoParent := &config.FlatteningConfig{ SourceField: "tags", TargetField: "tag", IncludeParent: boolPtr(false), }
	flattenError := &config.FlatteningConfig{ SourceField: "items", TargetField: "item", ErrorOnNonList: boolPtr(true), }
	flattenCond := &config.FlatteningConfig{ SourceField: "ips", TargetField: "ip", ConditionField: "process", ConditionValue: "yes", }

	trueVal := true
	errorHandlingHalt := &config.ErrorHandlingConfig{Mode: config.ErrorHandlingModeHalt}
	errorHandlingSkipLog := &config.ErrorHandlingConfig{Mode: config.ErrorHandlingModeSkip, LogErrors: &trueVal}
	mockWriter := &mockErrorWriter{}

	testCases := []struct {
		name           string
		mappings       []config.MappingRule
		flatteningCfg  *config.FlatteningConfig
		dedupCfg       *config.DedupConfig
		errorHandling  *config.ErrorHandlingConfig
		useErrorWriter bool
		writerSetup    func(*mockErrorWriter)
		inputRecords   []map[string]interface{}
		wantRecords    []map[string]interface{}
		wantErr        bool
		wantErrMsg     string
		wantErrorCount int64
		wantWriteCalls int
		checkWrites    func(*testing.T, *mockErrorWriter)
	}{
		// --- Mapping/Validation/Dedup Tests (No Flattening) ---
		{ name: "Basic mapping success", mappings: basicMappings, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"id": "1", "name": "Test One", "value": "100", "extra": "ignored"}, {"id": "2", "name": "Test two", "value": 200.0}, }, wantRecords: []map[string]interface{}{ {"output_id": "1", "full_name": "TEST ONE", "numeric_value": int64(100)}, {"output_id": "2", "full_name": "TEST TWO", "numeric_value": int64(200)}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Mapping chain success", mappings: []config.MappingRule{ {Source: "raw_value", Target: "temp_int", Transform: "toInt"}, {Source: "temp_int", Target: "final_value"}, }, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"raw_value": "55"}, }, wantRecords: []map[string]interface{}{ {"temp_int": int64(55), "final_value": int64(55)}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Mapping source missing (uses nil)", mappings: []config.MappingRule{ {Source: "missing_field", Target: "output", Transform: "toUpperCase"}, }, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"id": 1}, }, wantRecords: []map[string]interface{}{ {"output": nil}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Validation pass", mappings: validationMappings, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, }, wantRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Validation fail (Halt Mode)", mappings: validationMappings, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "invalid-email", "status": "active", "age": 40}, }, wantRecords: nil, wantErr: true, wantErrMsg: "error processing record 1 (mapping, halting)", wantErrorCount: 1, wantWriteCalls: 0, },
		{ name: "Validation fail (Skip Mode - Log)", mappings: validationMappings, errorHandling: errorHandlingSkipLog, useErrorWriter: false, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "good@email.org", "status": "", "age": 40}, {"email": "ok@domain.net", "status": "active", "age": 150}, {"email": "final@test.io", "status": "active", "age": 50}, }, wantRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "final@test.io", "status": "active", "age": 50}, }, wantErr: false, wantErrorCount: 2, wantWriteCalls: 0, },
		{ name: "Validation fail (Skip Mode - With Error Writer)", mappings: validationMappings, errorHandling: errorHandlingSkipLog, useErrorWriter: true, writerSetup: nil, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "invalid", "status": "active", "age": 40}, {"email": "ok@domain.net", "status": "ok", "age": -5}, }, wantRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, }, wantErr: false, wantErrorCount: 2, wantWriteCalls: 2, checkWrites: func(t *testing.T, mw *mockErrorWriter) { if len(mw.writeCalls) != 2 { t.Fatalf("W#!=2") }; if !reflect.DeepEqual(mw.writeCalls[0].Record["email"], "invalid") {t.Error("W0 rec")}; if !strings.Contains(mw.writeCalls[0].Err.Error(),"validateRegex") {t.Error("W0 err")}; if !reflect.DeepEqual(mw.writeCalls[1].Record["email"], "ok@domain.net") {t.Error("W1 rec")}; if !strings.Contains(mw.writeCalls[1].Err.Error(),"validateNumericRange") {t.Error("W1 err")} }, },
		{ name: "Validation fail (Skip Mode - Error Writer Fails)", mappings: validationMappings, errorHandling: errorHandlingSkipLog, useErrorWriter: true, writerSetup: func(m *mockErrorWriter) { m.writeShouldFail = true }, inputRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, {"email": "invalid", "status": "active", "age": 40}, }, wantRecords: []map[string]interface{}{ {"email": "test@example.com", "status": "active", "age": 30}, }, wantErr: false, wantErrorCount: 1, wantWriteCalls: 1, checkWrites: func(t *testing.T, mw *mockErrorWriter) { if len(mw.writeCalls) != 1 || !reflect.DeepEqual(mw.writeCalls[0].Record["email"], "invalid") { t.Errorf("Expected write fail for 'invalid'") } }, },
		{ name: "Deduplication (First)", mappings: []config.MappingRule{{Source: "k",Target:"k"},{Source:"v",Target:"v"}}, dedupCfg: dedupConfigFirst, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"k":"A", "v":1},{"k":"B", "v":2},{"k":"A", "v":3},{"k":"C", "v":4},{"k":"B", "v":5}, }, wantRecords: []map[string]interface{}{ {"k":"A", "v":1},{"k":"B", "v":2},{"k":"C", "v":4}, }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "Empty input records", mappings: basicMappings, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{}, wantRecords: []map[string]interface{}{}, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },
		{ name: "No mappings defined", mappings: []config.MappingRule{}, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"id": 1}}, wantRecords: []map[string]interface{}{ {} }, wantErr: false, wantErrorCount: 0, wantWriteCalls: 0, },

		// --- Flattening Tests ---
		{ name: "Flatten Simple List (IncludeParent=true)", mappings: []config.MappingRule{{Source: "id", Target: "id"}, {Source: "items", Target: "items"}}, flatteningCfg: flattenSimple, errorHandling: errorHandlingHalt, inputRecords:  []map[string]interface{}{ {"id": 1, "items": []string{"A", "B"}}, {"id": 2, "items": []string{"C"}}, }, wantRecords: []map[string]interface{}{ {"id": 1, "item": "A"}, {"id": 1, "item": "B"}, {"id": 2, "item": "C"}, }, wantErr: false, wantErrorCount: 0, },
		// *** CORRECTED EXPECTATION for Flatten_Nested_List ***
		{
			name:          "Flatten Nested List (Dot Notation)",
			mappings:      []config.MappingRule{{Source: "user", Target: "user"}, {Source: "details", Target: "details"}},
			flatteningCfg: flattenNested, // SourceField: "details.addresses"
			errorHandling: errorHandlingHalt,
			inputRecords: []map[string]interface{}{
				{"user": "U1", "details": map[string]interface{}{"city": "A", "addresses": []string{"1 Main", "2 Oak"}}},
				{"user": "U2", "details": map[string]interface{}{"city": "B", "addresses": []string{"3 Elm"}}},
			},
			wantRecords: []map[string]interface{}{
				// Parent 'details' map is copied, but 'addresses' key within it is removed
				{"user": "U1", "details": map[string]interface{}{"city": "A"}, "address": "1 Main"},
				{"user": "U1", "details": map[string]interface{}{"city": "A"}, "address": "2 Oak"},
				{"user": "U2", "details": map[string]interface{}{"city": "B"}, "address": "3 Elm"},
			},
			wantErr: false, wantErrorCount: 0,
		},
		// *** END CORRECTION ***
		{ name: "Flatten List (IncludeParent=false)", mappings: []config.MappingRule{{Source: "id", Target: "id"}, {Source: "tags", Target: "tags"}}, flatteningCfg: flattenNoParent, errorHandling: errorHandlingHalt, inputRecords:  []map[string]interface{}{ {"id": 10, "tags": []int{100, 200}}, }, wantRecords: []map[string]interface{}{ {"tag": 100}, {"tag": 200}, }, wantErr: false, wantErrorCount: 0, },
		{ name: "Flatten Skip Non-List (ErrorOnNonList=false)", mappings: []config.MappingRule{{Source: "id", Target: "id"}, {Source: "items", Target: "items"}}, flatteningCfg: flattenSimple, errorHandling: errorHandlingHalt, inputRecords:  []map[string]interface{}{ {"id": 1, "items": "not-a-list"}, {"id": 2, "items": []string{"A"}}, }, wantRecords: []map[string]interface{}{ {"id": 2, "item": "A"}, }, wantErr: false, wantErrorCount: 0, },
		{ name: "Flatten Error Non-List (ErrorOnNonList=true)", mappings: []config.MappingRule{{Source: "id", Target: "id"}, {Source: "items", Target: "items"}}, flatteningCfg: flattenError, errorHandling: errorHandlingHalt, inputRecords:  []map[string]interface{}{ {"id": 1, "items": "not-a-list"}, {"id": 2, "items": []string{"A"}}, }, wantRecords:   nil, wantErr: true, wantErrMsg: "error processing record 0 (flattening, halting): flattening source field 'items' is not a slice", wantErrorCount: 1, },
		{ name: "Flatten Error Missing Field (ErrorOnNonList=true)", mappings: []config.MappingRule{{Source: "id", Target: "id"}}, flatteningCfg: flattenError, errorHandling: errorHandlingHalt, inputRecords:  []map[string]interface{}{ {"id": 1}, }, wantRecords:   nil, wantErr: true, wantErrMsg: "error processing record 0 (flattening, halting): flattening source field 'items' not found or is nil", wantErrorCount: 1, },
		{ name: "Flatten Conditional - Match", mappings: []config.MappingRule{{Source: "id", Target: "id"}, {Source: "process", Target: "process"}, {Source: "ips", Target: "ips"}}, flatteningCfg: flattenCond, errorHandling: errorHandlingHalt, inputRecords:  []map[string]interface{}{ {"id": 1, "process": "yes", "ips": []string{"1.1.1.1", "2.2.2.2"}}, }, wantRecords: []map[string]interface{}{ {"id": 1, "process": "yes", "ip": "1.1.1.1"}, {"id": 1, "process": "yes", "ip": "2.2.2.2"}, }, wantErr: false, wantErrorCount: 0, },
		{ name: "Flatten Conditional - No Match", mappings: []config.MappingRule{{Source: "id", Target: "id"}, {Source: "process", Target: "process"}, {Source: "ips", Target: "ips"}}, flatteningCfg: flattenCond, errorHandling: errorHandlingHalt, inputRecords:  []map[string]interface{}{ {"id": 1, "process": "no", "ips": []string{"1.1.1.1"}}, }, wantRecords: []map[string]interface{}{ {"id": 1, "process": "no", "ips": []string{"1.1.1.1"}}, }, wantErr: false, wantErrorCount: 0, },
		{ name: "Flatten Empty List", mappings: []config.MappingRule{{Source: "id", Target: "id"}, {Source: "items", Target: "items"}}, flatteningCfg: flattenSimple, errorHandling: errorHandlingHalt, inputRecords:  []map[string]interface{}{ {"id": 1, "items": []string{}}, }, wantRecords:   []map[string]interface{}{}, wantErr: false, wantErrorCount: 0, },
		{ name: "Flatten Skip Error Write", mappings: []config.MappingRule{{Source: "id", Target: "id"}, {Source: "items", Target: "items"}}, flatteningCfg: flattenError, errorHandling: errorHandlingSkipLog, useErrorWriter: true, inputRecords: []map[string]interface{}{ {"id": 1, "items": "not-list"}, {"id": 2, "items": []string{"A"}}, }, wantRecords: []map[string]interface{}{ {"id": 2, "item": "A"}, }, wantErr: false, wantErrorCount: 1, wantWriteCalls: 1, checkWrites: func(t *testing.T, mw *mockErrorWriter) { if len(mw.writeCalls) != 1 { t.Fatalf("W#!=1") }; if !reflect.DeepEqual(mw.writeCalls[0].Record["id"], 1) { t.Error("W0 rec ID")}; if mw.writeCalls[0].Err == nil || !strings.Contains(mw.writeCalls[0].Err.Error(), "not a slice") {t.Error("W0 err msg")} }, },
		{ name: "Flatten then Dedup (First)", mappings: []config.MappingRule{{Source: "id", Target: "id"}, {Source: "vals", Target: "vals"}}, flatteningCfg: &config.FlatteningConfig{SourceField: "vals", TargetField: "k", IncludeParent: boolPtr(true)}, dedupCfg:      &config.DedupConfig{Keys: []string{"k"}, Strategy: config.DedupStrategyFirst}, errorHandling: errorHandlingHalt, inputRecords: []map[string]interface{}{ {"id": 1, "vals": []string{"A", "B"}}, {"id": 2, "vals": []string{"C", "A"}}, }, wantRecords: []map[string]interface{}{ {"id": 1, "k": "A"}, {"id": 1, "k": "B"}, {"id": 2, "k": "C"}, }, wantErr: false, wantErrorCount: 0, },

	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockWriter.reset(); var writerForProcessor etlio.ErrorWriter
			if tc.useErrorWriter { writerForProcessor = mockWriter; if tc.writerSetup != nil { tc.writerSetup(mockWriter) } }
			p := NewProcessor(tc.mappings, tc.flatteningCfg, tc.dedupCfg, tc.errorHandling, writerForProcessor)
			gotRecords, gotErr := p.ProcessRecords(tc.inputRecords)
			gotErrorCount := p.GetErrorCount()
			gotWriteCalls := len(mockWriter.writeCalls)

			if tc.wantErr { if gotErr == nil { t.Fatalf("Err=nil, want %q", tc.wantErrMsg) }; if tc.wantErrMsg != "" && !strings.Contains(gotErr.Error(), tc.wantErrMsg) { t.Errorf("Err mismatch:\ngot: %q\nwant substring: %q", gotErr.Error(), tc.wantErrMsg) }
			} else { if gotErr != nil { t.Fatalf("Unexpected err: %v", gotErr) } }

			if !tc.wantErr || (tc.errorHandling != nil && tc.errorHandling.Mode == config.ErrorHandlingModeSkip) {
				if !recordsEqualIgnoringOrder(gotRecords, tc.wantRecords) {
					t.Errorf("Records mismatch:")
					printRecordsDiff(t, gotRecords, tc.wantRecords)
				}
			} else if len(gotRecords) > 0 && tc.wantErr {
				t.Errorf("Got records back despite expecting halting error:")
				printRecordsDiff(t, gotRecords, tc.wantRecords)
			}

			if gotErrorCount != tc.wantErrorCount { t.Errorf("Error count = %d, want %d", gotErrorCount, tc.wantErrorCount) }
			if tc.useErrorWriter { if gotWriteCalls != tc.wantWriteCalls { t.Errorf("Writer calls = %d, want %d", gotWriteCalls, tc.wantWriteCalls) }; if tc.checkWrites != nil { tc.checkWrites(t, mockWriter) }
			} else { if gotWriteCalls > 0 { t.Errorf("Writer calls = %d, want 0", gotWriteCalls) } }
		})
	}
}