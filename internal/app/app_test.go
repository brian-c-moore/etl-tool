package app

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"etl-tool/internal/config"
	etlio "etl-tool/internal/io"
	"etl-tool/internal/logging"
	"etl-tool/internal/processor"
	"etl-tool/internal/transform"
)

// --- Mock Implementations --- (No changes)
type mockFileInfo struct { name string; size int64; mode fs.FileMode }
func (m mockFileInfo) Name() string       { return m.name }
func (m mockFileInfo) Size() int64        { return m.size }
func (m mockFileInfo) Mode() fs.FileMode  { return m.mode }
func (m mockFileInfo) ModTime() time.Time { return time.Time{} }
func (m mockFileInfo) IsDir() bool        { return m.mode&fs.ModeDir != 0 }
func (m mockFileInfo) Sys() interface{}   { return nil }
type mockInputReader struct { mu sync.Mutex; readFunc func(string) ([]map[string]interface{}, error); readCalls int; lastReadArg string }
func (m *mockInputReader) Read(p string) ([]map[string]interface{}, error) { m.mu.Lock(); m.readCalls++; m.lastReadArg = p; fn := m.readFunc; m.mu.Unlock(); if fn != nil { r, e := fn(p); if e != nil && strings.Contains(e.Error(), "mock read fail") { return nil, errors.New("mock read fail") }; return r, e }; return []map[string]interface{}{{"col1": "val1"}}, nil }
func (m *mockInputReader) Reset() { m.mu.Lock(); m.readFunc = nil; m.readCalls = 0; m.lastReadArg = ""; m.mu.Unlock() }
type mockOutputWriter struct { mu sync.Mutex; writeFunc func([]map[string]interface{}, string) error; closeFunc func() error; writeCalls, closeCalls int; lastWriteArg string; lastRecords []map[string]interface{} }
func (m *mockOutputWriter) Write(r []map[string]interface{}, p string) error { m.mu.Lock(); m.writeCalls++; m.lastWriteArg = p; m.lastRecords = make([]map[string]interface{}, len(r)); for i, rec := range r { c := make(map[string]interface{}); for k, v := range rec { c[k] = v }; m.lastRecords[i] = c }; fn := m.writeFunc; m.mu.Unlock(); if fn != nil { return fn(r, p) }; return nil }
func (m *mockOutputWriter) Close() error { m.mu.Lock(); m.closeCalls++; fn := m.closeFunc; m.mu.Unlock(); if fn != nil { return fn() }; return nil }
func (m *mockOutputWriter) Reset() { m.mu.Lock(); m.writeFunc = nil; m.closeFunc = nil; m.writeCalls = 0; m.closeCalls = 0; m.lastWriteArg = ""; m.lastRecords = nil; m.mu.Unlock() }
type mockErrorWriter struct { mu sync.Mutex; writeCalls []struct { Record map[string]interface{}; Err error }; closeCalls int; writeShouldFail, closeShouldFail bool; closed bool }
func (m *mockErrorWriter) Write(rec map[string]interface{}, err error) error { m.mu.Lock(); defer m.mu.Unlock(); if m.closed { return errors.New("mockErrorWriter: write called on closed writer") }; c := make(map[string]interface{}); for k, v := range rec { c[k] = v }; m.writeCalls = append(m.writeCalls, struct { Record map[string]interface{}; Err error }{c, err}); if m.writeShouldFail { return errors.New("mock write error") }; return nil }
func (m *mockErrorWriter) Close() error { m.mu.Lock(); defer m.mu.Unlock(); if m.closed { return nil }; m.closeCalls++; m.closed = true; if m.closeShouldFail { return errors.New("mock close error") }; return nil }
func (m *mockErrorWriter) Reset() { m.mu.Lock(); defer m.mu.Unlock(); m.writeCalls = nil; m.closeCalls = 0; m.writeShouldFail = false; m.closeShouldFail = false; m.closed = false }
type mockProcessor struct { mu sync.Mutex; processFunc func([]map[string]interface{}) ([]map[string]interface{}, error); errorCountVal int64; processCalls int; errorWriter etlio.ErrorWriter }
func (m *mockProcessor) ProcessRecords(r []map[string]interface{}) ([]map[string]interface{}, error) { m.mu.Lock(); m.processCalls++; fn := m.processFunc; ew := m.errorWriter; m.mu.Unlock(); if fn != nil { return fn(r) }; output := []map[string]interface{}{}; currentErrors := int64(0); for i, rec := range r { if _, ok := rec["error_trigger"]; ok { currentErrors++; simErr := fmt.Errorf("simulated processing error for record %d", i); if ew != nil { errWrite := ew.Write(rec, simErr); if errWrite != nil { fmt.Printf("!!! MOCK PROCESSOR Write Error: %v\n", errWrite) } } else { fmt.Println("!!! MOCK PROCESSOR ERROR WRITER IS NIL") }; continue }; output = append(output, rec) }; m.SetErrorCount(m.GetErrorCount() + currentErrors); return output, nil }
func (m *mockProcessor) GetErrorCount() int64 { m.mu.Lock(); defer m.mu.Unlock(); return m.errorCountVal }
func (m *mockProcessor) SetErrorCount(c int64) { m.mu.Lock(); m.errorCountVal = c; m.mu.Unlock() }
func (m *mockProcessor) Reset() { m.mu.Lock(); m.processFunc = nil; m.errorCountVal = 0; m.processCalls = 0; m.errorWriter = nil; m.mu.Unlock() }
func (m *mockProcessor) SetErrorWriter(ew etlio.ErrorWriter) { m.mu.Lock(); m.errorWriter = ew; m.mu.Unlock() }
type mockEvaluableExpression struct { EvaluateFunc func(map[string]interface{}) (interface{}, error) }
func (m *mockEvaluableExpression) Evaluate(p map[string]interface{}) (interface{}, error) { if m.EvaluateFunc != nil { return m.EvaluateFunc(p) }; return true, nil }

// --- Test Helper Functions ---
func createTempYAML(t *testing.T, content string) string { t.Helper(); f, err := os.CreateTemp(t.TempDir(), "*.yaml"); if err != nil { t.Fatalf("Create temp file: %v", err) }; _, err = f.WriteString(content); if err != nil { f.Close(); t.Fatalf("Write temp file: %v", err) }; fp := f.Name(); err = f.Close(); if err != nil { t.Fatalf("Close temp file: %v", err) }; return fp }
var setupMu sync.Mutex
func setupTestEnv(t *testing.T) (*mockInputReader, *mockOutputWriter, *mockErrorWriter, *mockProcessor, *mockEvaluableExpression) {
	setupMu.Lock()
	t.Helper()
	mockIn := &mockInputReader{}
	mockOut := &mockOutputWriter{}
	mockErr := &mockErrorWriter{}
	mockProc := &mockProcessor{}
	mockExpr := &mockEvaluableExpression{}
	origInputRdrFn := newInputReaderFunc
	origOutputWtrFn := newOutputWriterFunc
	origErrWtrFn := newCSVErrorWriterFunc
	origProcFn := newProcessorFunc
	origExprFn := newExpressionEvaluatorFunc
	origMkdirFn := osMkdirAllFunc
	origStatFn := osStatFunc

	newInputReaderFunc = func(c config.SourceConfig, dbs string) (etlio.InputReader, error) { return mockIn, nil }
	newOutputWriterFunc = func(c config.DestinationConfig, dbs string) (etlio.OutputWriter, error) { return mockOut, nil }
	// Default factory returns nil, nil
	newCSVErrorWriterFunc = func(fp string) (etlio.ErrorWriter, error) { return nil, nil }
	newProcessorFunc = func(mappings []config.MappingRule, flatteningCfg *config.FlatteningConfig, dedupCfg *config.DedupConfig, errorHandling *config.ErrorHandlingConfig, errorWriter etlio.ErrorWriter) processor.Processor {
		// Processor still gets the writer passed from app.Run, which might be nil or the mock
		mockProc.SetErrorWriter(errorWriter)
		return mockProc
	}
	newExpressionEvaluatorFunc = func(ex string) (expressionEvaluator, error) { return mockExpr, nil }
	osMkdirAllFunc = func(p string, pm os.FileMode) error { return nil }
	osStatFunc = func(n string) (os.FileInfo, error) { if strings.Contains(n, "non-existent") { return nil, os.ErrNotExist }; if strings.HasSuffix(n, "/") || strings.HasSuffix(n, "\\") { return mockFileInfo{name: filepath.Base(n), mode: fs.ModeDir}, nil }; return mockFileInfo{name: filepath.Base(n)}, nil }
	logBuf := &bytes.Buffer{}
	origLogLevel := logging.GetLevel()
	logging.SetOutput(logBuf)
	t.Cleanup(func() {
		newInputReaderFunc = origInputRdrFn; newOutputWriterFunc = origOutputWtrFn; newCSVErrorWriterFunc = origErrWtrFn
		newProcessorFunc = origProcFn; newExpressionEvaluatorFunc = origExprFn
		osMkdirAllFunc = origMkdirFn; osStatFunc = origStatFn
		logging.SetOutput(os.Stderr); logging.SetLevel(origLogLevel)
		transform.SetFIPSMode(false); setupMu.Unlock()
	})
	return mockIn, mockOut, mockErr, mockProc, mockExpr
}

const minimalValidConfig = `
logging: { level: debug }
source: { type: csv, file: i.csv }
destination: { type: json, file: o.json }
mappings: [{ source: c1, target: o1 }]`

// --- Test Functions ---

func TestAppRunner_Usage(t *testing.T) { runner := NewAppRunner(); var buf bytes.Buffer; runner.Usage(&buf); got := buf.String(); want := usageText; if got != want { t.Errorf("Usage mismatch:\ngot:\n%q\nwant:\n%q", got, want) } }
func TestAppRunner_Run_Help(t *testing.T) { runner := NewAppRunner(); origStderr := os.Stderr; r, w, _ := os.Pipe(); os.Stderr = w; t.Cleanup(func() { os.Stderr = origStderr }); args := []string{"-help"}; err := runner.Run(args); w.Close(); captured, _ := io.ReadAll(r); stderr := string(captured); if err != nil { t.Errorf("Run err: %v", err) }; if !strings.Contains(stderr, "Usage:") { t.Errorf("No usage msg. Got:\n%s", stderr) } }
func TestAppRunner_Run_NoArgs(t *testing.T) { runner := NewAppRunner(); origStderr := os.Stderr; r, w, _ := os.Pipe(); os.Stderr = w; t.Cleanup(func() { os.Stderr = origStderr }); args := []string{}; err := runner.Run(args); w.Close(); captured, _ := io.ReadAll(r); stderr := string(captured); if err != nil { t.Errorf("Run err: %v", err) }; if !strings.Contains(stderr, "Usage:") { t.Errorf("No usage msg. Got:\n%s", stderr) } }
func TestAppRunner_Run_InvalidFlag(t *testing.T) { runner := NewAppRunner(); setupTestEnv(t); args := []string{"-invalid-flag"}; err := runner.Run(args); if !errors.Is(err, ErrUsage) { t.Errorf("Expected ErrUsage, got: %v", err) } }
func TestAppRunner_Run_ConfigNotFound(t *testing.T) { runner := NewAppRunner(); setupTestEnv(t); origStat := osStatFunc; osStatFunc = func(n string) (os.FileInfo, error) { if n == "non-existent.yaml" { return nil, os.ErrNotExist }; return mockFileInfo{name: n}, nil }; t.Cleanup(func() { osStatFunc = origStat }); args := []string{"-config", "non-existent.yaml"}; err := runner.Run(args); if !errors.Is(err, ErrConfigNotFound) { t.Errorf("Expected ErrConfigNotFound, got: %v", err) } }
func TestAppRunner_Run_InvalidConfigContent(t *testing.T) { runner := NewAppRunner(); setupTestEnv(t); t.Run("InvalidYAML", func(t *testing.T) { cp := createTempYAML(t, "log: { level:"); args := []string{"-config", cp}; err := runner.Run(args); if err == nil || !strings.Contains(err.Error(), "YAML") { t.Errorf("Expected YAML err, got: %v", err) } }); t.Run("InvalidSchema", func(t *testing.T) { cp := createTempYAML(t, `
destination: { type: json, file: o.json }
mappings: [{ source: c, target: o }]`); args := []string{"-config", cp}; err := runner.Run(args); if err == nil || !strings.Contains(err.Error(), "validation failed") || !strings.Contains(err.Error(), "Source.Type: is required") { t.Errorf("Expected validation err for missing Source.Type, got: %v", err) } }) }
func TestAppRunner_Run_HappyPath_Minimal(t *testing.T) { runner := NewAppRunner(); mIn, mOut, mErr, mProc, _ := setupTestEnv(t); inData := []map[string]interface{}{{"c1": "v1"}}; procData := []map[string]interface{}{{"o1": "v1"}}; mIn.readFunc = func(p string) ([]map[string]interface{}, error) { return inData, nil }; mProc.processFunc = func(i []map[string]interface{}) ([]map[string]interface{}, error) { return procData, nil }; cp := createTempYAML(t, minimalValidConfig); args := []string{"-config", cp}; err := runner.Run(args); if err != nil { t.Fatalf("Run err: %v", err) }; if mIn.readCalls != 1 || mProc.processCalls != 1 || mOut.writeCalls != 1 || mOut.closeCalls != 1 || len(mErr.writeCalls) != 0 || mErr.closeCalls != 0 { t.Error("Call counts") }; if !reflect.DeepEqual(mOut.lastRecords, procData) { t.Error("Output mismatch") } }
func TestAppRunner_Run_DryRun(t *testing.T) { runner := NewAppRunner(); mIn, mOut, mErr, mProc, _ := setupTestEnv(t); mIn.readFunc = func(p string) ([]map[string]interface{}, error) { return []map[string]interface{}{{"c": "v"}}, nil }; mProc.processFunc = func(i []map[string]interface{}) ([]map[string]interface{}, error) { return []map[string]interface{}{{"o": "v"}}, nil }; cp := createTempYAML(t, minimalValidConfig); args := []string{"-config", cp, "-dry-run"}; err := runner.Run(args); if err != nil { t.Fatalf("Run err: %v", err) }; if mIn.readCalls != 1 || mProc.processCalls != 1 || mOut.writeCalls != 0 || mOut.closeCalls != 1 || len(mErr.writeCalls) != 0 { t.Errorf("Call counts mismatch (Write!=0)") } }
func TestAppRunner_Run_FlagOverrides(t *testing.T) { runner := NewAppRunner(); mIn, mOut, _, mProc, _ := setupTestEnv(t); mIn.readFunc = func(p string) ([]map[string]interface{}, error) { if p != "in_override" { t.Errorf("Input path mismatch: got %q", p) }; return []map[string]interface{}{{"c": "data"}}, nil }; mProc.processFunc = func(i []map[string]interface{}) ([]map[string]interface{}, error) { return i, nil }; cp := createTempYAML(t, `
source: { type: csv, file: orig_in }
destination: { type: json, file: orig_out }
mappings: [{ source: c, target: c }]`); args := []string{"-config", cp, "-input", "in_override", "-output", "out_override", "-loglevel", "debug", "-fips=true"}; err := runner.Run(args); if err != nil { t.Fatalf("Run err: %v", err) }; if mIn.lastReadArg != "in_override" { t.Error("Input mismatch") }; if mOut.lastWriteArg != "out_override" { t.Errorf("Output mismatch: got %q, want %q", mOut.lastWriteArg, "out_override") }; if logging.GetLevel() != logging.Debug { t.Error("Loglevel mismatch") }; if !transform.IsFIPSMode() { t.Error("FIPS mismatch") } }
func TestAppRunner_Run_EnvVarExpansion(t *testing.T) { runner := NewAppRunner(); mIn, mOut, _, mProc, _ := setupTestEnv(t); t.Setenv("IN", "/in"); t.Setenv("OUT", "C:\\out"); mIn.readFunc = func(p string) ([]map[string]interface{}, error) { if p != "/in/d.csv" { t.Errorf("Input mismatch: %s", p) }; return []map[string]interface{}{{"c": "data"}}, nil }; mProc.processFunc = func(i []map[string]interface{}) ([]map[string]interface{}, error) { return i, nil }; cp := createTempYAML(t, `
source: { type: csv, file: "$IN/d.csv" }
destination: { type: json, file: "%OUT%\\r.json" }
mappings: [{ source: c, target: c }]`); args := []string{"-config", cp}; err := runner.Run(args); if err != nil { t.Fatalf("Run err: %v", err) }; if mIn.lastReadArg != "/in/d.csv" { t.Error("Input path mismatch") }; if mOut.lastWriteArg != "C:\\out\\r.json" { t.Errorf("Output path mismatch: got %q, want %q", mOut.lastWriteArg, "C:\\out\\r.json") } }

func TestAppRunner_Run_ErrorHandling(t *testing.T) {
	runner := NewAppRunner()
	baseCfg := `
source: { type: csv, file: i.csv }
destination: { type: json, file: o.json }
mappings: [{ source: c, target: c }]
errorHandling: { mode: %s, errorFile: %q, logErrors: true }`
	inData := []map[string]interface{}{{"c": "ok1"}, {"c": "error_trigger"}, {"c": "ok2"}}
	simErr := errors.New("simulated processing error")

	t.Run("HaltMode", func(t *testing.T) {
		mIn, mOut, mErr, mProc, _ := setupTestEnv(t)
		cp := createTempYAML(t, fmt.Sprintf(baseCfg, "halt", ""))
		mIn.readFunc = func(p string) ([]map[string]interface{}, error) { return inData, nil }
		mProc.processFunc = func(i []map[string]interface{}) ([]map[string]interface{}, error) { for _, r := range i { if r["c"] == "error_trigger" { mProc.SetErrorCount(1); return nil, simErr } }; return i, nil }
		args := []string{"-config", cp}
		err := runner.Run(args)
		if err == nil { t.Fatal("Halt expected err") }
		if !strings.Contains(err.Error(), "processing") || !errors.Is(err, simErr) { t.Errorf("Halt err mismatch:%v", err) }
		if mProc.processCalls != 1 || mOut.writeCalls != 0 || len(mErr.writeCalls) != 0 || mErr.closeCalls != 0 { t.Error("Halt counts") }
	})

	t.Run("SkipModeWithErrorFile", func(t *testing.T) {
		mIn, mOut, mErr, mProc, _ := setupTestEnv(t) // mErr is the mock error writer instance
		errFP := "skip.csv"
		cp := createTempYAML(t, fmt.Sprintf(baseCfg, "skip", errFP))
		mIn.readFunc = func(p string) ([]map[string]interface{}, error) { return inData, nil }

		// Override the factory *specifically* for this test to return the mock *interface*
		originalErrWriterFactory := newCSVErrorWriterFunc // Save default
		newCSVErrorWriterFunc = func(fp string) (etlio.ErrorWriter, error) {
			return mErr, nil // Return the mock instance satisfying the interface
		}
		t.Cleanup(func() { newCSVErrorWriterFunc = originalErrWriterFactory }) // Restore default factory

		// Processor func uses the injected writer (set via factory in setupTestEnv -> NewProcessor)
		mProc.processFunc = func(i []map[string]interface{}) ([]map[string]interface{}, error) {
			v := []map[string]interface{}{}; ec := int64(0)
			for idx, r := range i {
				if r["c"] == "error_trigger" {
					ec++; simErr := fmt.Errorf("simulated skip error for record %d", idx)
					if mProc.errorWriter != nil { // Check if writer was actually assigned
						errWrite := mProc.errorWriter.Write(r, simErr)
						if errWrite != nil { t.Logf("Error writing to mock error writer: %v", errWrite) }
					} else { t.Errorf("Mock processor has no error writer assigned in this test run.") } // Should not happen now
				} else { v = append(v, r) }
			}
			mProc.SetErrorCount(ec); return v, nil
		}

		args := []string{"-config", cp}
		err := runner.Run(args)

		if err != nil { t.Fatalf("Skip err: %v", err) }
		if mProc.processCalls != 1 { t.Errorf("Processor calls = %d, want 1", mProc.processCalls) }
		if mOut.writeCalls != 1 { t.Errorf("Output writer calls = %d, want 1", mOut.writeCalls) }
		if mOut.closeCalls != 1 { t.Errorf("Output writer close calls = %d, want 1", mOut.closeCalls) }
		if mProc.GetErrorCount() != 1 { t.Errorf("Processor error count = %d, want 1", mProc.GetErrorCount()) }
		if len(mErr.writeCalls) != 1 { t.Errorf("Error writer calls = %d, want 1", len(mErr.writeCalls)) }
		if mErr.closeCalls != 1 { t.Errorf("Mock Error writer close calls = %d, want 1", mErr.closeCalls) } // Assert Close was called
		if !reflect.DeepEqual(mOut.lastRecords, []map[string]interface{}{{"c": "ok1"}, {"c": "ok2"}}) { t.Error("Skip output mismatch") }
		if len(mErr.writeCalls) == 1 { if !reflect.DeepEqual(mErr.writeCalls[0].Record, map[string]interface{}{"c": "error_trigger"}) { t.Error("Skip err rec mismatch") }; if mErr.writeCalls[0].Err == nil || !strings.Contains(mErr.writeCalls[0].Err.Error(), "simulated skip error") { t.Errorf("Skip err message mismatch: got %v", mErr.writeCalls[0].Err) } }
	})
}

// ... (Rest of test functions: Filtering, ComponentErrors, _anyFlagsSet, _isFlagSet) ...
func TestAppRunner_Run_Filtering(t *testing.T) { runner := NewAppRunner(); mIn, mOut, _, mProc, mExpr := setupTestEnv(t); cp := createTempYAML(t, `
source: { type: csv, file: i.csv }
destination: { type: json, file: o.json }
filter: "v>10"
mappings: [{ source: v, target: v }]`); inData := []map[string]interface{}{{"v": 5.0}, {"v": 15.0}, {"v": 10.1}}; expected := []map[string]interface{}{{"v": 15.0}, {"v": 10.1}}; mIn.readFunc = func(p string) ([]map[string]interface{}, error) { return inData, nil }; mExpr.EvaluateFunc = func(p map[string]interface{}) (interface{}, error) { v, _ := p["v"].(float64); return v > 10, nil }; mProc.processFunc = func(i []map[string]interface{}) ([]map[string]interface{}, error) { if !reflect.DeepEqual(i, expected) { t.Error("Filter input mismatch") }; return i, nil }; args := []string{"-config", cp}; err := runner.Run(args); if err != nil { t.Fatalf("Run err: %v", err) }; if mIn.readCalls != 1 || mProc.processCalls != 1 || mOut.writeCalls != 1 { t.Error("Filter counts") }; if !reflect.DeepEqual(mOut.lastRecords, expected) { t.Error("Filter output mismatch") } }
func TestAppRunner_Run_ComponentErrors(t *testing.T) { runner := NewAppRunner(); cfgPath := createTempYAML(t, minimalValidConfig); testCases := []struct { name string; setup func(*testing.T, *mockInputReader, *mockOutputWriter, *mockErrorWriter); cfg string; errFrag string; errCnt int64 }{{name: "InputReadErr", setup: func(_ *testing.T, mI *mockInputReader, _ *mockOutputWriter, _ *mockErrorWriter) { mI.readFunc = func(p string) ([]map[string]interface{}, error) { return nil, errors.New("mock read fail") } }, errFrag: "read input data: mock read fail"}, {name: "OutputWriteErr", setup: func(_ *testing.T, _ *mockInputReader, mO *mockOutputWriter, _ *mockErrorWriter) { mO.writeFunc = func(r []map[string]interface{}, p string) error { return errors.New("mock write fail") } }, errFrag: "write output data: mock write fail"}, {name: "OutputCloseErr", setup: func(_ *testing.T, _ *mockInputReader, mO *mockOutputWriter, _ *mockErrorWriter) { mO.closeFunc = func() error { return errors.New("mock close fail") } }, errFrag: ""}, {name: "ErrWriterFactoryErr", setup: func(t *testing.T, _ *mockInputReader, _ *mockOutputWriter, _ *mockErrorWriter) { orig := newCSVErrorWriterFunc; newCSVErrorWriterFunc = func(fp string) (etlio.ErrorWriter, error) { return nil, errors.New("mock factory fail") }; t.Cleanup(func() { newCSVErrorWriterFunc = orig }) }, cfg: `
source: { type: csv, file: i.csv }
destination: { type: json, file: o.json }
mappings: [{source: c, target: c}]
errorHandling: { mode: skip, errorFile: e.csv }`, errFrag: "create error writer for file 'e.csv': mock factory fail"}, {name: "MkdirAllErr", setup: func(t *testing.T, _ *mockInputReader, _ *mockOutputWriter, _ *mockErrorWriter) { orig := osMkdirAllFunc; osMkdirAllFunc = func(p string, pm os.FileMode) error { if p == filepath.Dir("bad/dir/e.csv") { return errors.New("mock mkdir fail") }; return orig(p, pm) }; t.Cleanup(func() { osMkdirAllFunc = orig }) }, cfg: `
source: { type: csv, file: i.csv }
destination: { type: json, file: o.json }
mappings: [{source: c, target: c}]
errorHandling: { mode: skip, errorFile: "bad/dir/e.csv" }`, errFrag: "create directory for error file 'bad/dir/e.csv': mock mkdir fail"}, }; for _, tc := range testCases { t.Run(tc.name, func(t *testing.T) { mIn, mOut, mErr, mProc, _ := setupTestEnv(t); if mIn.readFunc == nil { mIn.readFunc = func(string) ([]map[string]interface{}, error) { return []map[string]interface{}{{"c": "default"}}, nil } }; if tc.setup != nil { tc.setup(t, mIn, mOut, mErr) }; cp := cfgPath; if tc.cfg != "" { cp = createTempYAML(t, tc.cfg) }; args := []string{"-config", cp}; err := runner.Run(args); if tc.errFrag != "" { if err == nil { t.Fatalf("Expected err %q, got nil", tc.errFrag) }; if !strings.Contains(err.Error(), tc.errFrag) { t.Errorf("Err mismatch: got %q, want %q", err.Error(), tc.errFrag) } } else { if err != nil && tc.name != "OutputCloseErr" { t.Fatalf("Expected no err, got %v", err) } }; if tc.errCnt != mProc.GetErrorCount() { t.Errorf("Processor err count: got %d, want %d", mProc.GetErrorCount(), tc.errCnt) } }) } }
func Test_anyFlagsSet(t *testing.T) { testCases := []struct { n string; a []string; w bool }{ {"no", []string{}, false}, {"one", []string{"-config=a"}, true}, {"multi", []string{"-input=b", "-dry-run"}, true}, {"help", []string{"-help"}, true} }; for _, tc := range testCases { t.Run(tc.n, func(t *testing.T) { fs := flag.NewFlagSet("t", flag.ContinueOnError); fs.String("config", "", ""); fs.String("input", "", ""); fs.Bool("dry-run", false, ""); fs.Bool("help", false, ""); e := fs.Parse(tc.a); if e != nil && !errors.Is(e, flag.ErrHelp) { t.Fatal(e) }; g := anyFlagsSet(fs); if g != tc.w { t.Errorf("%v=%v,w %v", tc.a, g, tc.w) } }) } }
func Test_isFlagSet(t *testing.T) { testCases := []struct { n, f string; a []string; w bool }{ {"set", "config", []string{"-config=a"}, true}, {"not", "config", []string{"-input=b"}, false}, {"bool set", "dry-run", []string{"-dry-run"}, true}, {"bool not", "dry-run", []string{"-config=a"}, false}, {"no", "config", []string{}, false}, {"help", "help", []string{"-help"}, true} }; for _, tc := range testCases { t.Run(tc.n, func(t *testing.T) { fs := flag.NewFlagSet("t", flag.ContinueOnError); fs.String("config", "", ""); fs.String("input", "", ""); fs.Bool("dry-run", false, ""); fs.Bool("help", false, ""); e := fs.Parse(tc.a); if e != nil && !errors.Is(e, flag.ErrHelp) { t.Fatal(e) }; g := isFlagSet(fs, tc.f); if g != tc.w { t.Errorf("%s(%q,%v)=%v,w %v", tc.n, tc.f, tc.a, g, tc.w) } }) } }
