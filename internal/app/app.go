// --- START OF CORRECTED FILE internal/app/app.go ---
package app

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"etl-tool/internal/config"
	etlio "etl-tool/internal/io"
	"etl-tool/internal/logging"
	"etl-tool/internal/processor"
	"etl-tool/internal/transform"
	"etl-tool/internal/util"

	"github.com/Knetic/govaluate"
)

// Define common application-level errors.
var (
	ErrUsage          = errors.New("usage error")
	ErrConfigNotFound = errors.New("configuration file not found")
	ErrMissingArgs    = errors.New("missing required arguments")
)

// --- Interfaces for Mocking ---
type expressionEvaluator interface {
	Evaluate(map[string]interface{}) (interface{}, error)
}

// --- Factory Variables (Allow Overriding for Testing) ---
var (
	newInputReaderFunc  = etlio.NewInputReader
	newOutputWriterFunc = etlio.NewOutputWriter
	// *** CORRECTED Signature: Return interface type ***
	newCSVErrorWriterFunc = func(filePath string) (etlio.ErrorWriter, error) {
		// Production implementation calls the real constructor
		return etlio.NewCSVErrorWriter(filePath)
	}
	newProcessorFunc = processor.NewProcessor
	newExpressionEvaluatorFunc = func(expr string) (expressionEvaluator, error) {
		evalExpr, err := govaluate.NewEvaluableExpression(expr)
		if err != nil { return nil, err }
		return evalExpr, nil
	}
	osMkdirAllFunc = os.MkdirAll
	osStatFunc     = os.Stat
)

// AppRunner encapsulates the application's execution logic.
type AppRunner struct{}

// NewAppRunner creates a new instance of the application runner.
func NewAppRunner() *AppRunner {
	return &AppRunner{}
}

// usageText defines the command-line help information.
const usageText = `Usage:
  etl-tool [options]
... (rest of usage text remains the same) ...
`

// Usage prints the command-line help information to the specified writer.
func (a *AppRunner) Usage(writer io.Writer) {
	fmt.Fprint(writer, usageText)
}

// Run parses command-line arguments and executes the ETL workflow.
func (a *AppRunner) Run(args []string) error {
	fs := flag.NewFlagSet("etl-tool", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configFile := fs.String("config", "config/etl-config.yaml", "YAML configuration file")
	flagInputFile := fs.String("input", "", "Override input file path from config")
	flagOutputFile := fs.String("output", "", "Override output file path from config")
	dbConnStr := fs.String("db", "", "PostgreSQL connection string")
	logLevelStr := fs.String("loglevel", "info", "Logging level")
	dryRunFlag := fs.Bool("dry-run", false, "Perform dry run")
	fipsFlag := fs.Bool("fips", false, "Enable FIPS mode")
	helpFlag := fs.Bool("help", false, "Show help")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) { a.Usage(os.Stderr); return nil }
		logging.Logf(logging.Error, "Failed to parse args: %v", err); return fmt.Errorf("%w: %v", ErrUsage, err)
	}
	if *helpFlag || (len(args) == 0 && !anyFlagsSet(fs)) { a.Usage(os.Stderr); return nil }

	logging.SetupLogging(*logLevelStr)
	if _, err := osStatFunc(*configFile); err != nil {
		if os.IsNotExist(err) { logging.Logf(logging.Error, "Config file '%s' not found.", *configFile); return ErrConfigNotFound }
		return fmt.Errorf("failed to stat config file '%s': %w", *configFile, err)
	}
	cfg, err := config.LoadConfig(*configFile); if err != nil { logging.Logf(logging.Error, "Error loading/validating config '%s': %v", *configFile, err); return err }

	if !isFlagSet(fs, "loglevel") && cfg.Logging.Level != "" { logging.SetupLogging(cfg.Logging.Level) }
	logging.Logf(logging.Info, "Starting ETL with config: %s", *configFile)
	fipsEnabled := *fipsFlag; if !isFlagSet(fs, "fips") { fipsEnabled = cfg.FIPSMode }
	if fipsEnabled { logging.Logf(logging.Info, "FIPS mode enabled."); transform.SetFIPSMode(fipsEnabled) }

	inputFile := cfg.Source.File; if *flagInputFile != "" { inputFile = *flagInputFile; logging.Logf(logging.Info, "Override input: %s", inputFile) }; inputFile = util.ExpandEnvUniversal(inputFile)
	outputFile := cfg.Destination.File; if *flagOutputFile != "" { outputFile = *flagOutputFile; logging.Logf(logging.Info, "Override output: %s", outputFile) }; outputFile = util.ExpandEnvUniversal(outputFile)
	finalDBConn := *dbConnStr; if finalDBConn == "" { finalDBConn = os.Getenv("DB_CREDENTIALS") }; finalDBConn = util.ExpandEnvUniversal(finalDBConn)

	errorFile := ""; errorFileMsg := ""
	if cfg.ErrorHandling != nil && cfg.ErrorHandling.ErrorFile != "" {
		errorFile = util.ExpandEnvUniversal(cfg.ErrorHandling.ErrorFile)
		errorDir := filepath.Dir(errorFile)
		if errorDir != "." && errorDir != "" {
			if err := osMkdirAllFunc(errorDir, 0755); err != nil { return fmt.Errorf("failed to create directory for error file '%s': %w", errorFile, err) }
		}
		errorFileMsg = fmt.Sprintf(" (see error file: %s)", errorFile)
	}

	inputReader, err := newInputReaderFunc(cfg.Source, finalDBConn); if err != nil { return fmt.Errorf("failed to create input reader: %w", err) }
	outputWriter, err := newOutputWriterFunc(cfg.Destination, finalDBConn); if err != nil { return fmt.Errorf("failed to create output writer: %w", err) }
	defer func() { if outputWriter != nil { logging.Logf(logging.Debug, "Closing output writer..."); if closeErr := outputWriter.Close(); closeErr != nil { logging.Logf(logging.Error, "Failed to close output writer: %v", closeErr) } else { logging.Logf(logging.Debug, "Output writer closed.") } } }()

	var errorWriter etlio.ErrorWriter // Stays as interface type
	if errorFile != "" {
		// *** CORRECTED: Factory now returns interface ***
		createdErrorWriter, err := newCSVErrorWriterFunc(errorFile) // Returns etlio.ErrorWriter, error
		if err != nil {
			return fmt.Errorf("failed to create error writer for file '%s': %w", errorFile, err)
		}
		// *** CORRECTED: Check if the returned interface is nil ***
		if createdErrorWriter != nil {
			errorWriter = createdErrorWriter // Assign interface to interface
			defer func(ew etlio.ErrorWriter) { // Defer operates on the interface
				logging.Logf(logging.Debug, "Closing error writer...")
				if cerr := ew.Close(); cerr != nil {
					logging.Logf(logging.Error, "Failed to close error writer '%s': %v", errorFile, cerr)
				} else {
					logging.Logf(logging.Debug, "Error writer closed.")
				}
			}(errorWriter)
			logging.Logf(logging.Info, "Error records will be written to: %s", errorFile)
		} else {
			logging.Logf(logging.Debug, "Error writer instance is nil after factory call (likely testing or empty file).")
			// errorWriter remains nil
		}
	}

	proc := newProcessorFunc(cfg.Mappings, cfg.Flattening, cfg.Dedup, cfg.ErrorHandling, errorWriter)

	logging.Logf(logging.Info, "Extracting from %s...", cfg.Source.Type); initialRecords, err := inputReader.Read(inputFile); if err != nil { return fmt.Errorf("failed to read input data: %w", err) }; logging.Logf(logging.Info, "Extracted %d records.", len(initialRecords))

	filteredRecords := initialRecords
	if cfg.Filter != "" {
		logging.Logf(logging.Info, "Applying filter: %s", cfg.Filter)
		filterEvaluator, err := newExpressionEvaluatorFunc(cfg.Filter)
		if err != nil { return fmt.Errorf("invalid filter expression '%s': %w", cfg.Filter, err) }
		keptRecords := make([]map[string]interface{}, 0, len(initialRecords)); skippedCount := 0
		for i, record := range initialRecords {
			result, evalErr := filterEvaluator.Evaluate(record)
			if evalErr != nil { logging.Logf(logging.Error, "Filter fail R#%d: %v. Skip. Rec(masked): %v", i, evalErr, util.MaskSensitiveData(record)); skippedCount++; if errorWriter != nil { _ = errorWriter.Write(record, fmt.Errorf("filter eval error: %w", evalErr)) }; continue }
			keep, isBool := result.(bool); if !isBool { logging.Logf(logging.Error, "Filter non-bool R#%d (type %T): %v. Skip.", i, result, result); skippedCount++; if errorWriter != nil { _ = errorWriter.Write(record, fmt.Errorf("filter non-bool: %T (%v)", result, result)) }; continue }
			if keep { keptRecords = append(keptRecords, record) } else { skippedCount++; logging.Logf(logging.Debug, "Record %d skipped by filter.", i) }
		}
		logging.Logf(logging.Info, "Filter applied: %d kept, %d skipped.", len(keptRecords), skippedCount); filteredRecords = keptRecords
	}
	if len(filteredRecords) == 0 { logging.Logf(logging.Info, "No records after filtering."); return nil }

	logging.Logf(logging.Info, "Processing %d records...", len(filteredRecords))
	processedRecords, err := proc.ProcessRecords(filteredRecords)
	if err != nil { return fmt.Errorf("failed during record processing: %w", err) }
	finalRecordCount := len(processedRecords); errorCount := proc.GetErrorCount()
	if cfg.Dedup != nil && len(cfg.Dedup.Keys) > 0 { logging.Logf(logging.Info, "Processed %d unique records.", finalRecordCount) } else { logging.Logf(logging.Info, "Processed %d records.", finalRecordCount) }
	if errorCount > 0 { logging.Logf(logging.Warning, "%d records/parents skipped due to processing errors%s.", errorCount, errorFileMsg) }
	if finalRecordCount == 0 { logging.Logf(logging.Info, "No records remaining after processing%s.", errorFileMsg); return nil }

	if *dryRunFlag {
		logging.Logf(logging.Info, "DRY RUN: Skip load. Would write %d records to %s.", finalRecordCount, cfg.Destination.Type)
		sampleSize := 5; if finalRecordCount < sampleSize { sampleSize = finalRecordCount }
		if sampleSize > 0 { logging.Logf(logging.Debug, "Sample (first %d, masked):", sampleSize); for i := 0; i < sampleSize; i++ { logging.Logf(logging.Debug, "Record %d: %v", i, util.MaskSensitiveData(processedRecords[i])) } }
	} else {
		logging.Logf(logging.Info, "Loading %d records to %s...", finalRecordCount, cfg.Destination.Type)
		if err := outputWriter.Write(processedRecords, outputFile); err != nil { return fmt.Errorf("failed to write output data: %w", err) }
		logging.Logf(logging.Info, "Data loaded successfully.")
	}
	return nil
}

// Helper functions
func anyFlagsSet(fs *flag.FlagSet) bool { any := false; fs.Visit(func(*flag.Flag) { any = true }); return any }
func isFlagSet(fs *flag.FlagSet, name string) bool { set := false; fs.Visit(func(f *flag.Flag) { if f.Name == name { set = true } }); return set }
