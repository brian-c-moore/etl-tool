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

// expressionEvaluator defines the interface for evaluating filter expressions.
// This allows mocking the govaluate dependency.
type expressionEvaluator interface {
	Evaluate(map[string]interface{}) (interface{}, error)
}

// --- Factory Variables (Allow Overriding for Testing) ---
// These variables hold the functions used to create dependencies.
// Tests can replace these functions with mocks.
var (
	// IO Factories
	newInputReaderFunc    = etlio.NewInputReader    // Returns etlio.InputReader
	newOutputWriterFunc   = etlio.NewOutputWriter   // Returns etlio.OutputWriter
	newCSVErrorWriterFunc = etlio.NewCSVErrorWriter // Returns *etlio.CSVErrorWriter, error

	// Processor Factory
	newProcessorFunc = processor.NewProcessor // Returns processor.Processor

	// Evaluator Factory (Wraps govaluate)
	newExpressionEvaluatorFunc = func(expr string) (expressionEvaluator, error) { // Returns expressionEvaluator
		evalExpr, err := govaluate.NewEvaluableExpression(expr)
		if err != nil {
			return nil, err
		}
		// *govaluate.EvaluableExpression implicitly satisfies the expressionEvaluator interface
		return evalExpr, nil
	}

	// Filesystem Function Mocks
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

Options:
  -config string
        YAML configuration file (default "config/etl-config.yaml")
  -input string
        Override input file path from config (ignored for type=postgres)
  -output string
        Override output file path from config (ignored for type=postgres)
  -db string
        PostgreSQL connection string (overrides DB_CREDENTIALS env var)
  -loglevel string
        Logging level (none, error, warn, info, debug) (default "info")
  -dry-run
        Perform all steps except writing to the destination (default false)
  -fips
        Enable FIPS mode (restricts certain crypto algorithms) (default false)
  -help
        Show help

Environment Variables:
  DB_CREDENTIALS   PostgreSQL connection string (used if -db is not set)
  Any VAR          Can be used in config paths/connection strings via $VAR/${VAR} or %VAR%

Examples:
  etl-tool -config=path/to/your_config.yaml -loglevel=debug
  etl-tool -config=pg_config.yaml -db="postgres://user:pass@host:port/db"
  etl-tool -config=file_config.yaml -input=/data/new_input.csv -output=/tmp/output.json
  etl-tool -config=transform_config.yaml -dry-run
`

// Usage prints the command-line help information to the specified writer.
func (a *AppRunner) Usage(writer io.Writer) {
	fmt.Fprint(writer, usageText)
}

// Run parses command-line arguments and executes the ETL workflow.
func (a *AppRunner) Run(args []string) error {
	// --- Flag Parsing ---
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

	// --- Initial Setup & Config Loading ---
	logging.SetupLogging(*logLevelStr)
	if _, err := osStatFunc(*configFile); err != nil { // Use mocked stat
		if os.IsNotExist(err) { logging.Logf(logging.Error, "Config file '%s' not found.", *configFile); return ErrConfigNotFound }
		return fmt.Errorf("failed to stat config file '%s': %w", *configFile, err)
	}
	cfg, err := config.LoadConfig(*configFile); if err != nil { logging.Logf(logging.Error, "Error loading/validating config '%s': %v", *configFile, err); return err }

	// --- Final Configuration ---
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
			// Use mocked filesystem function
			if err := osMkdirAllFunc(errorDir, 0755); err != nil { return fmt.Errorf("failed to create directory for error file '%s': %w", errorFile, err) }
		}
		errorFileMsg = fmt.Sprintf(" (see error file: %s)", errorFile)
	}

	// --- Instantiate Components via Factory Variables ---
	inputReader, err := newInputReaderFunc(cfg.Source, finalDBConn); if err != nil { return fmt.Errorf("failed to create input reader: %w", err) }
	outputWriter, err := newOutputWriterFunc(cfg.Destination, finalDBConn); if err != nil { return fmt.Errorf("failed to create output writer: %w", err) }
	defer func() { if outputWriter != nil { logging.Logf(logging.Debug, "Closing output writer..."); if closeErr := outputWriter.Close(); closeErr != nil { logging.Logf(logging.Error, "Failed to close output writer: %v", closeErr) } else { logging.Logf(logging.Debug, "Output writer closed.") } } }()

	var errorWriter etlio.ErrorWriter // Interface type variable
	if errorFile != "" {
		// Call factory variable
		csvErrWriterInstance, err := newCSVErrorWriterFunc(errorFile) // Returns concrete type *etlio.CSVErrorWriter
		if err != nil {
			// Factory itself failed
			return fmt.Errorf("failed to create error writer for file '%s': %w", errorFile, err)
		}

		// --- CORRECTED CHECK ---
		// Check if the concrete instance is nil *before* assigning to the interface
		// This prevents assigning a typed nil to the interface, avoiding the defer issue.
		if csvErrWriterInstance != nil {
			errorWriter = csvErrWriterInstance // Assign concrete instance to interface

			// Defer close only if we have a valid, non-nil instance
			defer func(ew etlio.ErrorWriter) { // Capture the current value of errorWriter
				logging.Logf(logging.Debug, "Closing error writer...")
				if cerr := ew.Close(); cerr != nil {
					logging.Logf(logging.Error, "Failed to close error writer '%s': %v", errorFile, cerr)
				} else {
					logging.Logf(logging.Debug, "Error writer closed.")
				}
			}(errorWriter) // Pass the interface variable to the defer closure

			logging.Logf(logging.Info, "Error records will be written to: %s", errorFile)
		} else {
			// Factory returned (nil, nil), likely during testing
			logging.Logf(logging.Debug, "Error writer instance is nil after factory call (likely testing).")
			// errorWriter remains nil
		}
		// --- END CORRECTION ---
	}

	// Processor created via factory variable, receives potentially nil errorWriter
	proc := newProcessorFunc(cfg.Mappings, cfg.Dedup, cfg.ErrorHandling, errorWriter) // Returns processor.Processor interface

	// --- Execute ETL Steps ---
	// 1. Extraction
	logging.Logf(logging.Info, "Extracting from %s...", cfg.Source.Type); initialRecords, err := inputReader.Read(inputFile); if err != nil { return fmt.Errorf("failed to read input data: %w", err) }; logging.Logf(logging.Info, "Extracted %d records.", len(initialRecords))

	// 1.5 Filtering
	filteredRecords := initialRecords
	if cfg.Filter != "" {
		logging.Logf(logging.Info, "Applying filter: %s", cfg.Filter)
		filterEvaluator, err := newExpressionEvaluatorFunc(cfg.Filter) // Use interface factory
		if err != nil { return fmt.Errorf("invalid filter expression '%s': %w", cfg.Filter, err) }
		keptRecords := make([]map[string]interface{}, 0, len(initialRecords)); skippedCount := 0
		for i, record := range initialRecords {
			result, evalErr := filterEvaluator.Evaluate(record) // Use interface
			if evalErr != nil { logging.Logf(logging.Error, "Filter fail R#%d: %v. Skip. Rec(masked): %v", i, evalErr, util.MaskSensitiveData(record)); skippedCount++; if errorWriter != nil { _ = errorWriter.Write(record, fmt.Errorf("filter eval error: %w", evalErr)) }; continue }
			keep, isBool := result.(bool); if !isBool { logging.Logf(logging.Error, "Filter non-bool R#%d (type %T): %v. Skip.", i, result, result); skippedCount++; if errorWriter != nil { _ = errorWriter.Write(record, fmt.Errorf("filter non-bool: %T (%v)", result, result)) }; continue }
			if keep { keptRecords = append(keptRecords, record) } else { skippedCount++; logging.Logf(logging.Debug, "Record %d skipped by filter.", i) }
		}
		logging.Logf(logging.Info, "Filter applied: %d kept, %d skipped.", len(keptRecords), skippedCount); filteredRecords = keptRecords
	}
	if len(filteredRecords) == 0 { logging.Logf(logging.Info, "No records after filtering."); return nil }

	// 2. Processing
	logging.Logf(logging.Info, "Processing %d records...", len(filteredRecords))
	processedRecords, err := proc.ProcessRecords(filteredRecords) // Use processor.Processor interface
	if err != nil { return fmt.Errorf("failed during record processing: %w", err) }
	finalRecordCount := len(processedRecords); errorCount := proc.GetErrorCount() // Use interface
	if cfg.Dedup != nil && len(cfg.Dedup.Keys) > 0 { logging.Logf(logging.Info, "Processed %d unique records.", finalRecordCount) } else { logging.Logf(logging.Info, "Processed %d records.", finalRecordCount) }
	if errorCount > 0 { logging.Logf(logging.Warning, "%d records skipped due to processing errors%s.", errorCount, errorFileMsg) }
	if finalRecordCount == 0 { logging.Logf(logging.Info, "No records remaining after processing%s.", errorFileMsg); return nil }

	// 3. Loading
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
