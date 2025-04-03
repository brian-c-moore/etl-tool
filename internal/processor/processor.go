package processor

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"etl-tool/internal/config"
	etlio "etl-tool/internal/io"
	"etl-tool/internal/logging"
	"etl-tool/internal/transform"
	"etl-tool/internal/util"
)

// Processor defines the interface for processing records.
// This allows mocking the processor implementation in tests.
type Processor interface {
	ProcessRecords(inputRecords []map[string]interface{}) ([]map[string]interface{}, error)
	GetErrorCount() int64
}

// processorImpl handles transformation, validation, and deduplication.
// It implements the Processor interface.
type processorImpl struct {
	mappings      []config.MappingRule
	dedupCfg      *config.DedupConfig
	errorHandling *config.ErrorHandlingConfig
	errorWriter   etlio.ErrorWriter
	errorCount    atomic.Int64
}

// NewProcessor creates a new Processor instance satisfying the Processor interface.
func NewProcessor(mappings []config.MappingRule, dedupCfg *config.DedupConfig, errorHandling *config.ErrorHandlingConfig, errorWriter etlio.ErrorWriter) Processor { // Return interface type
	// Apply defaults if configurations are nil or partially set
	eh := errorHandling
	if eh == nil {
		eh = &config.ErrorHandlingConfig{Mode: config.ErrorHandlingModeHalt}
	} else {
		if eh.Mode == "" {
			eh.Mode = config.ErrorHandlingModeHalt
		}
		// Default LogErrors to true only if mode is skip and LogErrors is nil
		if eh.Mode == config.ErrorHandlingModeSkip && eh.LogErrors == nil {
			trueVal := true
			eh.LogErrors = &trueVal
		}
	}

	dc := dedupCfg
	if dc != nil && dc.Strategy == "" {
		dc.Strategy = config.DefaultDedupStrategy
	}

	// Return the concrete implementation that satisfies the interface
	return &processorImpl{
		mappings:      mappings,
		dedupCfg:      dc,
		errorHandling: eh,
		errorWriter:   errorWriter,
	}
}

// GetErrorCount returns the number of records skipped due to processing errors.
func (p *processorImpl) GetErrorCount() int64 {
	return p.errorCount.Load()
}

// ProcessRecords applies mappings, validations, and deduplication according to configuration.
func (p *processorImpl) ProcessRecords(inputRecords []map[string]interface{}) ([]map[string]interface{}, error) {
	if len(inputRecords) == 0 {
		logging.Logf(logging.Info, "Processor: No input records to process.")
		return []map[string]interface{}{}, nil
	}

	processedRecords := make([]map[string]interface{}, 0, len(inputRecords))
	p.errorCount.Store(0) // Reset error count for this run

	logging.Logf(logging.Debug, "Processor: Starting transformation/validation for %d records.", len(inputRecords))
	for i, originalRec := range inputRecords {
		recordIndex := i
		targetRecord, err := p.processSingleRecord(originalRec) // Internal method call
		if err != nil {
			p.errorCount.Add(1)
			shouldLog := p.errorHandling.Mode == config.ErrorHandlingModeSkip && (p.errorHandling.LogErrors == nil || *p.errorHandling.LogErrors)

			if shouldLog {
				logging.Logf(logging.Warning, "Processor: Error record %d: %v. Skipping. Original (masked): %v", recordIndex, err, util.MaskSensitiveData(originalRec))
			} else if p.errorHandling.Mode == config.ErrorHandlingModeHalt {
				logging.Logf(logging.Error, "Processor: Error record %d: %v. Halting.", recordIndex, err)
			}

			// Write to error file if configured and available
			if p.errorHandling.Mode == config.ErrorHandlingModeSkip && p.errorWriter != nil {
				if writeErr := p.errorWriter.Write(originalRec, err); writeErr != nil {
					logging.Logf(logging.Error, "Processor: Failed to write record %d error to error file: %v", recordIndex, writeErr)
				}
			}

			if p.errorHandling.Mode == config.ErrorHandlingModeHalt {
				return nil, fmt.Errorf("error processing record %d (halting): %w", recordIndex, err) // Wrap original error
			}
			continue // Skip appending failed record
		}
		processedRecords = append(processedRecords, targetRecord)
	}
	logging.Logf(logging.Debug, "Processor: Transformation/validation phase completed.")

	// --- Deduplication ---
	finalRecords := processedRecords
	if p.dedupCfg != nil && len(p.dedupCfg.Keys) > 0 && len(processedRecords) > 0 {
		originalCount := len(processedRecords)
		logging.Logf(logging.Debug, "Processor: Starting deduplication (Strategy: '%s', Keys: %v) on %d records.", p.dedupCfg.Strategy, p.dedupCfg.Keys, originalCount)
		finalRecords = p.dedupRecords(processedRecords) // Internal method call
		dedupedCount := originalCount - len(finalRecords)
		if dedupedCount > 0 {
			logging.Logf(logging.Info, "Processor: Deduplication removed %d records (%d -> %d).", dedupedCount, originalCount, len(finalRecords))
		} else {
			logging.Logf(logging.Debug, "Processor: Deduplication found no duplicates with strategy '%s'.", p.dedupCfg.Strategy)
		}
	} else if p.dedupCfg != nil && len(p.dedupCfg.Keys) > 0 {
		logging.Logf(logging.Debug, "Processor: Skipping deduplication (no records after processing).")
	}

	// --- Final Logging ---
	totalErrors := p.GetErrorCount()
	if totalErrors > 0 {
		logging.Logf(logging.Warning, "Processor: Finished processing. Skipped %d records due to errors.", totalErrors)
	} else {
		logging.Logf(logging.Debug, "Processor: Finished processing successfully with no errors.")
	}

	return finalRecords, nil
}

// processSingleRecord applies mapping rules to one record.
func (p *processorImpl) processSingleRecord(originalRecord map[string]interface{}) (map[string]interface{}, error) {
	targetRecord := make(map[string]interface{})
	// Copy original record to allow transforms to read prior results within the same record processing step
	currentRecordState := make(map[string]interface{}, len(originalRecord)+len(p.mappings))
	for k, v := range originalRecord {
		currentRecordState[k] = v
	}

	for i, rule := range p.mappings {
		sourceValue, sourceExists := currentRecordState[rule.Source]
		logMsgDetail := fmt.Sprintf("Using source '%s': %v", rule.Source, sourceValue)
		if !sourceExists {
			sourceValue = nil // Use nil if source field doesn't exist in current state
			logMsgDetail = fmt.Sprintf("Source '%s' not found, using nil", rule.Source)
		}
		logging.Logf(logging.Debug, "Mapping #%d ('%s' -> '%s'): %s", i, rule.Source, rule.Target, logMsgDetail)

		var transformedValue interface{}
		if rule.Transform != "" {
			transformedValue = transform.ApplyTransform(rule.Transform, rule.Params, sourceValue, currentRecordState)
			logging.Logf(logging.Debug, "Mapping #%d: Applied transform '%s', result: %v", i, rule.Transform, transformedValue)

			// Check if the result is an error (from validation or strict transform)
			if err, isError := transformedValue.(error); isError {
				// Return the specific error for handling by ProcessRecords
				return nil, fmt.Errorf("validation failed for rule #%d ('%s' -> '%s', transform: '%s'): %w", i, rule.Source, rule.Target, rule.Transform, err)
			}
		} else {
			transformedValue = sourceValue // No transform, pass through
			logging.Logf(logging.Debug, "Mapping #%d: No transform, assigned source value: %v", i, transformedValue)
		}

		// Update target record and current state for subsequent rules
		targetRecord[rule.Target] = transformedValue
		currentRecordState[rule.Target] = transformedValue
	}
	logging.Logf(logging.Debug, "Finished record processing, final target: %v", util.MaskSensitiveData(targetRecord))
	return targetRecord, nil
}

// dedupRecords removes duplicates based on config.
func (p *processorImpl) dedupRecords(records []map[string]interface{}) []map[string]interface{} {
	seen := make(map[string]map[string]interface{}) // Map composite key to the record to keep
	keys := p.dedupCfg.Keys
	sort.Strings(keys) // Ensure consistent key order
	lcStrategy := strings.ToLower(p.dedupCfg.Strategy)
	strategyField := p.dedupCfg.StrategyField
	placeholder := "<ETL_NIL_OR_MISSING>"

	for recIndex, currentRec := range records { // Added recIndex for logging
		// Generate composite key from specified key fields
		var compositeKeyParts []string
		for _, key := range keys {
			// --- Debugging ---
			lookupValue, lookupOK := currentRec[key]
			logging.Logf(logging.Debug, "DEDUPE_DEBUG: Record %d, Key '%s', Lookup OK: %t, Value: %v (%T)", recIndex, key, lookupOK, lookupValue, lookupValue)
			// --- End Debugging ---

			// Original condition:
			if val, ok := currentRec[key]; ok && val != nil {
				// This branch should be taken based on trace
				compositeKeyParts = append(compositeKeyParts, transform.ValueToStringForHash(val))
				logging.Logf(logging.Debug, "DEDUPE_DEBUG: Record %d, Key '%s', IF condition TRUE, Appended: '%s'", recIndex, key, transform.ValueToStringForHash(val))
			} else {
				// This branch seems to be taken based on logs/output
				compositeKeyParts = append(compositeKeyParts, placeholder)
				logging.Logf(logging.Debug, "DEDUPE_DEBUG: Record %d, Key '%s', ELSE condition TRUE (ok=%t, valIsNil=%t), Appended: '%s'", recIndex, key, ok, val == nil, placeholder)
			}
		}
		compositeKey := strings.Join(compositeKeyParts, "||") // Separator
		logging.Logf(logging.Debug, "DEDUPE_DEBUG: Record %d, Final Composite Key: '%s'", recIndex, compositeKey) // Log the final key


		storedRec, keyExists := seen[compositeKey]

		// Apply strategy
		keepCurrent := false // Assume we keep the stored record by default
		if !keyExists {
			keepCurrent = true // Keep the first record seen for this key
		} else {
			// CORRECTED: Added explicit case for DedupStrategyFirst
			switch lcStrategy {
			case config.DedupStrategyFirst:
				// Logic is handled by the initial !keyExists check.
				// If keyExists is true, we don't replace (keepCurrent remains false).
				break
			case config.DedupStrategyLast:
				keepCurrent = true // Always keep the latest record seen
			case config.DedupStrategyMin, config.DedupStrategyMax:
				currentVal, currentOk := currentRec[strategyField]
				storedVal, storedOk := storedRec[strategyField]
				if !currentOk { // Current record missing strategy field, keep stored
					logging.Logf(logging.Warning, "Dedupe (%s): Field '%s' missing current key '%s'. Keep stored.", lcStrategy, strategyField, compositeKey)
				} else if !storedOk { // Stored record missing strategy field, keep current
					logging.Logf(logging.Warning, "Dedupe (%s): Field '%s' missing stored key '%s'. Replace.", lcStrategy, strategyField, compositeKey)
					keepCurrent = true
				} else { // Both have strategy field, compare them
					comparisonResult, err := transform.CompareValues(currentVal, storedVal)
					if err != nil { // Cannot compare, keep stored
						logging.Logf(logging.Warning, "Dedupe (%s): Cannot compare '%s' key '%s': %v. Keep stored.", lcStrategy, strategyField, compositeKey, err)
					} else {
						if (lcStrategy == config.DedupStrategyMin && comparisonResult < 0) || (lcStrategy == config.DedupStrategyMax && comparisonResult > 0) {
							keepCurrent = true // Keep current if it's smaller (min) or larger (max)
						}
					}
				}
			default: // Should no longer be hit for known strategies
				logging.Logf(logging.Error, "Dedupe: Internal error - reached default case for known strategy '%s'. Key '%s'.", p.dedupCfg.Strategy, compositeKey)
				if !keyExists {
					keepCurrent = true // Fallback logic just in case
				}
			}
		}

		// Update seen map if current record should be kept
		if keepCurrent {
			if keyExists && lcStrategy != config.DedupStrategyFirst { // Log replacement only if not first strategy
				logging.Logf(logging.Debug, "Dedupe (%s): Replace key '%s'. New: %v", lcStrategy, compositeKey, util.MaskSensitiveData(currentRec))
			} else if !keyExists { // Log initial addition
				logging.Logf(logging.Debug, "Dedupe: Add key '%s'. Record: %v", compositeKey, util.MaskSensitiveData(currentRec))
			}
			seen[compositeKey] = currentRec
		} else {
			if keyExists { // Log skip only if key was already seen
				logging.Logf(logging.Debug, "Dedupe (%s): Skip key '%s'. Keep stored: %v", lcStrategy, compositeKey, util.MaskSensitiveData(storedRec))
			}
		}
	}

	// Collect the final unique records
	uniqueRecords := make([]map[string]interface{}, 0, len(seen))
	for key, record := range seen { // Iterate over the VALUES of the 'seen' map
	    logging.Logf(logging.Debug, "DEDUPE_DEBUG: Collecting record from seen map for key '%s'", key) // Log collection
		uniqueRecords = append(uniqueRecords, record)
	}
	// Optional: Sort final records if needed for deterministic output order
	// If order needs to match input order for 'first'/'last', more complex tracking is needed.
	// For now, the order is determined by map iteration.
	return uniqueRecords
}
