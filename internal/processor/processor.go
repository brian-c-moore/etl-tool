package processor

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"

	"etl-tool/internal/config"
	etlio "etl-tool/internal/io"
	"etl-tool/internal/logging"
	"etl-tool/internal/transform"
	"etl-tool/internal/util"
	"github.com/mohae/deepcopy" // Import for deep copy functionality
)

// Processor defines the interface for processing records.
type Processor interface {
	ProcessRecords(inputRecords []map[string]interface{}) ([]map[string]interface{}, error)
	GetErrorCount() int64
}

// processorImpl handles transformation, validation, and deduplication.
type processorImpl struct {
	mappings      []config.MappingRule
	flatteningCfg *config.FlatteningConfig
	dedupCfg      *config.DedupConfig
	errorHandling *config.ErrorHandlingConfig
	errorWriter   etlio.ErrorWriter
	errorCount    atomic.Int64
}

// NewProcessor creates a new Processor instance satisfying the Processor interface.
func NewProcessor(mappings []config.MappingRule, flatteningCfg *config.FlatteningConfig, dedupCfg *config.DedupConfig, errorHandling *config.ErrorHandlingConfig, errorWriter etlio.ErrorWriter) Processor {
	eh := errorHandling
	if eh == nil {
		eh = &config.ErrorHandlingConfig{Mode: config.ErrorHandlingModeHalt}
	} else {
		if eh.Mode == "" {
			eh.Mode = config.ErrorHandlingModeHalt
		}
		if eh.Mode == config.ErrorHandlingModeSkip && eh.LogErrors == nil {
			trueVal := true
			eh.LogErrors = &trueVal
		}
	}

	dc := dedupCfg
	if dc != nil && dc.Strategy == "" {
		dc.Strategy = config.DefaultDedupStrategy
	}

	fc := flatteningCfg
	if fc != nil {
		if fc.IncludeParent == nil {
			trueVal := true
			fc.IncludeParent = &trueVal
		}
		if fc.ErrorOnNonList == nil {
			falseVal := false
			fc.ErrorOnNonList = &falseVal
		}
	}

	return &processorImpl{
		mappings:      mappings,
		flatteningCfg: fc,
		dedupCfg:      dc,
		errorHandling: eh,
		errorWriter:   errorWriter,
	}
}

// GetErrorCount returns the number of records skipped due to processing errors.
func (p *processorImpl) GetErrorCount() int64 {
	return p.errorCount.Load()
}

// ProcessRecords applies mappings, validations, flattening, and deduplication.
func (p *processorImpl) ProcessRecords(inputRecords []map[string]interface{}) ([]map[string]interface{}, error) {
	if len(inputRecords) == 0 {
		logging.Logf(logging.Info, "Processor: No input records to process.")
		return []map[string]interface{}{}, nil
	}

	transformedRecords := make([]map[string]interface{}, 0, len(inputRecords))
	p.errorCount.Store(0)

	logging.Logf(logging.Debug, "Processor: Starting transformation/validation for %d records.", len(inputRecords))
	for i, originalRec := range inputRecords {
		recordIndex := i
		targetRecord, err := p.processSingleRecord(originalRec)
		if err != nil {
			p.errorCount.Add(1)
			shouldLog := p.errorHandling.Mode == config.ErrorHandlingModeSkip && (p.errorHandling.LogErrors == nil || *p.errorHandling.LogErrors)
			if shouldLog { logging.Logf(logging.Warning, "Processor: Error record %d (mapping): %v. Skipping. Original (masked): %v", recordIndex, err, util.MaskSensitiveData(originalRec)) } else if p.errorHandling.Mode == config.ErrorHandlingModeHalt { logging.Logf(logging.Error, "Processor: Error record %d (mapping): %v. Halting.", recordIndex, err) }
			if p.errorHandling.Mode == config.ErrorHandlingModeSkip && p.errorWriter != nil { if writeErr := p.errorWriter.Write(originalRec, err); writeErr != nil { logging.Logf(logging.Error, "Processor: Failed to write record %d (mapping) error to error file: %v", recordIndex, writeErr) } }
			if p.errorHandling.Mode == config.ErrorHandlingModeHalt { return nil, fmt.Errorf("error processing record %d (mapping, halting): %w", recordIndex, err) }
			continue
		}
		transformedRecords = append(transformedRecords, targetRecord)
	}
	logging.Logf(logging.Debug, "Processor: Transformation/validation phase completed. %d records remain.", len(transformedRecords))

	flattenedRecords := transformedRecords
	if p.flatteningCfg != nil && len(flattenedRecords) > 0 {
		logging.Logf(logging.Debug, "Processor: Starting flattening (Source: '%s', Target: '%s').", p.flatteningCfg.SourceField, p.flatteningCfg.TargetField)
		flattenedOutput := make([]map[string]interface{}, 0, len(flattenedRecords))
		for i, parentRecord := range flattenedRecords {
			recordIndex := i
			flatRecs, err := p.flattenSingleRecord(parentRecord)
			if err != nil {
				p.errorCount.Add(1)
				shouldLog := p.errorHandling.Mode == config.ErrorHandlingModeSkip && (p.errorHandling.LogErrors == nil || *p.errorHandling.LogErrors)
				if shouldLog { logging.Logf(logging.Warning, "Processor: Error record %d (flattening): %v. Skipping parent record. Parent (masked): %v", recordIndex, err, util.MaskSensitiveData(parentRecord)) } else if p.errorHandling.Mode == config.ErrorHandlingModeHalt { logging.Logf(logging.Error, "Processor: Error record %d (flattening): %v. Halting.", recordIndex, err) }
				if p.errorHandling.Mode == config.ErrorHandlingModeSkip && p.errorWriter != nil { if writeErr := p.errorWriter.Write(parentRecord, err); writeErr != nil { logging.Logf(logging.Error, "Processor: Failed to write record %d (flattening) error to error file: %v", recordIndex, writeErr) } }
				if p.errorHandling.Mode == config.ErrorHandlingModeHalt { return nil, fmt.Errorf("error processing record %d (flattening, halting): %w", recordIndex, err) }
				continue
			}
			flattenedOutput = append(flattenedOutput, flatRecs...)
		}
		flattenedRecords = flattenedOutput
		logging.Logf(logging.Debug, "Processor: Flattening phase completed. %d records remain.", len(flattenedRecords))
	}

	finalRecords := flattenedRecords
	if p.dedupCfg != nil && len(p.dedupCfg.Keys) > 0 && len(flattenedRecords) > 0 {
		originalCount := len(flattenedRecords)
		logging.Logf(logging.Debug, "Processor: Starting deduplication (Strategy: '%s', Keys: %v) on %d records.", p.dedupCfg.Strategy, p.dedupCfg.Keys, originalCount)
		finalRecords = p.dedupRecords(flattenedRecords)
		dedupedCount := originalCount - len(finalRecords)
		if dedupedCount > 0 { logging.Logf(logging.Info, "Processor: Deduplication removed %d records (%d -> %d).", dedupedCount, originalCount, len(finalRecords)) } else { logging.Logf(logging.Debug, "Processor: Deduplication found no duplicates with strategy '%s'.", p.dedupCfg.Strategy) }
	} else if p.dedupCfg != nil && len(p.dedupCfg.Keys) > 0 {
		logging.Logf(logging.Debug, "Processor: Skipping deduplication (no records after processing/flattening).")
	}

	totalErrors := p.GetErrorCount()
	if totalErrors > 0 { logging.Logf(logging.Warning, "Processor: Finished processing. Skipped %d records/parents due to errors.", totalErrors) } else { logging.Logf(logging.Debug, "Processor: Finished processing successfully with no errors.") }
	return finalRecords, nil
}

// processSingleRecord applies mapping rules to one record.
func (p *processorImpl) processSingleRecord(originalRecord map[string]interface{}) (map[string]interface{}, error) {
	targetRecord := make(map[string]interface{})
	currentRecordState := make(map[string]interface{}, len(originalRecord)+len(p.mappings))
	for k, v := range originalRecord { currentRecordState[k] = v }
	for i, rule := range p.mappings {
		sourceValue, sourceExists := currentRecordState[rule.Source]
		logMsgDetail := fmt.Sprintf("Using source '%s': %v", rule.Source, sourceValue)
		if !sourceExists { sourceValue = nil; logMsgDetail = fmt.Sprintf("Source '%s' not found, using nil", rule.Source) }
		logging.Logf(logging.Debug, "Mapping #%d ('%s' -> '%s'): %s", i, rule.Source, rule.Target, logMsgDetail)
		var transformedValue interface{}
		if rule.Transform != "" {
			transformedValue = transform.ApplyTransform(rule.Transform, rule.Params, sourceValue, currentRecordState)
			logging.Logf(logging.Debug, "Mapping #%d: Applied transform '%s', result: %v", i, rule.Transform, transformedValue)
			if err, isError := transformedValue.(error); isError { return nil, fmt.Errorf("validation failed for rule #%d ('%s' -> '%s', transform: '%s'): %w", i, rule.Source, rule.Target, rule.Transform, err) }
		} else {
			transformedValue = sourceValue
			logging.Logf(logging.Debug, "Mapping #%d: No transform, assigned source value: %v", i, transformedValue)
		}
		targetRecord[rule.Target] = transformedValue
		currentRecordState[rule.Target] = transformedValue
	}
	logging.Logf(logging.Debug, "Finished record processing, final target: %v", util.MaskSensitiveData(targetRecord))
	return targetRecord, nil
}

// flattenSingleRecord handles the flattening logic for one input record based on config.
func (p *processorImpl) flattenSingleRecord(parentRecord map[string]interface{}) ([]map[string]interface{}, error) {
	cfg := p.flatteningCfg

	if cfg.ConditionField != "" {
		condValRaw, condOk := getNestedField(parentRecord, cfg.ConditionField)
		condValStr := ""
		if condOk && condValRaw != nil { condValStr = fmt.Sprintf("%v", condValRaw) }
		if !condOk || condValStr != cfg.ConditionValue {
			logging.Logf(logging.Debug, "Flattening: Condition %s=%s not met for record. Skipping flattening.", cfg.ConditionField, cfg.ConditionValue)
			return []map[string]interface{}{parentRecord}, nil
		}
	}

	sourceValRaw, srcOk := getNestedField(parentRecord, cfg.SourceField)
	if !srcOk || sourceValRaw == nil {
		if cfg.ErrorOnNonList != nil && *cfg.ErrorOnNonList { return nil, fmt.Errorf("flattening source field '%s' not found or is nil", cfg.SourceField) }
		logging.Logf(logging.Debug, "Flattening: Source field '%s' not found or nil. Skipping record.", cfg.SourceField)
		return []map[string]interface{}{}, nil
	}

	sourceValReflect := reflect.ValueOf(sourceValRaw)
	if sourceValReflect.Kind() != reflect.Slice {
		if cfg.ErrorOnNonList != nil && *cfg.ErrorOnNonList { return nil, fmt.Errorf("flattening source field '%s' is not a slice (type: %T)", cfg.SourceField, sourceValRaw) }
		logging.Logf(logging.Debug, "Flattening: Source field '%s' is not a slice (type: %T). Skipping record.", cfg.SourceField, sourceValRaw)
		return []map[string]interface{}{}, nil
	}

	sourceSliceLen := sourceValReflect.Len()
	if sourceSliceLen == 0 {
		logging.Logf(logging.Debug, "Flattening: Source field '%s' is an empty slice. Generating zero records.", cfg.SourceField)
		return []map[string]interface{}{}, nil
	}

	flattenedOutput := make([]map[string]interface{}, 0, sourceSliceLen)
	includeParent := cfg.IncludeParent == nil || *cfg.IncludeParent

	for i := 0; i < sourceSliceLen; i++ {
		item := sourceValReflect.Index(i).Interface()
		newRec := make(map[string]interface{})

		if includeParent {
			// *** CORRECTED PARENT COPY LOGIC FOR NESTED SOURCES ***
			// Use deepcopy to avoid modifying the original parentRecord or shared nested maps/slices
			copiedParent := deepcopy.Copy(parentRecord).(map[string]interface{})

			// Remove the source field from the copied parent structure
			removeNestedField(copiedParent, cfg.SourceField)

			// Copy the modified parent data into the new record
			for key, value := range copiedParent {
				newRec[key] = value
			}
			// *** END CORRECTION ***
		}

		newRec[cfg.TargetField] = item
		flattenedOutput = append(flattenedOutput, newRec)
	}

	return flattenedOutput, nil
}

// getNestedField retrieves a value from a nested map structure using a dot-notation path.
func getNestedField(data map[string]interface{}, path string) (interface{}, bool) {
	keys := strings.Split(path, ".")
	var currentVal interface{} = data

	for i, key := range keys {
		currentMap, ok := currentVal.(map[string]interface{})
		if !ok { return nil, false }
		currentVal, ok = currentMap[key]
		if !ok { return nil, false }
		if i == len(keys)-1 { return currentVal, true }
	}
	return currentVal, true
}

// removeNestedField removes a field from a potentially nested map structure.
func removeNestedField(data map[string]interface{}, path string) {
    keys := strings.Split(path, ".")
    currentMap := data

    for i, key := range keys {
        if i == len(keys)-1 {
            // Last key, delete it from the current map
            delete(currentMap, key)
            return
        }

        // Not the last key, navigate down
        nextVal, ok := currentMap[key]
        if !ok {
            // Path doesn't exist, nothing to remove
            return
        }
        nextMap, ok := nextVal.(map[string]interface{})
        if !ok {
            // Path exists but leads to a non-map value before the end
            // Cannot remove the target field
            return
        }
        currentMap = nextMap // Move to the next map level
    }
}


// dedupRecords removes duplicates based on config.
func (p *processorImpl) dedupRecords(records []map[string]interface{}) []map[string]interface{} {
	seen := make(map[string]map[string]interface{})
	keys := p.dedupCfg.Keys
	sort.Strings(keys)
	lcStrategy := strings.ToLower(p.dedupCfg.Strategy)
	strategyField := p.dedupCfg.StrategyField
	placeholder := "<ETL_NIL_OR_MISSING>"

	for _, currentRec := range records {
		var compositeKeyParts []string
		for _, key := range keys {
			lookupValue, lookupOK := getNestedField(currentRec, key)
			if lookupOK && lookupValue != nil { compositeKeyParts = append(compositeKeyParts, transform.ValueToStringForHash(lookupValue)) } else { compositeKeyParts = append(compositeKeyParts, placeholder) }
		}
		compositeKey := strings.Join(compositeKeyParts, "||")

		storedRec, keyExists := seen[compositeKey]
		keepCurrent := false
		if !keyExists { keepCurrent = true } else {
			switch lcStrategy {
			case config.DedupStrategyFirst: break
			case config.DedupStrategyLast: keepCurrent = true
			case config.DedupStrategyMin, config.DedupStrategyMax:
				currentVal, currentOk := getNestedField(currentRec, strategyField)
				storedVal, storedOk := getNestedField(storedRec, strategyField)
				if !currentOk { logging.Logf(logging.Warning, "Dedupe (%s): Field '%s' missing from current record for key '%s'. Keeping stored record.", lcStrategy, strategyField, compositeKey) } else if !storedOk { logging.Logf(logging.Warning, "Dedupe (%s): Field '%s' missing from stored record for key '%s'. Replacing with current record.", lcStrategy, strategyField, compositeKey); keepCurrent = true } else {
					comparisonResult, err := transform.CompareValues(currentVal, storedVal)
					if err != nil { logging.Logf(logging.Warning, "Dedupe (%s): Cannot compare strategy field '%s' for key '%s': %v. Keeping stored record.", lcStrategy, strategyField, compositeKey, err) } else { if (lcStrategy == config.DedupStrategyMin && comparisonResult < 0) || (lcStrategy == config.DedupStrategyMax && comparisonResult > 0) { keepCurrent = true } }
				}
			default: logging.Logf(logging.Error, "Dedupe: Internal error - unknown strategy '%s'. Key '%s'. Keeping first.", p.dedupCfg.Strategy, compositeKey); if !keyExists { keepCurrent = true }
			}
		}
		if keepCurrent { seen[compositeKey] = currentRec }
	}
	uniqueRecords := make([]map[string]interface{}, 0, len(seen))
	for _, record := range seen { uniqueRecords = append(uniqueRecords, record) }
	return uniqueRecords
}
