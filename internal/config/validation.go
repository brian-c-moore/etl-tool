package config

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"etl-tool/internal/logging"

	"github.com/Knetic/govaluate"
)

// Define known valid enum values for configuration fields.
var (
	knownLogLevels          = []string{"none", "error", "warn", "warning", "info", "debug"}
	knownSourceTypes        = []string{SourceTypeJSON, SourceTypeCSV, SourceTypeXLSX, SourceTypeXML, SourceTypeYAML, SourceTypePostgres}
	knownDestinationTypes   = []string{DestinationTypeJSON, DestinationTypeCSV, DestinationTypeXLSX, DestinationTypeXML, DestinationTypeYAML, DestinationTypePostgres}
	knownLoaderModes        = []string{"", LoaderModeSQL}
	knownErrorModes         = []string{ErrorHandlingModeHalt, ErrorHandlingModeSkip}
	knownDedupStrategies    = []string{DedupStrategyFirst, DedupStrategyLast, DedupStrategyMin, DedupStrategyMax}
	knownHashAlgorithms     = []string{"sha256", "sha512", "md5"} // FIPS mode check happens during validation logic
	knownTransformBaseFuncs = []string{
		// Permissive transformations
		"epochToDate", "calculateAge", "regexExtract", "trim", "toUpperCase",
		"toLowerCase", "branch", "dateConvert", "multiDateConvert", "toInt",
		"toFloat", "toBool", "toString", "replaceAll", "substring", "coalesce",
		"hash",
		// Strict transformations
		"musttoint", "musttofloat", "musttobool", "mustepochtodate", "mustdateconvert",
		// Validations
		"validateRequired", "validateRegex", "validateNumericRange",
		"validateAllowedValues",
	}
)

// isValidEnumValue checks if a value is present in a list of allowed string values (case-insensitive).
func isValidEnumValue(value string, allowedValues []string) bool {
	lowerValue := strings.ToLower(value)
	for _, allowed := range allowedValues {
		if lowerValue == strings.ToLower(allowed) {
			return true
		}
	}
	return false
}

// ValidateConfig performs comprehensive validation of the entire ETL configuration.
func ValidateConfig(cfg *ETLConfig) error {
	var allErrors []string

	if !isValidEnumValue(cfg.Logging.Level, knownLogLevels) {
		allErrors = append(allErrors, fmt.Sprintf("- Config.Logging.Level: invalid log level '%s', must be one of %v", cfg.Logging.Level, knownLogLevels))
	}

	allErrors = append(allErrors, validateSourceConfig("Config.Source", &cfg.Source)...)
	allErrors = append(allErrors, validateDestinationConfig("Config.Destination", &cfg.Destination)...)

	if cfg.Filter != "" {
		if _, err := govaluate.NewEvaluableExpression(cfg.Filter); err != nil {
			allErrors = append(allErrors, fmt.Sprintf("- Config.Filter: invalid expression syntax: %v", err))
		}
	}

	// Store defined target fields to check dependencies and duplicates
	mappingTargetFields := make(map[string]bool)
	if len(cfg.Mappings) == 0 {
		allErrors = append(allErrors, "- Config.Mappings: at least one mapping rule is required")
	} else {
		for i, rule := range cfg.Mappings {
			ruleCopy := rule // Work with a copy if needed, though validation doesn't modify
			// Check for shorthand parameter usage (e.g., "regexExtract:pattern")
			parts := strings.SplitN(rule.Transform, ":", 2)
			// Determine if a non-empty shorthand value was provided
			hasShorthandValue := len(parts) == 2 && strings.TrimSpace(parts[1]) != ""
			// Pass the original transform string and other info to the validation function
			allErrors = append(allErrors, validateMappingRule(fmt.Sprintf("Config.Mappings[%d]", i), &ruleCopy, cfg.FIPSMode, hasShorthandValue)...)

			// Check for duplicate target field definitions
			if _, exists := mappingTargetFields[rule.Target]; exists {
				allErrors = append(allErrors, fmt.Sprintf("- Config.Mappings[%d].Target: duplicate target field '%s' defined", i, rule.Target))
			}
			if rule.Target != "" {
				mappingTargetFields[rule.Target] = true
			}
		}
	}

	// Flattening Validation ---
	if cfg.Flattening != nil {
		allErrors = append(allErrors, validateFlatteningConfig("Config.Flattening", cfg.Flattening, mappingTargetFields)...)
	}

	if cfg.Dedup != nil {
		// Pass mapping targets for dedup field validation
		allErrors = append(allErrors, validateDedupConfig("Config.Dedup", cfg.Dedup, mappingTargetFields)...)
	}

	if cfg.ErrorHandling != nil {
		allErrors = append(allErrors, validateErrorHandlingConfig("Config.ErrorHandling", cfg.ErrorHandling)...)
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("configuration validation failed:\n%s", strings.Join(allErrors, "\n"))
	}
	logging.Logf(logging.Debug, "Configuration validation successful.")
	return nil
}

// validateSourceConfig validates the Source section of the configuration.
func validateSourceConfig(prefix string, cfg *SourceConfig) []string {
	var errs []string
	if cfg.Type == "" {
		errs = append(errs, fmt.Sprintf("- %s.Type: is required", prefix))
	} else if !isValidEnumValue(cfg.Type, knownSourceTypes) {
		errs = append(errs, fmt.Sprintf("- %s.Type: invalid source type '%s', must be one of %v", prefix, cfg.Type, knownSourceTypes))
		return errs // Stop further source validation if type is invalid
	}

	lcType := strings.ToLower(cfg.Type)
	isPostgres := lcType == SourceTypePostgres
	isFileBased := !isPostgres // JSON, CSV, XLSX, XML, YAML

	if isFileBased {
		if cfg.File == "" {
			errs = append(errs, fmt.Sprintf("- %s.File: is required for source type '%s'", prefix, cfg.Type))
		}
		if cfg.Query != "" {
			logging.Logf(logging.Warning, "Validation: %s.Query is specified but will be ignored for source type '%s'", prefix, cfg.Type)
		}
	} else { // isPostgres
		if cfg.Query == "" {
			errs = append(errs, fmt.Sprintf("- %s.Query: is required for source type 'postgres'", prefix))
		}
		if cfg.File != "" {
			logging.Logf(logging.Warning, "Validation: %s.File is specified but will be ignored for source type 'postgres'", prefix)
		}
	}

	// Format-specific checks
	switch lcType {
	case SourceTypeCSV:
		if err := validateSingleRuneString(cfg.Delimiter, fmt.Sprintf("%s.Delimiter", prefix), false); err != nil {
			errs = append(errs, err.Error())
		}
		// CommentChar can be empty
		if err := validateSingleRuneString(cfg.CommentChar, fmt.Sprintf("%s.CommentChar", prefix), true); err != nil {
			errs = append(errs, err.Error())
		}
	case SourceTypeXLSX:
		if cfg.SheetName != "" {
			if err := validateSheetName(cfg.SheetName, fmt.Sprintf("%s.SheetName", prefix)); err != nil {
				errs = append(errs, err.Error())
			}
		}
		if cfg.SheetIndex != nil && *cfg.SheetIndex < 0 {
			errs = append(errs, fmt.Sprintf("- %s.SheetIndex: cannot be negative", prefix))
		}
		if cfg.SheetName != "" && cfg.SheetIndex != nil {
			logging.Logf(logging.Warning, "Validation: Both %s.SheetName ('%s') and %s.SheetIndex (%d) are specified. SheetName will be used.", prefix, cfg.SheetName, prefix, *cfg.SheetIndex)
		}
	case SourceTypeXML:
		// Default is applied if empty, so only validate if *set* to something invalid
		if cfg.XMLRecordTag != "" {
			if err := validateXMLName(cfg.XMLRecordTag); err != nil {
				errs = append(errs, fmt.Sprintf("- %s.XMLRecordTag: %v", prefix, err))
			}
		}
	case SourceTypeYAML, SourceTypeJSON, SourceTypePostgres:
		// No specific format options to validate currently
	}

	// Check for unused options specific to other formats
	validateUnusedFormatOptions(prefix, cfg.Type, cfg)
	return errs
}

// validateDestinationConfig validates the Destination section of the configuration.
func validateDestinationConfig(prefix string, cfg *DestinationConfig) []string {
	var errs []string
	if cfg.Type == "" {
		errs = append(errs, fmt.Sprintf("- %s.Type: is required", prefix))
	} else if !isValidEnumValue(cfg.Type, knownDestinationTypes) {
		// Stop further destination validation if type is invalid
		errs = append(errs, fmt.Sprintf("- %s.Type: invalid destination type '%s', must be one of %v", prefix, cfg.Type, knownDestinationTypes))
		return errs
	}

	lcType := strings.ToLower(cfg.Type)
	isPostgres := lcType == DestinationTypePostgres

	if isPostgres {
		if cfg.TargetTable == "" {
			errs = append(errs, fmt.Sprintf("- %s.TargetTable: is required for destination type 'postgres'", prefix))
		}
		if cfg.File != "" {
			logging.Logf(logging.Warning, "Validation: %s.File is specified but will be ignored for destination type 'postgres'", prefix)
		}
		if cfg.Loader != nil {
			errs = append(errs, validateLoaderConfig(prefix+".Loader", cfg.Loader)...)
		}
	} else { // isFileBased
		if cfg.File == "" {
			errs = append(errs, fmt.Sprintf("- %s.File: is required for destination type '%s'", prefix, cfg.Type))
		}
		if cfg.TargetTable != "" {
			logging.Logf(logging.Warning, "Validation: %s.TargetTable is specified but will be ignored for destination type '%s'", prefix, cfg.Type)
		}
		if cfg.Loader != nil {
			logging.Logf(logging.Warning, "Validation: %s.Loader is specified but will be ignored for destination type '%s'", prefix, cfg.Type)
		}
	}

	// Format-specific checks
	switch lcType {
	case DestinationTypeCSV:
		if err := validateSingleRuneString(cfg.Delimiter, fmt.Sprintf("%s.Delimiter", prefix), false); err != nil {
			errs = append(errs, err.Error())
		}
	case DestinationTypeXLSX:
		// Default is applied if empty, so only validate if *set* to something invalid
		if cfg.SheetName != "" {
			if err := validateSheetName(cfg.SheetName, fmt.Sprintf("%s.SheetName", prefix)); err != nil {
				errs = append(errs, err.Error())
			}
		}
	case DestinationTypeXML:
		// Default is applied if empty, so only validate if *set* to something invalid
		if cfg.XMLRecordTag != "" {
			if err := validateXMLName(cfg.XMLRecordTag); err != nil {
				errs = append(errs, fmt.Sprintf("- %s.XMLRecordTag: %v", prefix, err))
			}
		}
		if cfg.XMLRootTag != "" {
			if err := validateXMLName(cfg.XMLRootTag); err != nil {
				errs = append(errs, fmt.Sprintf("- %s.XMLRootTag: %v", prefix, err))
			}
		}
	case DestinationTypeYAML, DestinationTypeJSON, DestinationTypePostgres:
		// No specific format options to validate currently
	}

	// Check for unused options specific to other formats
	validateUnusedFormatOptions(prefix, cfg.Type, cfg)
	return errs
}

// validateLoaderConfig validates the PostgreSQL Loader settings.
func validateLoaderConfig(prefix string, cfg *LoaderConfig) []string {
	var errs []string
	lcMode := strings.ToLower(cfg.Mode)
	if lcMode != "" && !isValidEnumValue(lcMode, knownLoaderModes) {
		errs = append(errs, fmt.Sprintf("- %s.Mode: invalid loader mode '%s', must be '%s' or empty (for COPY)", prefix, cfg.Mode, LoaderModeSQL))
	}

	if lcMode == LoaderModeSQL {
		if cfg.Command == "" {
			errs = append(errs, fmt.Sprintf("- %s.Command: is required when loader mode is 'sql'", prefix))
		}
		// Preload/Postload/BatchSize are valid only in SQL mode
	} else {
		// Log warnings if SQL-specific options are set without SQL mode
		if cfg.Command != "" {
			logging.Logf(logging.Warning, "Validation: %s.Command is specified but will be ignored when loader mode is not 'sql'", prefix)
		}
		if len(cfg.Preload) > 0 {
			logging.Logf(logging.Warning, "Validation: %s.Preload is specified but will be ignored when loader mode is not 'sql'", prefix)
		}
		if len(cfg.Postload) > 0 {
			logging.Logf(logging.Warning, "Validation: %s.Postload is specified but will be ignored when loader mode is not 'sql'", prefix)
		}
		if cfg.BatchSize != DefaultLoaderBatchSize && cfg.BatchSize > 0 { // Allow default value
			logging.Logf(logging.Warning, "Validation: %s.BatchSize is specified but will be ignored when loader mode is not 'sql'", prefix)
		}
	}
	// Validate BatchSize range regardless of mode (simplifies logic)
	if cfg.BatchSize < 0 {
		errs = append(errs, fmt.Sprintf("- %s.BatchSize: cannot be negative", prefix))
	}
	return errs
}

// validateMappingRule validates a single mapping rule.
// hasShorthandValue indicates if rule.Transform contained a non-empty value after ':'.
func validateMappingRule(prefix string, rule *MappingRule, fipsEnabled bool, hasShorthandValue bool) []string {
	var errs []string
	if rule.Source == "" {
		errs = append(errs, fmt.Sprintf("- %s.Source: is required", prefix))
	}
	if rule.Target == "" {
		errs = append(errs, fmt.Sprintf("- %s.Target: is required", prefix))
	}

	if rule.Transform != "" {
		parts := strings.SplitN(rule.Transform, ":", 2)
		baseFunc := strings.ToLower(parts[0])

		if !isValidEnumValue(baseFunc, knownTransformBaseFuncs) {
			errs = append(errs, fmt.Sprintf("- %s.Transform: unknown base transformation function '%s'", prefix, baseFunc))
		} else {
			// Validate parameters specific to the known function
			// Pass the original transform string for potential re-splitting
			paramErrs := validateTransformParams(prefix, baseFunc, rule.Transform, rule.Params, fipsEnabled, hasShorthandValue)
			errs = append(errs, paramErrs...)

			// Specific check for FIPS mode and MD5 hash
			if baseFunc == "hash" && fipsEnabled {
				if algoRaw, exists := rule.Params["algorithm"]; exists {
					if algo, isString := algoRaw.(string); isString && strings.ToLower(algo) == "md5" {
						errMsg := fmt.Sprintf("- %s.Params: hash algorithm 'md5' is not allowed in FIPS mode", prefix)
						alreadyReported := false
						for _, e := range paramErrs {
							if e == errMsg {
								alreadyReported = true
								break
							}
						}
						if !alreadyReported {
							errs = append(errs, errMsg)
						}
					}
				}
			}
		}
	}
	return errs
}

// validateTransformParams checks parameters for specific transformation functions.
// transformString is the original string from the config (e.g., "regexExtract:pattern").
// hasShorthandValue indicates if the transform string provided a value after ':'.
func validateTransformParams(prefix, funcName, transformString string, params map[string]interface{}, fipsEnabled bool, hasShorthandValue bool) []string {
	var errs []string

	// Helper: Checks for required keys, considering shorthand alternatives.
	expectParams := func(keys ...string) {
		for _, key := range keys {
			// Check if the key exists explicitly in the params map
			_, explicitParamExists := params[key]

			// Determine if shorthand *can* satisfy this specific key
			canUseShorthandForKey := (funcName == "regexextract" || funcName == "validateregex") && key == "pattern"

			// Report missing parameter only if it's not present explicitly AND
			// (shorthand wasn't used OR shorthand cannot satisfy this key)
			if !explicitParamExists && !(hasShorthandValue && canUseShorthandForKey) {
				shorthandMsg := ""
				if canUseShorthandForKey {
					shorthandMsg = " (and not provided via shorthand)"
				}
				errs = append(errs, fmt.Sprintf("- %s.Params: missing required parameter '%s' for transform '%s'%s", prefix, key, funcName, shorthandMsg))
			}
		}
	}

	expectStringParam := func(key string, allowEmpty bool) {
		if params != nil {
			if val, ok := params[key]; ok {
				strVal, isStr := val.(string)
				if !isStr {
					errs = append(errs, fmt.Sprintf("- %s.Params: parameter '%s' must be a string for transform '%s'", prefix, key, funcName))
				} else if !allowEmpty && strVal == "" {
					errs = append(errs, fmt.Sprintf("- %s.Params: parameter '%s' cannot be an empty string for transform '%s'", prefix, key, funcName))
				}
			}
		}
	}

	expectIntParam := func(key string) {
		if params != nil {
			if val, ok := params[key]; ok {
				if _, isValidInt := parseParamAsInt(val); !isValidInt {
					errs = append(errs, fmt.Sprintf("- %s.Params: parameter '%s' must be a valid integer for transform '%s'", prefix, key, funcName))
				}
			}
		}
	}

	expectNumberParam := func(key string) {
		if params != nil {
			if val, ok := params[key]; ok {
				if _, isValidNum := parseParamAsNumber(val); !isValidNum {
					errs = append(errs, fmt.Sprintf("- %s.Params: parameter '%s' must be a valid number for transform '%s'", prefix, key, funcName))
				}
			}
		}
	}

	expectSliceParam := func(key string, allowEmpty bool) {
		if params != nil {
			if val, ok := params[key]; ok {
				sliceVal := reflect.ValueOf(val)
				if sliceVal.Kind() != reflect.Slice {
					errs = append(errs, fmt.Sprintf("- %s.Params: parameter '%s' must be a slice/array for transform '%s'", prefix, key, funcName))
				} else if !allowEmpty && sliceVal.Len() == 0 {
					errs = append(errs, fmt.Sprintf("- %s.Params: parameter '%s' cannot be an empty slice/array for transform '%s'", prefix, key, funcName))
				}
			}
		}
	}

	// --- Function-specific validations ---
	switch funcName {
	case "regexextract", "validateregex":
		// Use expectParams to report error if pattern is missing and wasn't provided via shorthand.
		expectParams("pattern")

		// Check type and validity if pattern exists (either explicitly or via shorthand)
		patternVal, patternInParams := params["pattern"]
		patternIsFromShorthand := !patternInParams && hasShorthandValue

		if patternInParams {
			expectStringParam("pattern", false) // Validate type only if explicit
			if patternStr, isStr := patternVal.(string); isStr && patternStr != "" {
				if _, err := regexp.Compile(patternStr); err != nil {
					errs = append(errs, fmt.Sprintf("- %s.Params: invalid regex pattern for '%s': %v", prefix, funcName, err))
				}
			}
		} else if patternIsFromShorthand {
			// If pattern came from shorthand, assume it's a string, just check regex validity.
			// Use the passed transformString to get the shorthand value
			parts := strings.SplitN(transformString, ":", 2) // Re-split here
			if len(parts) == 2 {
				shorthandPattern := strings.TrimSpace(parts[1])
				if shorthandPattern == "" {
					// This case should have been caught by expectParams, but double-check.
					// Error message added in expectParams, no need to repeat here.
				} else if _, err := regexp.Compile(shorthandPattern); err != nil {
					errs = append(errs, fmt.Sprintf("- %s.Params: invalid regex pattern '%s' (from shorthand) for '%s': %v", prefix, shorthandPattern, funcName, err))
				}
			}
		}

	case "dateconvert", "mustdateconvert":
		// Params are optional; check type only if provided
		if params != nil {
			if _, ok := params["inputFormat"]; ok {
				expectStringParam("inputFormat", false)
			}
			if _, ok := params["outputFormat"]; ok {
				expectStringParam("outputFormat", true) // Allow empty outputFormat
			}
		}
	case "multidateconvert":
		expectParams("formats", "outputFormat")
		expectSliceParam("formats", false)
		expectStringParam("outputFormat", false)
		if params != nil {
			if fmtsRaw, ok := params["formats"]; ok {
				if fmts, isSlice := fmtsRaw.([]interface{}); isSlice {
					for i, fmtInterface := range fmts {
						if strFmt, isStr := fmtInterface.(string); !isStr || strFmt == "" {
							errs = append(errs, fmt.Sprintf("- %s.Params.formats[%d]: item must be a non-empty string format", prefix, i))
						}
					}
				}
			}
		}
	case "replaceall":
		expectParams("old", "new")
		expectStringParam("old", true) // Allow empty 'old' string
		expectStringParam("new", true) // Allow empty 'new' string
	case "substring":
		expectParams("start", "length")
		expectIntParam("start")
		expectIntParam("length")
	case "coalesce":
		expectParams("fields")
		expectSliceParam("fields", false)
		if params != nil {
			if fieldsRaw, ok := params["fields"]; ok {
				if fields, isSlice := fieldsRaw.([]interface{}); isSlice {
					for i, fieldInterface := range fields {
						if strField, isStr := fieldInterface.(string); !isStr || strField == "" {
							errs = append(errs, fmt.Sprintf("- %s.Params.fields[%d]: item must be a non-empty string field name", prefix, i))
						}
					}
				}
			}
		}
	case "branch":
		expectParams("branches")
		expectSliceParam("branches", false)
		if params != nil {
			if branchesRaw, ok := params["branches"]; ok {
				if branches, isSlice := branchesRaw.([]interface{}); isSlice {
					for i, branch := range branches {
						branchPrefix := fmt.Sprintf("%s.Params.branches[%d]", prefix, i)
						if branchMap, isMap := branch.(map[string]interface{}); isMap {
							if condRaw, condExists := branchMap["condition"]; !condExists {
								errs = append(errs, fmt.Sprintf("- %s: missing required key 'condition'", branchPrefix))
							} else {
								if condStr, isStr := condRaw.(string); !isStr || condStr == "" {
									errs = append(errs, fmt.Sprintf("- %s: 'condition' must be a non-empty string", branchPrefix))
								} else {
									if _, err := govaluate.NewEvaluableExpression(condStr); err != nil {
										errs = append(errs, fmt.Sprintf("- %s: invalid condition syntax: %v", branchPrefix, err))
									}
								}
							}
							if _, valExists := branchMap["value"]; !valExists {
								errs = append(errs, fmt.Sprintf("- %s: missing required key 'value'", branchPrefix))
							}
						} else {
							errs = append(errs, fmt.Sprintf("- %s: must be a map with 'condition' and 'value' keys", branchPrefix))
						}
					}
				}
			}
		}
	case "hash":
		expectParams("fields", "algorithm")
		expectStringParam("algorithm", false)
		expectSliceParam("fields", false)
		if params != nil {
			if algoRaw, ok := params["algorithm"]; ok {
				if algo, isStr := algoRaw.(string); isStr {
					if !isValidEnumValue(algo, knownHashAlgorithms) {
						errs = append(errs, fmt.Sprintf("- %s.Params: unknown hash algorithm '%s', must be one of %v", prefix, algo, knownHashAlgorithms))
					} else if fipsEnabled && strings.ToLower(algo) == "md5" {
						errs = append(errs, fmt.Sprintf("- %s.Params: hash algorithm 'md5' is not allowed in FIPS mode", prefix))
					}
				}
			}
			if fieldsRaw, ok := params["fields"]; ok {
				if fields, isSlice := fieldsRaw.([]interface{}); isSlice {
					for i, fieldInterface := range fields {
						if strField, isStr := fieldInterface.(string); !isStr || strField == "" {
							errs = append(errs, fmt.Sprintf("- %s.Params.fields[%d]: item must be a non-empty string field name", prefix, i))
						}
					}
				}
			}
		}
	case "validaterequired":
		// No parameters needed
		break
	case "validatenumericrange":
		minExists, maxExists := false, false
		if params != nil {
			_, minExists = params["min"]
			_, maxExists = params["max"]
		}
		if !minExists && !maxExists {
			errs = append(errs, fmt.Sprintf("- %s.Params: requires at least 'min' or 'max' for '%s'", prefix, funcName))
		}
		if minExists {
			expectNumberParam("min")
		}
		if maxExists {
			expectNumberParam("max")
		}
		if minExists && maxExists && params != nil {
			if minVal, minOK := parseParamAsNumber(params["min"]); minOK {
				if maxVal, maxOK := parseParamAsNumber(params["max"]); maxOK {
					if minVal > maxVal {
						errs = append(errs, fmt.Sprintf("- %s.Params: 'min' value (%v) cannot be greater than 'max' value (%v)", prefix, minVal, maxVal))
					}
				}
			}
		}
	case "validateallowedvalues":
		expectParams("values")
		expectSliceParam("values", false)
	// Functions without parameters
	case "epochtodate", "calculateage", "trim", "touppercase", "tolowercase",
		"toint", "tofloat", "tobool", "tostring",
		"musttoint", "musttofloat", "musttobool", "mustepochtodate":
		if len(params) > 0 {
			logging.Logf(logging.Warning, "Validation: %s.Params are specified but ignored for transform '%s'", prefix, funcName)
		}
	default:
		// Should not happen if knownTransformBaseFuncs is maintained
		logging.Logf(logging.Error, "Validation internal error: Reached default case in validateTransformParams for known function '%s'", funcName)
	}
	return errs
}

// Flattening Validation ---
// validateFlatteningConfig validates the Flattening section.
func validateFlatteningConfig(prefix string, cfg *FlatteningConfig, mappingTargets map[string]bool) []string {
	var errs []string
	if cfg.SourceField == "" {
		errs = append(errs, fmt.Sprintf("- %s.SourceField: is required", prefix))
	}
	if cfg.TargetField == "" {
		errs = append(errs, fmt.Sprintf("- %s.TargetField: is required", prefix))
	}

	// Check if ConditionField is set without ConditionValue
	if cfg.ConditionField != "" && cfg.ConditionValue == "" {
		// Changed from warning to error as per plan review
		errs = append(errs, fmt.Sprintf("- %s.ConditionValue: is required when ConditionField ('%s') is set", prefix, cfg.ConditionField))
	}

	// Check if Flattening.TargetField conflicts with any MappingRule.Target
	if cfg.TargetField != "" {
		if _, exists := mappingTargets[cfg.TargetField]; exists {
			errs = append(errs, fmt.Sprintf("- %s.TargetField: '%s' conflicts with a target field defined in mappings", prefix, cfg.TargetField))
		}
	}

	// Note: Validating if Flattening.TargetField conflicts with parent fields
	// when IncludeParent is true is difficult at config time and is deferred to runtime/documentation.

	return errs
}

// validateDedupConfig validates the Deduplication section.
func validateDedupConfig(prefix string, cfg *DedupConfig, mappingTargets map[string]bool) []string {
	var errs []string
	if len(cfg.Keys) == 0 {
		errs = append(errs, fmt.Sprintf("- %s.Keys: requires at least one key for deduplication", prefix))
	} else {
		// Check if keys are valid target fields from mappings
		for i, key := range cfg.Keys {
			if key == "" {
				errs = append(errs, fmt.Sprintf("- %s.Keys[%d]: key cannot be empty", prefix, i))
			}
			// Check against mapping targets AND the flattening target field if it exists
			// (Dedup happens after flattening)
			// Note: We don't have flattening target here. Revisit if needed, but likely okay
			// as dedup keys refer to fields in the *final* record structure before writing.
			// The user needs to ensure the keys exist post-mapping/flattening.
			// We only warn if it's not a MAPPING target for now.
			if _, isMappingTarget := mappingTargets[key]; !isMappingTarget {
				logging.Logf(logging.Warning, "Validation: %s.Keys[%d]: key '%s' is not an explicit target field in mappings. Ensure it exists in the final processed record.", prefix, i, key)
			}
		}
	}

	// Validate strategy
	if cfg.Strategy == "" {
		// Default will be applied, no error
	} else if !isValidEnumValue(cfg.Strategy, knownDedupStrategies) {
		errs = append(errs, fmt.Sprintf("- %s.Strategy: invalid strategy '%s', must be one of %v", prefix, cfg.Strategy, knownDedupStrategies))
	} else {
		// Check strategyField requirement
		lcStrategy := strings.ToLower(cfg.Strategy)
		if lcStrategy == DedupStrategyMin || lcStrategy == DedupStrategyMax {
			if cfg.StrategyField == "" {
				errs = append(errs, fmt.Sprintf("- %s.StrategyField: is required when strategy is '%s' or '%s'", prefix, DedupStrategyMin, DedupStrategyMax))
			} else {
				// Similar check for StrategyField's existence in mapping targets
				if _, isMappingTarget := mappingTargets[cfg.StrategyField]; !isMappingTarget {
					logging.Logf(logging.Warning, "Validation: %s.StrategyField: field '%s' is not an explicit target field in mappings. Ensure it exists for comparison.", prefix, cfg.StrategyField)
				}
			}
		} else {
			// Strategy is 'first' or 'last', StrategyField should not be set
			if cfg.StrategyField != "" {
				logging.Logf(logging.Warning, "Validation: %s.StrategyField ('%s') is specified but will be ignored when strategy is '%s'", prefix, cfg.StrategyField, cfg.Strategy)
			}
		}
	}
	return errs
}

// validateErrorHandlingConfig validates the ErrorHandling section.
func validateErrorHandlingConfig(prefix string, cfg *ErrorHandlingConfig) []string {
	var errs []string
	if !isValidEnumValue(cfg.Mode, knownErrorModes) {
		errs = append(errs, fmt.Sprintf("- %s.Mode: invalid error handling mode '%s', must be one of %v", prefix, cfg.Mode, knownErrorModes))
	}

	// Check dependent options based on mode
	if cfg.Mode == ErrorHandlingModeHalt {
		if cfg.LogErrors != nil {
			logging.Logf(logging.Warning, "Validation: %s.LogErrors is specified but will be ignored when mode is '%s'", prefix, ErrorHandlingModeHalt)
		}
		if cfg.ErrorFile != "" {
			logging.Logf(logging.Warning, "Validation: %s.ErrorFile is specified but will be ignored when mode is '%s'", prefix, ErrorHandlingModeHalt)
		}
	} else if cfg.Mode == ErrorHandlingModeSkip {
		// LogErrors defaults to true if nil, nothing to validate there.
		// Validate ErrorFile path if provided
		if cfg.ErrorFile != "" {
			// Basic check: path should not end with a separator, suggesting a directory
			if strings.HasSuffix(cfg.ErrorFile, "/") || strings.HasSuffix(cfg.ErrorFile, "\\") {
				errs = append(errs, fmt.Sprintf("- %s.ErrorFile: path '%s' appears to be a directory, not a file", prefix, cfg.ErrorFile))
			}
		}
	}
	return errs
}

// validateSingleRuneString checks if a string contains exactly one UTF-8 rune.
func validateSingleRuneString(s, fieldName string, allowEmpty bool) error {
	if s == "" {
		if !allowEmpty {
			return fmt.Errorf("- %s: cannot be empty", fieldName)
		}
		return nil // Empty is allowed
	}
	if utf8.RuneCountInString(s) != 1 {
		return fmt.Errorf("- %s: '%s' must be a single character", fieldName, strconv.Quote(s))
	}
	return nil
}

// validateSheetName checks if an Excel sheet name is valid according to Excel limitations.
func validateSheetName(sheetName, fieldName string) error {
	if sheetName == "" {
		return fmt.Errorf("- %s: sheet name cannot be empty", fieldName)
	}
	if utf8.RuneCountInString(sheetName) > 31 {
		return fmt.Errorf("- %s: '%s' exceeds maximum length of 31 characters", fieldName, sheetName)
	}
	// Check for invalid characters as defined by Excel documentation
	if strings.ContainsAny(sheetName, `:\/?*[]`) {
		return fmt.Errorf("- %s: '%s' contains invalid characters (: \\ / ? * [ ])", fieldName, sheetName)
	}
	// Check if name starts or ends with a single quote (apostrophe)
	if strings.HasPrefix(sheetName, "'") || strings.HasSuffix(sheetName, "'") {
		return fmt.Errorf("- %s: '%s' cannot start or end with a single quote", fieldName, sheetName)
	}
	return nil
}

// validateXMLName checks if a string is a valid XML name (simplified check based on common issues).
func validateXMLName(name string) error {
	if name == "" {
		return fmt.Errorf("invalid XML name: cannot be empty")
	}
	// Check for common invalid characters in XML names
	if strings.ContainsAny(name, " <>/?!=\"'#%&+,;@^`~(){}|\\") {
		return fmt.Errorf("invalid XML name '%s': contains invalid characters", name)
	}
	// Check if starts with a number or hyphen-minus
	if r, _ := utf8.DecodeRuneInString(name); (r >= '0' && r <= '9') || r == '-' {
		return fmt.Errorf("invalid XML name '%s': cannot start with a digit or hyphen", name)
	}
	// Check for "xml" prefix (case-insensitive) which is reserved
	if len(name) >= 3 && strings.ToLower(name[:3]) == "xml" {
		return fmt.Errorf("invalid XML name '%s': cannot start with 'xml'", name)
	}
	return nil
}

// validateUnusedFormatOptions logs warnings if format-specific options are present for the wrong type.
func validateUnusedFormatOptions(prefix, actualType string, cfg interface{}) {
	lcActualType := strings.ToLower(actualType)
	v := reflect.ValueOf(cfg)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return // Should not happen with Source/DestinationConfig
	}

	// Check CSV options
	if lcActualType != SourceTypeCSV && lcActualType != DestinationTypeCSV {
		if isFieldSet(v, "Delimiter") {
			logging.Logf(logging.Warning, "Validation: %s.Delimiter is specified but will be ignored for type '%s'", prefix, actualType)
		}
		// CommentChar is source-specific
		if _, isSource := cfg.(*SourceConfig); isSource && isFieldSet(v, "CommentChar") {
			logging.Logf(logging.Warning, "Validation: %s.CommentChar is specified but will be ignored for type '%s'", prefix, actualType)
		}
	}

	// Check XLSX options
	if lcActualType != SourceTypeXLSX && lcActualType != DestinationTypeXLSX {
		if isFieldSet(v, "SheetName") {
			logging.Logf(logging.Warning, "Validation: %s.SheetName is specified but will be ignored for type '%s'", prefix, actualType)
		}
		// SheetIndex is source-specific
		if _, isSource := cfg.(*SourceConfig); isSource && isFieldSet(v, "SheetIndex") {
			logging.Logf(logging.Warning, "Validation: %s.SheetIndex is specified but will be ignored for type '%s'", prefix, actualType)
		}
	}

	// Check XML options
	if lcActualType != SourceTypeXML && lcActualType != DestinationTypeXML {
		if isFieldSet(v, "XMLRecordTag") {
			logging.Logf(logging.Warning, "Validation: %s.XMLRecordTag is specified but will be ignored for type '%s'", prefix, actualType)
		}
		// XMLRootTag is destination-specific
		if _, isDest := cfg.(*DestinationConfig); isDest && isFieldSet(v, "XMLRootTag") {
			logging.Logf(logging.Warning, "Validation: %s.XMLRootTag is specified but will be ignored for type '%s'", prefix, actualType)
		}
	}
}

// isFieldSet checks if a field in a struct has a non-zero/non-empty value.
func isFieldSet(v reflect.Value, fieldName string) bool {
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return false // Field doesn't exist
	}
	return !field.IsZero()
}

// --- Parameter Parsing Helpers (used within validation) ---

// parseParamAsInt parses various numeric types or string representations into an int.
func parseParamAsInt(v interface{}) (int, bool) {
	const maxIntPlatform = int(^uint(0) >> 1)
	const minIntPlatform = -maxIntPlatform - 1

	var i64 int64
	var ok bool

	switch val := v.(type) {
	case int:
		i64, ok = int64(val), true
	case int8:
		i64, ok = int64(val), true
	case int16:
		i64, ok = int64(val), true
	case int32:
		i64, ok = int64(val), true
	case int64:
		i64, ok = val, true
	case uint:
		if uint64(val) <= uint64(math.MaxInt64) {
			i64, ok = int64(val), true
		} else {
			ok = false
		}
	case uint8:
		i64, ok = int64(val), true
	case uint16:
		i64, ok = int64(val), true
	case uint32:
		i64, ok = int64(val), true
	case uint64:
		if val <= uint64(math.MaxInt64) {
			i64, ok = int64(val), true
		} else {
			ok = false
		}
	case float32:
		if float32(int64(val)) == val && val >= float32(math.MinInt64) && val <= float32(math.MaxInt64) {
			i64, ok = int64(val), true
		} else {
			ok = false
		}
	case float64:
		if float64(int64(val)) == val && val >= float64(math.MinInt64) && val <= float64(math.MaxInt64) {
			i64, ok = int64(val), true
		} else {
			ok = false
		}
	case string:
		cleanV := strings.TrimSpace(val)
		if cleanV == "" {
			ok = false
		} else {
			parsedI64, err := strconv.ParseInt(cleanV, 10, 64)
			if err == nil {
				i64, ok = parsedI64, true
			} else {
				parsedF64, errF := strconv.ParseFloat(cleanV, 64)
				if errF == nil && parsedF64 == math.Trunc(parsedF64) && parsedF64 >= float64(math.MinInt64) && parsedF64 <= float64(math.MaxInt64) {
					if float64(int64(parsedF64)) == parsedF64 {
						i64, ok = int64(parsedF64), true
					} else {
						ok = false
					}
				} else {
					ok = false
				}
			}
		}
	default:
		ok = false
	}

	if !ok {
		return 0, false
	}
	// Check platform int range
	if i64 >= int64(minIntPlatform) && i64 <= int64(maxIntPlatform) {
		return int(i64), true
	}
	return 0, false // Value is outside the platform's int range
}

// parseParamAsNumber parses various numeric types or string representations into a float64.
func parseParamAsNumber(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case string:
		cleanV := strings.TrimSpace(val)
		if cleanV == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(cleanV, 64)
		if err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}