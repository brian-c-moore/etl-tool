// --- START OF CORRECTED FILE internal/transform/transform.go ---
package transform

import (
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"etl-tool/internal/logging"

	"github.com/Knetic/govaluate"
)

// fipsModeEnabled tracks whether FIPS compliance is active.
var fipsModeEnabled atomic.Bool

// SetFIPSMode enables or disables FIPS compliance mode globally for transformations.
func SetFIPSMode(enabled bool) {
	fipsModeEnabled.Store(enabled)
	if enabled {
		logging.Logf(logging.Debug, "FIPS mode enabled for transformations.")
	}
}

// IsFIPSMode returns true if FIPS compliance mode is enabled.
func IsFIPSMode() bool {
	return fipsModeEnabled.Load()
}

// TransformFunc defines the signature for transformation/validation functions.
// It receives the input value, the full current record state, and any parameters.
// It returns the transformed value or an error for validation/strict failures.
type TransformFunc func(value interface{}, record map[string]interface{}, params map[string]interface{}) interface{}

// transformRegistry holds the mapping from function names (lowercase) to implementations.
var transformRegistry = make(map[string]TransformFunc)

// init registers all known transformation and validation functions.
func init() {
	// Register transformation functions (permissive variants)
	transformRegistry["epochtodate"] = epochToDate
	transformRegistry["calculateage"] = calculateAge
	transformRegistry["regexextract"] = regexExtract
	transformRegistry["trim"] = trim
	transformRegistry["touppercase"] = toUpperCase
	transformRegistry["tolowercase"] = toLowerCase
	transformRegistry["branch"] = branchTransform
	transformRegistry["dateconvert"] = dateConvert
	transformRegistry["multidateconvert"] = multiDateConvert
	transformRegistry["toint"] = toInt
	transformRegistry["tofloat"] = toFloat
	transformRegistry["tobool"] = toBool
	transformRegistry["tostring"] = toString
	transformRegistry["replaceall"] = replaceAll
	transformRegistry["substring"] = substring
	transformRegistry["coalesce"] = coalesceTransform
	transformRegistry["hash"] = hashTransform

	// Register STRICT transformation variants
	transformRegistry["musttoint"] = mustToInt
	transformRegistry["musttofloat"] = mustToFloat
	transformRegistry["musttobool"] = mustToBool
	transformRegistry["mustepochtodate"] = mustEpochToDate
	transformRegistry["mustdateconvert"] = mustDateConvert

	// Register validation functions (which return error on failure)
	transformRegistry["validaterequired"] = validateRequired
	transformRegistry["validateregex"] = validateRegex
	transformRegistry["validatenumericrange"] = validateNumericRange
	transformRegistry["validateallowedvalues"] = validateAllowedValues
}

// ApplyTransform looks up the specified transformation function by name and executes it.
// It handles parsing shorthand parameters from the transform string (e.g., "regexExtract:pattern").
// Returns the result of the transformation or the original value if the function is not found.
// Validation and strict transformation functions return an error, which is passed through.
func ApplyTransform(transformString string, params map[string]interface{}, sourceValue interface{}, recordState map[string]interface{}) interface{} {
	if transformString == "" {
		return sourceValue
	}

	parts := strings.SplitN(transformString, ":", 2)
	funcName := strings.ToLower(strings.TrimSpace(parts[0]))

	tf, found := transformRegistry[funcName]
	if !found {
		logging.Logf(logging.Warning, "Transformation function '%s' not found; using original value.", funcName)
		return sourceValue
	}

	effectiveParams := make(map[string]interface{})
	for k, v := range params {
		effectiveParams[k] = v
	}

	if len(parts) == 2 {
		shorthandParam := strings.TrimSpace(parts[1])
		if shorthandParam != "" {
			paramKey := ""
			switch funcName {
			case "regexextract", "validateregex":
				paramKey = "pattern"
			}

			if paramKey != "" {
				if _, exists := effectiveParams[paramKey]; !exists {
					effectiveParams[paramKey] = shorthandParam
					logging.Logf(logging.Debug, "Using parameter '%s' = '%s' from transform string shorthand for function '%s'", paramKey, shorthandParam, funcName)
				} else {
					logging.Logf(logging.Debug, "Parameter '%s' from transform string shorthand ignored; explicit param exists for function '%s'", paramKey, funcName)
				}
			} else {
				logging.Logf(logging.Warning, "Transform string '%s' has a shorthand parameter, but no default key is defined for function '%s'. Shorthand ignored.", transformString, funcName)
			}
		}
	}

	logging.Logf(logging.Debug, "Applying transform '%s' with value=%v, params=%v", funcName, sourceValue, effectiveParams)
	result := tf(sourceValue, recordState, effectiveParams)

	if err, isError := result.(error); isError {
		logging.Logf(logging.Debug, "Transform '%s' resulted in processing error: %v", funcName, err)
		return err
	}

	logging.Logf(logging.Debug, "Transform '%s' result: %v", funcName, result)
	return result
}

// --- Transformation Function Implementations ---

// epochToDate converts a Unix epoch timestamp (seconds or float seconds) to a date string (YYYY-MM-DD).
func epochToDate(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	var epoch int64
	parsed := false

	if fVal, ok := parseValueAsFloat64(value); ok {
		epoch = int64(math.Trunc(fVal))
		parsed = true
	}

	if !parsed {
		if iVal, ok := parseValueAsInt64(value); ok {
			epoch = iVal
			parsed = true
		}
	}

	if !parsed {
		logging.Logf(logging.Warning, "epochToDate: could not parse input '%v' (type %T) as epoch seconds.", value, value)
		return value
	}

	t := time.Unix(epoch, 0).UTC()
	return t.Format("2006-01-02")
}

// calculateAge calculates the age in days based on a Unix epoch timestamp (seconds).
func calculateAge(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	fEpoch, ok := parseValueAsFloat64(value)
	if !ok {
		logging.Logf(logging.Warning, "calculateAge: could not parse input '%v' (type %T) as numeric epoch seconds.", value, value)
		return nil
	}
	epoch := int64(math.Trunc(fEpoch))

	now := time.Now().UTC()
	nowEpoch := now.Unix()

	if epoch > nowEpoch {
		logging.Logf(logging.Debug, "calculateAge: input epoch %d is in the future, returning age 0.", epoch)
		return 0
	}

	birthTime := time.Unix(epoch, 0).UTC()
	nowDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	birthDay := time.Date(birthTime.Year(), birthTime.Month(), birthTime.Day(), 0, 0, 0, 0, time.UTC)
	daysFloat := nowDay.Sub(birthDay).Hours() / 24.0

	return int(math.Floor(daysFloat))
}

// regexExtract extracts the first capture group from a string using a regex pattern.
func regexExtract(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	strVal, ok := value.(string)
	if !ok {
		logging.Logf(logging.Warning, "regexExtract: input value is not a string (type %T)", value)
		return nil
	}
	pattern, ok := getStringParam(params, "pattern")
	if !ok || pattern == "" {
		logging.Logf(logging.Warning, "regexExtract: missing or empty 'pattern' string parameter.")
		return nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		logging.Logf(logging.Error, "regexExtract: Invalid regex pattern '%s': %v", pattern, err)
		return nil
	}

	matches := re.FindStringSubmatch(strVal)
	if len(matches) >= 2 {
		return matches[1]
	}

	logging.Logf(logging.Debug, "regexExtract: pattern '%s' did not match or capture a group in string '%s'", pattern, strVal)
	return nil
}

// trim removes leading and trailing whitespace from a string.
func trim(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if s, ok := value.(string); ok {
		return strings.TrimSpace(s)
	}
	return value
}

// toUpperCase converts a string to uppercase.
func toUpperCase(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if s, ok := value.(string); ok {
		return strings.ToUpper(s)
	}
	return value
}

// toLowerCase converts a string to lowercase.
func toLowerCase(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if s, ok := value.(string); ok {
		return strings.ToLower(s)
	}
	return value
}

// branchTransform evaluates conditions sequentially and returns the value from the first matching branch.
func branchTransform(value interface{}, record map[string]interface{}, params map[string]interface{}) interface{} {
	branchesRaw, ok := params["branches"]
	if !ok {
		logging.Logf(logging.Warning, "branchTransform: missing 'branches' parameter.")
		return value
	}
	branchesSlice, ok := branchesRaw.([]interface{})
	if !ok || len(branchesSlice) == 0 {
		logging.Logf(logging.Warning, "branchTransform: 'branches' parameter is not a non-empty array.")
		return value
	}

	exprParams := make(map[string]interface{}, len(record)+1)
	for k, v := range record {
		exprParams[k] = v
	}
	exprParams["inputValue"] = value

	for i, brInterface := range branchesSlice {
		branchMap, ok := brInterface.(map[string]interface{})
		if !ok {
			logging.Logf(logging.Warning, "branchTransform: branch %d definition is not a map structure.", i)
			continue
		}

		condRaw, condExists := branchMap["condition"]
		if !condExists {
			logging.Logf(logging.Warning, "branchTransform: branch %d is missing 'condition' key.", i)
			continue
		}
		condition, ok := condRaw.(string)
		if !ok || condition == "" {
			logging.Logf(logging.Warning, "branchTransform: branch %d 'condition' is not a non-empty string.", i)
			continue
		}

		branchVal, valExists := branchMap["value"]
		if !valExists {
			logging.Logf(logging.Warning, "branchTransform: branch %d is missing 'value' key.", i)
			continue
		}

		expression, err := govaluate.NewEvaluableExpression(condition)
		if err != nil {
			logging.Logf(logging.Error, "branchTransform: Failed to parse condition '%s' in branch %d: %v", condition, i, err)
			continue
		}

		result, err := expression.Evaluate(exprParams)
		if err != nil {
			logging.Logf(logging.Warning, "branchTransform: Failed to evaluate condition '%s' in branch %d: %v. Skipping branch.", condition, i, err)
			continue
		}

		if boolResult, isBool := result.(bool); isBool && boolResult {
			logging.Logf(logging.Debug, "branchTransform: Matched condition '%s' in branch %d; returning value: %v", condition, i, branchVal)
			return branchVal
		}
	}

	logging.Logf(logging.Debug, "branchTransform: No conditions matched; returning original value: %v", value)
	return value
}

// dateConvert converts a date/time string or time.Time object from one format to another.
func dateConvert(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	strVal, isString := value.(string)
	tVal, isTime := value.(time.Time)

	if !isString && !isTime {
		logging.Logf(logging.Warning, "dateConvert: input value is not a string or time.Time (type %T)", value)
		return value
	}

	outputFormat, _ := getStringParam(params, "outputFormat")
	if outputFormat == "" {
		outputFormat = time.RFC3339
	}

	if isTime {
		return tVal.Format(outputFormat)
	}

	inputFormat, _ := getStringParam(params, "inputFormat")
	originalInputFormat := inputFormat

	if inputFormat == "" {
		inputFormat = time.RFC3339
	}

	t, err := time.Parse(inputFormat, strVal)

	if err != nil && originalInputFormat == "" {
		fallbacks := []string{
			"2006-01-02", "2006/01/02", "01/02/2006", "2006-01-02T15:04:05Z07:00",
			"2006-01-02 15:04:05", time.RFC1123Z, time.RFC1123, time.RFC822Z,
			time.RFC822, "01-02-06", "20060102",
		}
		parsed := false
		for _, fbFormat := range fallbacks {
			if t, err = time.Parse(fbFormat, strVal); err == nil {
				parsed = true
				logging.Logf(logging.Debug, "dateConvert: Parsed '%s' using fallback format '%s'", strVal, fbFormat)
				break
			}
		}
		if !parsed {
			logging.Logf(logging.Warning, "dateConvert: failed to parse '%s' with default format '%s' or common fallbacks.", strVal, inputFormat)
			return value
		}
	} else if err != nil {
		logging.Logf(logging.Warning, "dateConvert: failed to parse '%s' with specified format '%s': %v", strVal, inputFormat, err)
		return value
	}

	return t.Format(outputFormat)
}

// multiDateConvert attempts to parse a date string using multiple potential input formats.
func multiDateConvert(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	strVal, ok := value.(string)
	if !ok {
		logging.Logf(logging.Warning, "multiDateConvert: input value is not a string (type %T)", value)
		return value
	}

	formatsRaw, formatsOk := params["formats"]
	outputFmt, outOk := getStringParam(params, "outputFormat")

	if !formatsOk || !outOk {
		logging.Logf(logging.Warning, "multiDateConvert: requires both 'formats' and 'outputFormat' parameters.")
		return value
	}

	formatsSlice, sliceOk := formatsRaw.([]interface{})
	if !sliceOk || len(formatsSlice) == 0 || outputFmt == "" {
		logging.Logf(logging.Warning, "multiDateConvert: requires non-empty 'formats' array and non-empty 'outputFormat' string parameters.")
		return value
	}

	var inputFormats []string
	for i, fInterface := range formatsSlice {
		formatStr, isStr := fInterface.(string)
		if !isStr || formatStr == "" {
			logging.Logf(logging.Warning, "multiDateConvert: format at index %d is not a valid non-empty string.", i)
			return value
		}
		inputFormats = append(inputFormats, formatStr)
	}

	for _, inputFmt := range inputFormats {
		if t, err := time.Parse(inputFmt, strVal); err == nil {
			logging.Logf(logging.Debug, "multiDateConvert: Parsed '%s' using format '%s'", strVal, inputFmt)
			return t.Format(outputFmt)
		}
	}

	logging.Logf(logging.Warning, "multiDateConvert: Could not parse '%s' with any of the provided formats: %v", strVal, inputFormats)
	return value
}

// toInt attempts to convert the input value to an int64.
func toInt(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if i, ok := parseValueAsInt64(value); ok {
		return i
	}
	logging.Logf(logging.Warning, "toInt: conversion failed for input '%v' (type %T); returning nil", value, value)
	return nil
}

// toFloat attempts to convert the input value to a float64.
func toFloat(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if f, ok := parseValueAsFloat64(value); ok {
		return f
	}
	logging.Logf(logging.Warning, "toFloat: conversion failed for input '%v' (type %T); returning nil", value, value)
	return nil
}

// toBool attempts to convert the input value to a boolean.
func toBool(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if value == nil {
		return false
	}

	switch v := value.(type) {
	case bool:
		return v
	case string:
		lower := strings.ToLower(strings.TrimSpace(v))
		switch lower {
		case "true", "1", "yes", "t", "y":
			return true
		case "false", "0", "no", "f", "n", "":
			return false
		default:
			logging.Logf(logging.Warning, "toBool: unrecognized string value '%s'; returning nil", v)
			return nil
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int() != 0
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return rv.Uint() != 0
		default:
			logging.Logf(logging.Warning, "toBool: internal error handling numeric type %T; returning nil", value)
			return nil
		}
	case float32, float64:
		numVal, _ := parseValueAsFloat64(v)
		return numVal != 0.0
	default:
		logging.Logf(logging.Warning, "toBool: conversion received unsupported type '%T'; returning nil", value)
		return nil
	}
}

// toString converts the input value to its string representation.
func toString(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if value == nil {
		return ""
	}
	if b, ok := value.([]byte); ok {
		return string(b)
	}
	// Use fmt.Sprintf for general-purpose string conversion.
	return fmt.Sprintf("%v", value)
}

// replaceAll replaces all occurrences of a substring within a string.
func replaceAll(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	strVal, ok := value.(string)
	if !ok {
		logging.Logf(logging.Warning, "replaceAll: input value is not a string (type %T)", value)
		return value
	}

	oldVal, okOld := getStringParam(params, "old")
	newVal, okNew := getStringParam(params, "new")

	if !okOld || !okNew {
		logging.Logf(logging.Warning, "replaceAll: requires both 'old' and 'new' string parameters.")
		return value
	}

	return strings.ReplaceAll(strVal, oldVal, newVal)
}

// substring extracts a portion of a string based on start index and length.
func substring(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	strVal, ok := value.(string)
	if !ok {
		logging.Logf(logging.Warning, "substring: input value is not a string (type %T)", value)
		return value
	}

	start, startOK := getIntParam(params, "start")
	length, lengthOK := getIntParam(params, "length")

	if !startOK || !lengthOK {
		logging.Logf(logging.Warning, "substring: requires both 'start' and 'length' integer parameters.")
		return value
	}

	runes := []rune(strVal)
	strLen := len(runes)

	if start < 0 {
		start = 0
	}
	if length <= 0 {
		return ""
	}
	if start >= strLen {
		return ""
	}

	end := start + length
	if end > strLen {
		end = strLen
	}

	return string(runes[start:end])
}

// coalesceTransform returns the first non-nil, non-empty string value from a list of fields in the record.
func coalesceTransform(_ interface{}, record map[string]interface{}, params map[string]interface{}) interface{} {
	fieldsRaw, ok := params["fields"]
	if !ok {
		logging.Logf(logging.Warning, "coalesceTransform: missing 'fields' array parameter.")
		return nil
	}
	fieldsSlice, sliceOk := fieldsRaw.([]interface{})
	if !sliceOk || len(fieldsSlice) == 0 {
		logging.Logf(logging.Warning, "coalesceTransform: 'fields' parameter is not a non-empty array.")
		return nil
	}

	for i, fieldInterface := range fieldsSlice {
		keyStr, isStr := fieldInterface.(string)
		if !isStr {
			logging.Logf(logging.Warning, "coalesceTransform: field name at index %d is not a string: %v. Skipping.", i, fieldInterface)
			continue
		}

		if val, found := record[keyStr]; found {
			if val != nil {
				if strVal, isString := val.(string); isString {
					if strVal != "" {
						logging.Logf(logging.Debug, "coalesceTransform: Found non-empty string value '%v' in field '%s'.", val, keyStr)
						return val
					}
				} else {
					logging.Logf(logging.Debug, "coalesceTransform: Found non-nil, non-string value '%v' in field '%s'.", val, keyStr)
					return val
				}
			}
		}
	}

	logging.Logf(logging.Debug, "coalesceTransform: No non-empty value found in fields: %v. Returning nil.", fieldsSlice)
	return nil
}

// ValueToStringForHash provides a consistent, canonical string representation // CORRECTED: Exported
// for different data types, suitable for generating stable hashes.
func ValueToStringForHash(v interface{}) string {
	if v == nil {
		return "<NIL>" // Use a specific marker for nil values
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return rv.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		// Use 'g' format with maximum precision (-1) for floats to ensure stability.
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	case reflect.Struct:
		if t, ok := v.(time.Time); ok {
			// Use RFC3339Nano with UTC for an unambiguous time representation.
			return t.UTC().Format(time.RFC3339Nano)
		}
		// Fallback for other structs (might not be stable depending on internal representation).
		// Consider specific handling for other known struct types if needed for hashing.
		return fmt.Sprintf("%#v", v)
	default:
		// Fallback for other types (slices, maps, etc. - hashing these is often unstable).
		// Consider specific stable representations if these types need to be hashed reliably.
		return fmt.Sprintf("%#v", v)
	}
}

// hashTransform generates a hash of concatenated values from specified fields
// using a canonical string representation for stability.
func hashTransform(_ interface{}, record map[string]interface{}, params map[string]interface{}) interface{} {
	algo, algoOk := getStringParam(params, "algorithm")
	if !algoOk {
		return fmt.Errorf("missing 'algorithm' parameter for hash transform")
	}
	fieldsRaw, fieldsOk := params["fields"]
	if !fieldsOk {
		return fmt.Errorf("missing 'fields' parameter for hash transform")
	}
	fieldsSlice, ok := fieldsRaw.([]interface{})
	if !ok || len(fieldsSlice) == 0 {
		return fmt.Errorf("'fields' parameter must be a non-empty array for hash transform")
	}

	fieldNames := make([]string, 0, len(fieldsSlice))
	for i, fInterface := range fieldsSlice {
		name, isStr := fInterface.(string)
		if !isStr {
			return fmt.Errorf("field name at index %d is not a string for hash transform", i)
		}
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames) // Ensure consistent field order

	algoLower := strings.ToLower(algo)
	if IsFIPSMode() && algoLower == "md5" {
		return fmt.Errorf("hash algorithm 'md5' not allowed in FIPS mode")
	}

	var hashFunc func([]byte) []byte
	switch algoLower {
	case "sha256":
		hashFunc = func(data []byte) []byte { h := sha256.Sum256(data); return h[:] }
	case "sha512":
		hashFunc = func(data []byte) []byte { h := sha512.Sum512(data); return h[:] }
	case "md5":
		hashFunc = func(data []byte) []byte { h := md5.Sum(data); return h[:] }
	default:
		return fmt.Errorf("unsupported hash algorithm: %s", algo)
	}

	var dataToHash strings.Builder
	separator := "||" // Use a consistent separator
	for i, fieldName := range fieldNames {
		if val, found := record[fieldName]; found {
			// Use the refined helper for canonical string representation
			strVal := ValueToStringForHash(val) // CORRECTED: Call exported func
			dataToHash.WriteString(strVal)
		} else {
			dataToHash.WriteString("<MISSING>") // Use distinct placeholder for missing fields
		}
		if i < len(fieldNames)-1 {
			dataToHash.WriteString(separator)
		}
	}

	// Convert string builder to string, then to bytes
	inputString := dataToHash.String()
	inputBytes := []byte(inputString)

	// Calculate the hash
	hashedBytes := hashFunc(inputBytes)

	// Return the final hex encoded string
	return hex.EncodeToString(hashedBytes)
}

// --- Strict Transformation Variants (Return error on failure) ---

// mustToInt ensures conversion to int64, returns error on failure.
func mustToInt(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if i, ok := parseValueAsInt64(value); ok {
		return i
	}
	return fmt.Errorf("mustToInt: conversion failed for input '%v' (type %T)", value, value)
}

// mustToFloat ensures conversion to float64, returns error on failure.
func mustToFloat(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if f, ok := parseValueAsFloat64(value); ok {
		return f
	}
	return fmt.Errorf("mustToFloat: conversion failed for input '%v' (type %T)", value, value)
}

// mustToBool ensures conversion to bool, returns error on failure or ambiguity.
func mustToBool(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if value == nil {
		return fmt.Errorf("mustToBool: input is nil")
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		lower := strings.ToLower(strings.TrimSpace(v))
		switch lower {
		case "true", "1", "yes", "t", "y":
			return true
		case "false", "0", "no", "f", "n":
			return false
		default:
			return fmt.Errorf("mustToBool: unrecognized or ambiguous string value '%s'", v)
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int() != 0
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return rv.Uint() != 0
		default:
			return fmt.Errorf("mustToBool: internal error handling numeric type %T", value)
		}
	case float32, float64:
		numVal, _ := parseValueAsFloat64(v)
		return numVal != 0.0
	default:
		return fmt.Errorf("mustToBool: conversion received unsupported type '%T'", value)
	}
}

// mustEpochToDate ensures conversion, returns error on failure.
func mustEpochToDate(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	var epoch int64
	parsed := false

	if fVal, ok := parseValueAsFloat64(value); ok {
		epoch = int64(math.Trunc(fVal))
		parsed = true
	}
	if !parsed {
		if iVal, ok := parseValueAsInt64(value); ok {
			epoch = iVal
			parsed = true
		}
	}

	if !parsed {
		return fmt.Errorf("mustEpochToDate: could not parse input '%v' (type %T) as epoch seconds", value, value)
	}

	t := time.Unix(epoch, 0).UTC()
	return t.Format("2006-01-02")
}

// mustDateConvert ensures date string conversion, returns error on failure.
func mustDateConvert(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	strVal, isString := value.(string)
	tVal, isTime := value.(time.Time)

	if !isString && !isTime {
		return fmt.Errorf("mustDateConvert: input value is not a string or time.Time (type %T)", value)
	}
	outputFormat, _ := getStringParam(params, "outputFormat")
	if outputFormat == "" {
		outputFormat = time.RFC3339
	}
	if isTime {
		return tVal.Format(outputFormat)
	}

	inputFormat, _ := getStringParam(params, "inputFormat")
	originalInputFormat := inputFormat
	if inputFormat == "" {
		inputFormat = time.RFC3339
	}

	var t time.Time
	var err error

	t, err = time.Parse(inputFormat, strVal)
	if err != nil && originalInputFormat == "" {
		fallbacks := []string{
			"2006-01-02", "2006/01/02", "01/02/2006", "2006-01-02T15:04:05Z07:00",
			"2006-01-02 15:04:05", time.RFC1123Z, time.RFC1123, time.RFC822Z,
			time.RFC822, "01-02-06", "20060102",
		}
		parsed := false
		var tLoop time.Time
		var errLoop error
		for _, fbFormat := range fallbacks {
			tLoop, errLoop = time.Parse(fbFormat, strVal)
			if errLoop == nil {
				t = tLoop
				err = nil
				parsed = true
				break
			}
		}
		if !parsed {
			return fmt.Errorf("mustDateConvert: failed to parse '%s' with default format or common fallbacks", strVal)
		}
	} else if err != nil {
		return fmt.Errorf("mustDateConvert: failed to parse '%s' with specified format '%s': %w", strVal, inputFormat, err)
	}
	return t.Format(outputFormat)
}

// --- Validation Function Implementations (Return error on failure) ---

// validateRequired checks if a value is present (non-nil and non-empty/whitespace string).
func validateRequired(value interface{}, _ map[string]interface{}, _ map[string]interface{}) interface{} {
	if value == nil {
		return fmt.Errorf("required value is missing (nil)")
	}
	if strVal, ok := value.(string); ok {
		if strings.TrimSpace(strVal) == "" {
			return fmt.Errorf("required string value is empty or whitespace")
		}
	}
	return value
}

// validateRegex checks if a string value matches a regex pattern.
func validateRegex(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	strVal, ok := value.(string)
	if !ok {
		// Allow non-strings to pass if not required implicitly
		// Return the original value if it's not a string
		return value
		// If non-strings should fail, use:
		// return fmt.Errorf("input value type %T is not a string, cannot validate with regex", value)
	}
	pattern, ok := getStringParam(params, "pattern")
	if !ok || pattern == "" {
		return fmt.Errorf("missing or empty 'pattern' string parameter for validateRegex")
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern '%s': %w", pattern, err)
	}

	if !re.MatchString(strVal) {
		return fmt.Errorf("value %s does not match required pattern '%s'", strconv.Quote(strVal), pattern)
	}

	return value
}

// validateNumericRange checks if a numeric value falls within a specified min/max range.
func validateNumericRange(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	numVal, ok := parseValueAsFloat64(value)
	if !ok {
		// Allow non-numerics to pass validation by default
		// Return the original value if it's not a valid number
		return value
		// If non-numerics should fail, use:
		// return fmt.Errorf("input value '%v' (type %T) is not a valid number for range check", value, value)
	}

	_, minKeyExists := params["min"]
	_, maxKeyExists := params["max"]

	if !minKeyExists && !maxKeyExists {
		return fmt.Errorf("requires at least 'min' or 'max' parameter for validateNumericRange")
	}

	var minVal float64
	var minOK bool
	if minKeyExists {
		minVal, minOK = parseParamAsNumber(params["min"])
		if !minOK {
			return fmt.Errorf("invalid 'min' parameter: '%v' is not a valid number", params["min"])
		}
	}

	var maxVal float64
	var maxOK bool
	if maxKeyExists {
		maxVal, maxOK = parseParamAsNumber(params["max"])
		if !maxOK {
			return fmt.Errorf("invalid 'max' parameter: '%v' is not a valid number", params["max"])
		}
	}

	if minKeyExists && minOK && numVal < minVal {
		return fmt.Errorf("value %v is less than minimum allowed %v", numVal, minVal)
	}
	if maxKeyExists && maxOK && numVal > maxVal {
		return fmt.Errorf("value %v is greater than maximum allowed %v", numVal, maxVal)
	}

	return value
}

// validateAllowedValues checks if a value is present in a predefined list.
func validateAllowedValues(value interface{}, _ map[string]interface{}, params map[string]interface{}) interface{} {
	allowedValuesRaw, ok := params["values"]
	if !ok {
		return fmt.Errorf("missing 'values' array parameter for validateAllowedValues")
	}
	allowedValuesSlice, ok := allowedValuesRaw.([]interface{})
	if !ok || len(allowedValuesSlice) == 0 {
		return fmt.Errorf("'values' parameter is not a non-empty array for validateAllowedValues")
	}

	found := false
	for _, allowed := range allowedValuesSlice {
		cmp, err := CompareValues(value, allowed)
		if err == nil && cmp == 0 {
			found = true
			break
		} else if err != nil {
			logging.Logf(logging.Debug, "validateAllowedValues: Could not compare input '%v' (%T) with allowed value '%v' (%T): %v", value, value, allowed, allowed, err)
		}
	}

	if !found {
		return fmt.Errorf("value '%v' is not in the list of allowed values", value)
	}

	return value
}

// --- Helper Functions ---

// getStringParam retrieves a string value from the parameters map.
func getStringParam(params map[string]interface{}, key string) (string, bool) {
	val, ok := params[key]
	if !ok {
		return "", false
	}
	strVal, ok := val.(string)
	return strVal, ok
}

// getIntParam retrieves an integer value from the parameters map.
func getIntParam(params map[string]interface{}, key string) (int, bool) {
	val, ok := params[key]
	if !ok {
		return 0, false
	}
	return parseParamAsInt(val)
}

// parseValueAsInt64 attempts to parse various input types into int64.
func parseValueAsInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		if uint64(v) > uint64(math.MaxInt64) {
			return 0, false
		}
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > uint64(math.MaxInt64) {
			return 0, false
		}
		return int64(v), true
	case float32:
		if float32(int64(v)) == v && v >= float32(math.MinInt64) && v <= float32(math.MaxInt64) {
			return int64(v), true
		}
		return 0, false
	case float64:
		if float64(int64(v)) == v && v >= float64(math.MinInt64) && v <= float64(math.MaxInt64) {
			return int64(v), true
		}
		return 0, false
	case string:
		cleanV := strings.TrimSpace(v)
		if cleanV == "" {
			return 0, false
		}
		i, err := strconv.ParseInt(cleanV, 10, 64)
		if err == nil {
			return i, true
		}
		f, errF := strconv.ParseFloat(cleanV, 64)
		if errF == nil && f == math.Trunc(f) && f >= float64(math.MinInt64) && f <= float64(math.MaxInt64) {
			if float64(int64(f)) == f {
				return int64(f), true
			}
		}
		return 0, false
	default:
		return 0, false
	}
}

// parseValueAsFloat64 attempts to parse various input types into float64.
func parseValueAsFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(v)
		return float64(rv.Int()), true
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(v)
		return float64(rv.Uint()), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		cleanV := strings.TrimSpace(v)
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

// parseParamAsNumber is a convenience wrapper for validating numeric parameters.
func parseParamAsNumber(v interface{}) (float64, bool) {
	return parseValueAsFloat64(v)
}

// parseParamAsInt is a convenience wrapper for validating integer parameters.
func parseParamAsInt(v interface{}) (int, bool) {
	i64, ok := parseValueAsInt64(v)
	if !ok {
		return 0, false
	}
	const maxInt = int(^uint(0) >> 1)
	const minInt = -maxInt - 1
	if i64 >= int64(minInt) && i64 <= int64(maxInt) {
		return int(i64), true
	}
	return 0, false
}

// CompareValues attempts to compare two values of potentially different types.
func CompareValues(a, b interface{}) (int, error) {
	if a == nil && b == nil {
		return 0, nil
	}
	if a == nil {
		return -1, nil
	}
	if b == nil {
		return 1, nil
	}

	aFloat, aIsNum := parseValueAsFloat64(a)
	bFloat, bIsNum := parseValueAsFloat64(b)

	if aIsNum && bIsNum {
		if aFloat < bFloat {
			return -1, nil
		}
		if aFloat > bFloat {
			return 1, nil
		}
		return 0, nil
	}

	// Only compare if types are compatible beyond numeric
	typeA := reflect.TypeOf(a)
	typeB := reflect.TypeOf(b)

	if typeA != typeB {
		return 0, fmt.Errorf("type mismatch: cannot compare %T with %T", a, b)
	}

	// Handle specific comparable types
	switch a.(type) {
	case string:
		return strings.Compare(a.(string), b.(string)), nil
	case time.Time:
		tA, _ := a.(time.Time)
		tB, _ := b.(time.Time)
		if tA.Before(tB) {
			return -1, nil
		}
		if tA.After(tB) {
			return 1, nil
		}
		return 0, nil
	case bool:
		bA, _ := a.(bool)
		bB, _ := b.(bool)
		if bA == bB {
			return 0, nil
		}
		if bA { // true > false
			return 1, nil
		}
		return -1, nil // false < true
	}

	// Use DeepEqual for non-ordered types like maps/slices ONLY for equality check
	if reflect.DeepEqual(a, b) {
		return 0, nil
	}

	// Cannot determine order for other non-primitive, non-time types
	return 0, fmt.Errorf("unsupported comparison ordering for type %T", a)

}