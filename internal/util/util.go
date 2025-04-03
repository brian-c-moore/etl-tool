package util

import (
	"os"
	"regexp"
	"strings"
)

// ExpandEnvUniversal expands environment variables ($VAR, ${VAR}, %VAR%).
// It handles both Unix-style ($VAR, ${VAR}) and Windows-style (%VAR%) variables.
// Variables that are not found are replaced with an empty string.
func ExpandEnvUniversal(s string) string {
	// Expand Unix-style variables first using os.ExpandEnv.
	unixExpanded := os.ExpandEnv(s)

	// Compile a regular expression to find Windows-style variables (%VAR%).
	// The regex captures the variable name inside the percentage signs.
	re := regexp.MustCompile(`%([A-Za-z0-9_]+)%`)

	// Replace Windows-style variables found in the string.
	winExpanded := re.ReplaceAllStringFunc(unixExpanded, func(match string) string {
		// Extract the variable name (without the % signs).
		varName := match[1 : len(match)-1]
		// Look up the environment variable.
		if value, ok := os.LookupEnv(varName); ok {
			// Return the found value if the variable exists.
			return value
		}
		// If the variable is not found, replace with an empty string,
		// mimicking os.ExpandEnv's behavior.
		return ""
	})
	return winExpanded
}

// Snippet returns a short prefix of a byte slice for logging or display purposes.
// If the input slice represents a string longer than a predefined limit (200 runes),
// it truncates the string and appends "...". Handles nil input gracefully.
func Snippet(b []byte) string {
	const maxLen = 200 // Maximum number of runes to display before truncating.
	// Handle nil slice gracefully by returning an empty string.
	if b == nil {
		return ""
	}
	s := string(b)
	// Convert to runes to handle multi-byte characters correctly.
	runes := []rune(s)
	if len(runes) > maxLen {
		// Truncate the rune slice and append ellipsis.
		return string(runes[:maxLen]) + "..."
	}
	// Return the full string if it's within the limit.
	return s
}

// LooksLikeJSON performs a basic heuristic check if a string appears to be
// a JSON object or array based on its starting and ending characters after trimming whitespace.
func LooksLikeJSON(s string) bool {
	trimmed := strings.TrimSpace(s)
	// Check if the trimmed string starts/ends with {} or [].
	return (strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")) ||
		(strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]"))
}

// --- Credential/Sensitive Data Masking ---

// sensitiveKeysRegex is a pre-compiled regular expression used to identify keys
// that likely contain sensitive information (case-insensitive).
var sensitiveKeysRegex = regexp.MustCompile(`(?i)password|secret|token|key|auth|credential|pass|pwd`)

const (
	// maskedValue is the standard replacement string for masked data.
	maskedValue = "********"
)

// MaskCredentials attempts to mask the password part of a URI string.
// It looks for standard URI formats like scheme://user:password@host...
// If a password component is detected, it's replaced with maskedValue.
func MaskCredentials(uri string) string {
	schemeSeparator := "://"
	schemeIndex := strings.Index(uri, schemeSeparator)
	// If the scheme separator isn't present, it's likely not a standard URI.
	if schemeIndex == -1 {
		return uri
	}
	scheme := uri[:schemeIndex]
	// Get the part after "://"
	rest := uri[schemeIndex+len(schemeSeparator):]

	// Find the last '@' which separates userinfo from the host part.
	lastAt := strings.LastIndex(rest, "@")
	// If no '@' is found, there's no userinfo part to mask.
	if lastAt == -1 {
		return uri
	}

	userInfo := rest[:lastAt]
	hostAndBeyond := rest[lastAt+1:]

	// Check for a colon within the userinfo part, indicating a password might be present.
	firstColon := strings.Index(userInfo, ":")

	// If no colon exists, it's just "user@host...", no password.
	if firstColon == -1 {
		return uri
	}

	// A colon exists; assume the part after it is the password.
	user := userInfo[:firstColon]
	// Reconstruct the URI with the user, masked password, and the rest.
	return scheme + schemeSeparator + user + ":" + maskedValue + "@" + hostAndBeyond
}

// MaskSensitiveData recursively traverses a map and masks values based on certain rules:
// 1. Recursively calls itself for nested maps.
// 2. If a key matches sensitiveKeysRegex, its corresponding value (string or otherwise) is masked.
// 3. If a value is a string and doesn't have a sensitive key, it's passed to MaskCredentials
//    to check if it's a URI with a password that needs masking.
// 4. Other non-sensitive key/value pairs are kept unchanged.
// Returns a new map with sensitive data masked. Handles nil input map.
func MaskSensitiveData(data map[string]interface{}) map[string]interface{} {
	if data == nil {
		return nil
	}
	// Create a new map to store the potentially masked data.
	maskedMap := make(map[string]interface{}, len(data))

	for key, value := range data {
		// Determine if the key itself suggests sensitivity.
		isSensitiveKey := sensitiveKeysRegex.MatchString(key)

		switch v := value.(type) {
		case map[string]interface{}:
			// Always recurse into nested maps.
			maskedMap[key] = MaskSensitiveData(v)
		case string:
			// If the key is sensitive, mask the string value directly.
			if isSensitiveKey {
				maskedMap[key] = maskedValue
			} else {
				// Otherwise, check if the string value looks like a URI that needs masking.
				maskedMap[key] = MaskCredentials(v)
			}
		default:
			// For any other type (int, bool, nil, slice, etc.):
			// Mask only if the key indicates sensitivity.
			if isSensitiveKey {
				maskedMap[key] = maskedValue
			} else {
				// Otherwise, keep the original value.
				maskedMap[key] = v
			}
		}
	}
	return maskedMap
}