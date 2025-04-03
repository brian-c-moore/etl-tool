package util

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

// TestExpandEnvUniversal tests the environment variable expansion logic.
func TestExpandEnvUniversal(t *testing.T) {
	// Helper function to set/unset env vars for the duration of a subtest
	setenv := func(t *testing.T, key, value string) {
		t.Helper()
		originalValue, exists := os.LookupEnv(key)
		os.Setenv(key, value)
		t.Cleanup(func() {
			if exists {
				os.Setenv(key, originalValue)
			} else {
				os.Unsetenv(key)
			}
		})
	}

	unsetenv := func(t *testing.T, key string) {
		t.Helper()
		originalValue, exists := os.LookupEnv(key)
		os.Unsetenv(key)
		t.Cleanup(func() {
			if exists {
				os.Setenv(key, originalValue)
			}
		})
	}

	testCases := []struct {
		name       string
		input      string
		setupEnv   func(t *testing.T) // Setup specific environment variables
		wantOutput string
	}{
		{
			name:       "no variables",
			input:      "plain string",
			setupEnv:   nil,
			wantOutput: "plain string",
		},
		{
			name:  "unix style var exists",
			input: "path is $MY_VAR/data",
			setupEnv: func(t *testing.T) {
				setenv(t, "MY_VAR", "/usr/local")
			},
			wantOutput: "path is /usr/local/data",
		},
		{
			name:       "unix style var missing",
			input:      "path is $MISSING_VAR/data",
			setupEnv:   func(t *testing.T) { unsetenv(t, "MISSING_VAR") }, // Explicitly ensure missing
			wantOutput: "path is /data", // $MISSING_VAR replaced with empty string
		},
		{
			name:  "unix style brace var exists",
			input: "config is ${MY_CONFIG}.yaml",
			setupEnv: func(t *testing.T) {
				setenv(t, "MY_CONFIG", "prod")
			},
			wantOutput: "config is prod.yaml",
		},
		{
			name:       "unix style brace var missing",
			input:      "config is ${MISSING_CONFIG}.yaml",
			setupEnv:   func(t *testing.T) { unsetenv(t, "MISSING_CONFIG") },
			wantOutput: "config is .yaml",
		},
		{
			name:  "windows style var exists",
			input: "path is %WIN_VAR%\\data",
			setupEnv: func(t *testing.T) {
				setenv(t, "WIN_VAR", "C:\\Temp")
			},
			wantOutput: "path is C:\\Temp\\data",
		},
		{
			name:       "windows style var missing",
			input:      "path is %MISSING_WIN_VAR%\\data",
			setupEnv:   func(t *testing.T) { unsetenv(t, "MISSING_WIN_VAR") },
			wantOutput: "path is \\data", // %VAR% gets replaced with empty string if missing
		},
		{
			name:  "mixed styles vars exist",
			input: "start %MIXED_WIN%/middle/$MIXED_NIX/end",
			setupEnv: func(t *testing.T) {
				setenv(t, "MIXED_WIN", "WinPart")
				setenv(t, "MIXED_NIX", "NixPart")
			},
			wantOutput: "start WinPart/middle/NixPart/end",
		},
		{
			name:  "mixed styles one missing",
			input: "start %MIXED_WIN%/middle/$MISSING_NIX/end",
			setupEnv: func(t *testing.T) {
				setenv(t, "MIXED_WIN", "WinPart")
				unsetenv(t, "MISSING_NIX") // Explicitly unset
			},
			wantOutput: "start WinPart/middle//end", // $MISSING replaced with empty
		},
		{
			name:  "mixed styles win missing",
			input: "start %MISSING_WIN%/middle/$MIXED_NIX/end",
			setupEnv: func(t *testing.T) {
				unsetenv(t, "MISSING_WIN") // Explicitly unset
				setenv(t, "MIXED_NIX", "NixPart")
			},
			wantOutput: "start /middle/NixPart/end", // %MISSING% replaced with empty
		},
		{
			name:       "empty input",
			input:      "",
			setupEnv:   nil,
			wantOutput: "",
		},
		{
			name:  "variable is entire string",
			input: "$ONLY_VAR",
			setupEnv: func(t *testing.T) {
				setenv(t, "ONLY_VAR", "complete")
			},
			wantOutput: "complete",
		},
		{
			name:  "windows variable is entire string",
			input: "%ONLY_WIN_VAR%",
			setupEnv: func(t *testing.T) {
				setenv(t, "ONLY_WIN_VAR", "complete_win")
			},
			wantOutput: "complete_win",
		},
		{
			name:       "consecutive variables",
			input:      "$VAR1$VAR2",
			setupEnv:   func(t *testing.T) { setenv(t, "VAR1", "A"); setenv(t, "VAR2", "B") },
			wantOutput: "AB",
		},
		{
			name:       "consecutive windows variables",
			input:      "%VAR1%%VAR2%",
			setupEnv:   func(t *testing.T) { setenv(t, "VAR1", "WinA"); setenv(t, "VAR2", "WinB") },
			wantOutput: "WinAWinB",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Apply environment setup if defined for the test case
			if tc.setupEnv != nil {
				tc.setupEnv(t)
			}
			// Call the function defined in util.go
			gotOutput := ExpandEnvUniversal(tc.input)
			if gotOutput != tc.wantOutput {
				t.Errorf("ExpandEnvUniversal(%q) = %q, want %q", tc.input, gotOutput, tc.wantOutput)
			}
		})
	}
}

// TestSnippet tests the creation of short byte slice prefixes.
func TestSnippet(t *testing.T) {
	// Create strings longer/shorter than the limit
	shortStr := "This is a short string."
	longStr := strings.Repeat("Long string content. ", 20) // Creates string longer than 200 runes

	testCases := []struct {
		name  string
		input []byte
		want  string
	}{
		{
			name:  "short input",
			input: []byte(shortStr),
			want:  shortStr,
		},
		{
			name:  "long input",
			input: []byte(longStr),
			// Expect prefix + "..."
			want: string([]rune(longStr)[:200]) + "...",
		},
		{
			name:  "empty input",
			input: []byte(""),
			want:  "",
		},
		{
			name:  "nil input",
			input: nil,
			want:  "",
		},
		{
			name:  "input exactly limit",
			input: []byte(string([]rune(longStr)[:200])), // Exactly 200 runes
			want:  string([]rune(longStr)[:200]),
		},
		{
			name:  "multibyte runes",
			input: []byte("你好世界" + strings.Repeat(" ", 200)), // Start with multibyte, then long padding
			want:  string([]rune("你好世界" + strings.Repeat(" ", 200))[:200]) + "...",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function defined in util.go
			got := Snippet(tc.input)
			if got != tc.want {
				// Use %q for clearer output differences, especially with whitespace/long strings
				t.Errorf("Snippet(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// TestLooksLikeJSON tests the JSON format heuristic.
func TestLooksLikeJSON(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "valid object", input: `{"key": "value"}`, want: true},
		{name: "valid array", input: `[1, 2, "three"]`, want: true},
		{name: "object with whitespace", input: `  {"key": 123}  `, want: true},
		{name: "array with whitespace", input: "\n\t[null]\n", want: true},
		{name: "empty object", input: `{}`, want: true},
		{name: "empty array", input: `[]`, want: true},
		{name: "plain string", input: `hello world`, want: false},
		{name: "xml string", input: `<tag>value</tag>`, want: false},
		{name: "incomplete object", input: `{"key":`, want: false},
		{name: "incomplete array", input: `[1, 2`, want: false},
		{name: "object missing closing brace", input: `{"a": 1`, want: false},
		{name: "array missing closing bracket", input: `["a", "b"`, want: false},
		{name: "number string", input: `123.45`, want: false},
		{name: "boolean string", input: `true`, want: false},
		{name: "null string", input: `null`, want: false},
		{name: "empty string", input: ``, want: false},
		{name: "whitespace string", input: `   `, want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function defined in util.go
			got := LooksLikeJSON(tc.input)
			if got != tc.want {
				t.Errorf("LooksLikeJSON(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

// TestMaskCredentials tests masking of passwords in connection strings.
func TestMaskCredentials(t *testing.T) {
	// Access the package-level constant defined in util.go
	const mask = maskedValue

	testCases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "postgres full",
			input: "postgres://user:password@host:5432/database?sslmode=require",
			want:  fmt.Sprintf("postgres://user:%s@host:5432/database?sslmode=require", mask),
		},
		{
			name:  "postgresql full",
			input: "postgresql://test_user:secret123@some.host.com/db_name",
			want:  fmt.Sprintf("postgresql://test_user:%s@some.host.com/db_name", mask),
		},
		{
			name:  "postgres user only",
			input: "postgres://user@host/db",
			want:  "postgres://user@host/db", // No password to mask
		},
		{
			name:  "postgres password only",
			input: "postgres://:password@host/db",
			want:  fmt.Sprintf("postgres://:%s@host/db", mask),
		},
		{
			name:  "postgres no user/pass",
			input: "postgres://host/db",
			want:  "postgres://host/db",
		},
		{
			name:  "mysql full", // Check other potential schemes
			input: "mysql://root:rootpwd@127.0.0.1:3306/my_db",
			want:  fmt.Sprintf("mysql://root:%s@127.0.0.1:3306/my_db", mask),
		},
		{
			name:  "sqlserver full", // Test password with '@'
			input: "sqlserver://sa:StrongP@ssw0rd@db.server.net?database=master",
			want:  fmt.Sprintf("sqlserver://sa:%s@db.server.net?database=master", mask),
		},
		{
			name:  "plain string",
			input: "just a regular string",
			want:  "just a regular string",
		},
		{
			name:  "string with colon/at",
			input: "value: other@domain.com",
			want:  "value: other@domain.com", // Not a valid URL scheme
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "uri without scheme",
			input: "user:password@host.com",
			want:  "user:password@host.com", // No scheme:// separator
		},
		{
			name:  "password with special chars", // Test password with '@' and ':'
			input: "postgres://u:p@ss:w/rd@host/db",
			want:  fmt.Sprintf("postgres://u:%s@host/db", mask),
		},
		{
			name:  "password with percent encoding",
			input: "postgres://u:p%40ssw%3Ard@host/db", // p%40ssw%3Ard
			want:  fmt.Sprintf("postgres://u:%s@host/db", mask),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function defined in util.go
			got := MaskCredentials(tc.input)
			if got != tc.want {
				t.Errorf("MaskCredentials(%q)\ngot:  %q\nwant: %q", tc.input, got, tc.want)
			}
		})
	}
}

// TestMaskSensitiveData tests masking of values associated with sensitive keys in maps.
func TestMaskSensitiveData(t *testing.T) {
	// Access the package-level constant defined in util.go
	const mask = maskedValue

	testCases := []struct {
		name  string
		input map[string]interface{}
		want  map[string]interface{}
	}{
		{
			name:  "no sensitive keys",
			input: map[string]interface{}{"user": "test", "id": 123},
			want:  map[string]interface{}{"user": "test", "id": 123},
		},
		{
			name: "simple password key",
			input: map[string]interface{}{
				"username": "admin",
				"password": "password123", // Lowercase key
				"port":     5432,
			},
			want: map[string]interface{}{
				"username": "admin",
				"password": mask, // Value masked
				"port":     5432,
			},
		},
		{
			name: "mixed case sensitive keys",
			input: map[string]interface{}{
				"api_key":     "abcdef12345",
				"USER_SECRET": "mysecret",
				"AuthToken":   "bearer xyz",
				"data":        "plain",
			},
			want: map[string]interface{}{
				"api_key":     mask,
				"USER_SECRET": mask,
				"AuthToken":   mask,
				"data":        "plain",
			},
		},
		{
			name: "nested map with sensitive leaf key",
			input: map[string]interface{}{
				"config": map[string]interface{}{
					"host": "localhost",
					"credential": map[string]interface{}{
						"user": "dbuser",
						"pwd":  "dbpass", // 'pwd' key IS sensitive
					},
				},
				"level": "debug",
			},
			want: map[string]interface{}{
				"config": map[string]interface{}{
					"host": "localhost",
					"credential": map[string]interface{}{
						"user": "dbuser",
						"pwd":  mask,     // 'pwd' is sensitive, mask leaf value
					},
				},
				"level": "debug",
			},
		},
		{
			name: "nested map does not mask based on parent key",
			input: map[string]interface{}{
				"secret_config": map[string]interface{}{ // 'secret_config' IS sensitive
					"host": "secure-host", // 'host' is not sensitive
					"port": 1234,
				},
			},
			want: map[string]interface{}{ // Values inside not masked by default
				"secret_config": map[string]interface{}{
					"host": "secure-host",
					"port": 1234,
				},
			},
		},
		{
			name: "value is connection string",
			input: map[string]interface{}{
				"db_url":       "postgres://user:password@host/db", // Key not sensitive, but value is URI
				"service_pass": "svcpass",                          // Key IS sensitive
			},
			want: map[string]interface{}{
				"db_url":       fmt.Sprintf("postgres://user:%s@host/db", mask), // Value masked by MaskCredentials
				"service_pass": mask,                                            // Value masked because key is sensitive
			},
		},
		{
			name: "value is nested map with connection string and sensitive key",
			input: map[string]interface{}{
				"connection": map[string]interface{}{ // Key not sensitive
					"primary":   "postgres://user1:pass1@host1/db1", // Value is URI
					"secondary": "postgres://user2:pass2@host2/db2", // Value is URI
					"api_key":   "key123",                           // Key is sensitive
				},
			},
			want: map[string]interface{}{ // Should recurse
				"connection": map[string]interface{}{
					"primary":   fmt.Sprintf("postgres://user1:%s@host1/db1", mask), // Value masked by MaskCredentials
					"secondary": fmt.Sprintf("postgres://user2:%s@host2/db2", mask), // Value masked by MaskCredentials
					"api_key":   mask,                                               // Value masked because key is sensitive
				},
			},
		},
		{
			name:  "nil map input",
			input: nil,
			want:  nil,
		},
		{
			name:  "empty map input",
			input: map[string]interface{}{},
			want:  map[string]interface{}{},
		},
		{
			name: "map with nil sensitive value",
			input: map[string]interface{}{
				"username": "test",
				"password": nil, // Sensitive key, nil value
			},
			want: map[string]interface{}{
				"username": "test",
				"password": mask, // Masked even if nil because key matches
			},
		},
		{
			name: "map with non-string non-map value",
			input: map[string]interface{}{
				"id":      101,
				"apiKey":  1234567890, // Sensitive key, non-string/non-map value
				"enabled": true,
			},
			want: map[string]interface{}{
				"id":      101,
				"apiKey":  mask, // Masked because key is sensitive
				"enabled": true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function defined in util.go
			got := MaskSensitiveData(tc.input)
			// Use DeepEqual for comparing maps, especially nested ones
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("MaskSensitiveData()\ngot:  %#v\nwant: %#v", got, tc.want)
			}
		})
	}
}