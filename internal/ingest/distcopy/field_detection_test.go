// Copied from grafana/loki@acaa752c pkg/distributor/field_detection_test.go
// (the tests of the functions in field_detection.go); modifications: package
// distcopy; Distributor-based tests dropped; the non-ASCII cases and the
// benchmark of detectLevelFromLogLine are upstream's, placed in runakefli's
// trimmed table; validationContext built with
// runakefli's copy.

package distcopy

import (
	"strings"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func Test_detectLogLevelFromLogEntry(t *testing.T) {
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       true,
			allowStructuredMetadata: true,
			logLevelFields:          []string{"level", "LEVEL", "Level", "severity", "SEVERITY", "Severity", "lvl", "LVL", "Lvl"},
		})

	for _, tc := range []struct {
		name             string
		entry            string
		expectedLogLevel string
	}{
		{
			name:             "non otlp with log level keywords in log line",
			entry:            "this is a warning log",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with debug keyword in log line",
			entry:            "this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with DEBUG keyword in log line",
			entry:            "this is a DEBUG message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with debug: prefix in log line",
			entry:            "debug: something happened",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with DEBUG: prefix in log line",
			entry:            "DEBUG: something happened",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with critical keyword in log line",
			entry:            "this is a critical message",
			expectedLogLevel: constants.LogLevelCritical,
		},
		{
			name:             "non otlp with CRITICAL keyword in log line",
			entry:            "this is a CRITICAL message",
			expectedLogLevel: constants.LogLevelCritical,
		},
		{
			name:             "non otlp with critical: prefix in log line",
			entry:            "critical: something happened",
			expectedLogLevel: constants.LogLevelCritical,
		},
		{
			name:             "non otlp with CRITICAL: prefix in log line",
			entry:            "CRITICAL: something happened",
			expectedLogLevel: constants.LogLevelCritical,
		},
		{
			name:             "non otlp with info keyword in log line",
			entry:            "this is a info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with error keyword in log line",
			entry:            "this is a error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with fatal keyword in log line",
			entry:            "this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
		{
			name:             "non otlp without any of the log level keywords in log line",
			entry:            "foo",
			expectedLogLevel: constants.LogLevelUnknown,
		},
		{
			name:             "non otlp with [debug] bracket pattern in log line",
			entry:            "[debug] this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with [DEBUG] bracket pattern in log line",
			entry:            "[DEBUG] this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with [warn] bracket pattern in log line",
			entry:            "[warn] this is a warn message",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with [WARN] bracket pattern in log line",
			entry:            "[WARN] this is a warn message",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with [error] bracket pattern in log line",
			entry:            "[error] this is an error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with [ERROR] bracket pattern in log line",
			entry:            "[ERROR] this is an error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with [info] bracket pattern in log line",
			entry:            "[info] this is an info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with [INFO] bracket pattern in log line",
			entry:            "[INFO] this is an info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with [fatal] bracket pattern in log line",
			entry:            "[fatal] this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
		{
			name:             "non otlp with [FATAL] bracket pattern in log line",
			entry:            "[FATAL] this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
		{
			name:             "non otlp with <debug> angle bracket pattern in log line",
			entry:            "<debug> this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with <DEBUG> angle bracket pattern in log line",
			entry:            "<DEBUG> this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with <warn> angle bracket pattern in log line",
			entry:            "<warn> this is a warn message",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with <WARN> angle bracket pattern in log line",
			entry:            "<WARN> this is a warn message",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with <error> angle bracket pattern in log line",
			entry:            "<error> this is an error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with <ERROR> angle bracket pattern in log line",
			entry:            "<ERROR> this is an error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with <info> angle bracket pattern in log line",
			entry:            "<info> this is an info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with <INFO> angle bracket pattern in log line",
			entry:            "<INFO> this is an info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with <fatal> angle bracket pattern in log line",
			entry:            "<fatal> this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
		{
			name:             "non otlp with <FATAL> angle bracket pattern in log line",
			entry:            "<FATAL> this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
		{
			name:             "non-ASCII text around a bounded keyword",
			entry:            "größe=ünï [ERROR] fehlgeschlagen — ñ",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non-ASCII character adjacent to keyword is not a boundary",
			entry:            "éerror erroré",
			expectedLogLevel: constants.LogLevelUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := logproto.Entry{
				Timestamp: time.Now(),
				Line:      tc.entry,
			}
			level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Test_detectLogLevelFromLogEntryWithCustomLabels(t *testing.T) {
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       true,
			allowStructuredMetadata: true,
			logLevelFields:          []string{"custom_level"},
		})

	for _, tc := range []struct {
		name             string
		entry            string
		expectedLogLevel string
	}{
		{
			name:             "custom level field in log line",
			entry:            `{"custom_level":"debug","message":"test"}`,
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "custom level field in log line uppercase",
			entry:            `{"custom_level":"DEBUG","message":"test"}`,
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "custom level field not present",
			entry:            `{"message":"test"}`,
			expectedLogLevel: constants.LogLevelUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := logproto.Entry{
				Timestamp: time.Now(),
				Line:      tc.entry,
			}
			level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Test_DetectGenericFields_Enabled(t *testing.T) {
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       false,
			allowStructuredMetadata: true,
			logLevelFields:          []string{},
		})

	entry := logproto.Entry{
		Timestamp: time.Now(),
		Line:      "this is a test message",
	}
	level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
	require.Equal(t, constants.LogLevelUnknown, level)
}

func Test_DetectGenericFields(t *testing.T) {
	// Test that generic field detection is working
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       true,
			allowStructuredMetadata: true,
			logLevelFields:          []string{"level"},
		})

	entry := logproto.Entry{
		Timestamp: time.Now(),
		Line:      `{"level":"info","message":"test"}`,
	}
	level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
	require.Equal(t, constants.LogLevelInfo, level)
}

func Test_NormalizeLogLevel(t *testing.T) {
	// Test the normalizeLogLevel function through the detectLogLevelFromLogEntry
	// which calls extractLogLevel which calls normalizeLogLevel
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       true,
			allowStructuredMetadata: true,
			logLevelFields:          []string{"level"},
		})

	for _, tc := range []struct {
		name             string
		entry            string
		expectedLogLevel string
	}{
		// Test all the level variations
		{name: "WaRn", entry: `{"level":"WaRn"}`, expectedLogLevel: constants.LogLevelWarn},
		{name: "InFo", entry: `{"level":"InFo"}`, expectedLogLevel: constants.LogLevelInfo},
		{name: "CRITICAL", entry: `{"level":"CRITICAL"}`, expectedLogLevel: constants.LogLevelCritical},
		{name: "Debug", entry: `{"level":"Debug"}`, expectedLogLevel: constants.LogLevelDebug},
		{name: "FaTaL", entry: `{"level":"FaTaL"}`, expectedLogLevel: constants.LogLevelFatal},
		{name: "tRaCe", entry: `{"level":"tRaCe"}`, expectedLogLevel: constants.LogLevelTrace},
		{name: "ERROR", entry: `{"level":"ERROR"}`, expectedLogLevel: constants.LogLevelError},
		// Test abbreviations
		{name: "wrn", entry: `{"level":"wrn"}`, expectedLogLevel: constants.LogLevelWarn},
		{name: "dbg", entry: `{"level":"dbg"}`, expectedLogLevel: constants.LogLevelDebug},
		{name: "inf", entry: `{"level":"inf"}`, expectedLogLevel: constants.LogLevelInfo},
		{name: "err", entry: `{"level":"err"}`, expectedLogLevel: constants.LogLevelError},
		// Test full words
		{name: "warning", entry: `{"level":"warning"}`, expectedLogLevel: constants.LogLevelWarn},
		{name: "information", entry: `{"level":"information"}`, expectedLogLevel: constants.LogLevelInfo},
		// Test unknown level (should be returned as-is)
		{name: "unknown", entry: `{"level":"unknown"}`, expectedLogLevel: "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := logproto.Entry{
				Timestamp: time.Now(),
				Line:      tc.entry,
			}
			level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Test_GenericFieldDetection(t *testing.T) {
	// Test generic field detection with discoverGenericFields
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       false,
			allowStructuredMetadata: true,
			logLevelFields:          []string{},
			discoverGenericFields: map[string][]string{
				"service_name": {"service", "app"},
			},
		})

	// Test that the field detector can be created with generic fields
	require.NotNil(t, ld)
	// The actual detection happens in discoverFields which is called from processStreamEntries
	// which is hard to test directly. For now, just ensure the setup works.
}

func TestGetLevelUsingJsonParser(t *testing.T) {
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       true,
			allowStructuredMetadata: true,
			logLevelFields:          []string{"level", "severity"},
		})

	for _, tc := range []struct {
		name             string
		line             string
		expectedLogLevel string
	}{
		{
			name:             "json with level field",
			line:             `{"level":"info","message":"test"}`,
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "json with severity field",
			line:             `{"severity":"warn","message":"test"}`,
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "json with level field uppercase",
			line:             `{"level":"INFO","message":"test"}`,
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "json with severity field uppercase",
			line:             `{"severity":"WARN","message":"test"}`,
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "json without level or severity",
			line:             `{"message":"test"}`,
			expectedLogLevel: constants.LogLevelUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := logproto.Entry{
				Timestamp: time.Now(),
				Line:      tc.line,
			}
			level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Test_detectLevelFromLogLine(t *testing.T) {
	for _, tc := range []struct {
		line             string
		expectedLogLevel string
	}{
		{"this is a warning log", constants.LogLevelWarn},
		{"this is a debug message", constants.LogLevelDebug},
		{"this is a critical message", constants.LogLevelCritical},
		{"this is a error message", constants.LogLevelError},
		{"this is a fatal message", constants.LogLevelFatal},
		{"this is a info message", constants.LogLevelInfo},
		{"foo", constants.LogLevelUnknown},
	} {
		t.Run(tc.line, func(t *testing.T) {
			level := detectLevelFromLogLine(tc.line)
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Test_detectLevelFromLogLine_earliestPosition(t *testing.T) {
	for _, tc := range []struct {
		line             string
		expectedLogLevel string
	}{
		{"debug: something happened", constants.LogLevelDebug},
		{"DEBUG: something happened", constants.LogLevelDebug},
		{"warn: something happened", constants.LogLevelWarn},
		{"WARN: something happened", constants.LogLevelWarn},
		{"error: something happened", constants.LogLevelError},
		{"ERROR: something happened", constants.LogLevelError},
		{"critical: something happened", constants.LogLevelCritical},
		{"CRITICAL: something happened", constants.LogLevelCritical},
		{"fatal: something happened", constants.LogLevelFatal},
		{"FATAL: something happened", constants.LogLevelFatal},
		{"info: something happened", constants.LogLevelInfo},
		{"INFO: something happened", constants.LogLevelInfo},
	} {
		t.Run(tc.line, func(t *testing.T) {
			level := detectLevelFromLogLine(tc.line)
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Benchmark_detectLevelFromLogLine(b *testing.B) {
	longPrefix := strings.Repeat("the quick brown fox jumped over the lazy dog ", 32)

	for _, bm := range []struct {
		name     string
		log      string
		expected string
	}{
		{
			name:     "level at start",
			log:      "ERROR 1234 --- [main] com.example.App : Application startup failed due to connection refused",
			expected: constants.LogLevelError,
		},
		{
			name:     "level in middle",
			log:      "2024-01-01 10:00:00.000 ERROR 1234 --- [main] com.example.App : Application startup failed",
			expected: constants.LogLevelError,
		},
		{
			name:     "level at end of long line",
			log:      longPrefix + "info",
			expected: constants.LogLevelInfo,
		},
		{
			name:     "no level in long line",
			log:      longPrefix,
			expected: constants.LogLevelUnknown,
		},
		{
			name:     "unbounded keywords only",
			log:      "fooerror errorbar misc:error error_code " + longPrefix,
			expected: constants.LogLevelUnknown,
		},
	} {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			var level string
			for i := 0; i < b.N; i++ {
				level = detectLevelFromLogLine(bm.log)
			}
			require.Equal(b, bm.expected, level)
		})
	}
}
