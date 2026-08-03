package loganalysis

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxLineBytes       = 1 << 20
	maxDurationMS      = int64(24 * time.Hour / time.Millisecond)
	maxRevision        = int64(1 << 62)
	maxDBSizeBytes     = int64(1 << 62)
	maxSystemdFieldLen = 1 << 20
)

var (
	timestampPrefixPattern = regexp.MustCompile(`^([0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9:.+Z-]+)\s+`)
	durationPattern        = regexp.MustCompile(`(?i)(?:duration|took|latency)\s*[=:]?\s*([0-9]+(?:\.[0-9]+)?)\s*(ms|milliseconds?|s|seconds?|us|µs)`)
	revisionPattern        = regexp.MustCompile(`(?i)\b(?:revision|rev)\s*[:=]?\s*([0-9]+)`)
	dbSizePattern          = regexp.MustCompile(`(?i)\b(?:db[_ ]?size|database[_ ]?size|size[_ ]?bytes)\s*[:=]?\s*([0-9]+)`)
)

// ParseFile streams one log file, detecting gzip from its magic bytes.
func ParseFile(ctx context.Context, path string, sink EventSink) (Summary, error) {
	if sink == nil {
		sink = func(context.Context, Event) error { return nil }
	}
	if err := ctx.Err(); err != nil {
		return Summary{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return Summary{}, err
	}
	defer file.Close()

	header := make([]byte, 2)
	read, readErr := io.ReadFull(file, header)
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return Summary{}, readErr
	}
	reader := io.Reader(io.MultiReader(bytes.NewReader(header[:read]), file))
	var compressed *gzip.Reader
	if read == len(header) && header[0] == 0x1f && header[1] == 0x8b {
		compressed, err = gzip.NewReader(reader)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return Summary{}, ctx.Err()
			}
			return Summary{ParseErrors: 1}, nil
		}
		defer compressed.Close()
		reader = compressed
	}

	return parseReader(ctx, reader, sink)
}

func parseReader(ctx context.Context, reader io.Reader, sink EventSink) (Summary, error) {
	buffered := bufio.NewReaderSize(reader, 64*1024)
	var summary Summary
	var lineNumber int64
	for {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		line, readErr, overlong, err := readBoundedLine(ctx, buffered)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return summary, err
			}
			summary.ParseErrors++
			if readErr == io.EOF && len(line) == 0 {
				break
			}
		}
		terminalReadError := readErr != nil && readErr != io.EOF
		if len(line) == 0 && terminalReadError {
			summary.ParseErrors++
			break
		}
		if len(line) == 0 && readErr == io.EOF {
			break
		}
		lineNumber++
		summary.TotalLines++
		if overlong {
			summary.ParseErrors++
			summary.UnknownLines++
			event := unknownEvent(lineNumber, "line_too_long", "")
			if err := sink(ctx, event); err != nil {
				return summary, err
			}
		} else {
			event, recognized, parseError := parseLine(lineNumber, line)
			if !utf8.Valid(line) {
				event, recognized, parseError = unknownEvent(lineNumber, "encoding_error", string(line)), false, true
			}
			if parseError {
				summary.ParseErrors++
			}
			if recognized {
				summary.RecognizedEvents++
				updateObservedRange(&summary, event.ObservedAt)
			} else {
				summary.UnknownLines++
			}
			if err := sink(ctx, event); err != nil {
				return summary, err
			}
		}
		if terminalReadError {
			break
		}
		if readErr == io.EOF {
			break
		}
	}
	return summary, nil
}

func readBoundedLine(ctx context.Context, reader *bufio.Reader) ([]byte, error, bool, error) {
	var line []byte
	overlong := false
	for {
		if err := ctx.Err(); err != nil {
			return line, nil, overlong, err
		}
		chunk, err := reader.ReadSlice('\n')
		if !overlong {
			remaining := maxLineBytes + 1 - len(line)
			if remaining <= 0 || len(chunk) > remaining {
				line = append(line, chunk[:maxInt(0, remaining)]...)
				overlong = true
			} else {
				line = append(line, chunk...)
				if len(line) > maxLineBytes {
					overlong = true
				}
			}
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		return line, err, overlong, nil
	}
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func parseLine(lineNumber int64, raw []byte) (Event, bool, bool) {
	line := strings.TrimSpace(strings.TrimSuffix(string(raw), "\r"))
	if line == "" {
		return unknownEvent(lineNumber, "empty", line), false, false
	}

	if criTime, body, ok := stripCRI(line); ok {
		if event, recognized, parseError := parseJSON(lineNumber, body, criTime); recognized || parseError {
			return event, recognized, parseError
		}
		line = body
	}
	if message, timestamp, severity, source, ok := parseSystemd(line); ok {
		return eventFromMessage(lineNumber, message, timestamp, severity, source, nil, nil, nil), true, false
	}
	if event, recognized, parseError := parseJSON(lineNumber, line, nil); recognized || parseError {
		return event, recognized, parseError
	}

	timestamp, body := extractTextTimestamp(line)
	if body == "" {
		body = line
	}
	event := eventFromMessage(lineNumber, body, timestamp, severityFromText(body), sourceFromText(body), nil, nil, nil)
	if event.Type == EventUnknown {
		return unknownEvent(lineNumber, "unknown", body), false, false
	}
	return event, true, false
}

func parseJSON(lineNumber int64, line string, fallbackTime *time.Time) (Event, bool, bool) {
	if !strings.HasPrefix(strings.TrimSpace(line), "{") {
		return Event{}, false, false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &fields); err != nil {
		return unknownEvent(lineNumber, "json_error", line), false, true
	}
	message := firstString(fields, "msg", "message", "log")
	if message == "" {
		return unknownEvent(lineNumber, "json_missing_message", line), false, true
	}
	timestamp := firstTime(fields, "ts", "timestamp", "time", "observed_at", "observedAt")
	if timestamp == nil {
		timestamp = fallbackTime
	}
	severity := severityFromValue(firstString(fields, "level", "severity", "priority"))
	source := firstString(fields, "caller", "component", "source", "logger", "name")
	if source == "" {
		source = sourceFromText(message)
	}
	eventType := eventTypeFromValue(firstString(fields, "event_type", "eventType", "type"))
	duration := firstSafeInt(fields, maxDurationMS, "duration_ms", "durationMs", "duration", "took_ms", "tookMs")
	revision := firstSafeInt(fields, maxRevision, "revision", "rev", "revision_number", "revisionNumber")
	dbSize := firstSafeInt(fields, maxDBSizeBytes, "db_size_bytes", "dbSizeBytes", "db_size", "database_size")
	event := eventFromMessage(lineNumber, message, timestamp, severity, source, duration, revision, dbSize)
	if eventType != EventUnknown {
		event.Type = eventType
	}
	return event, event.Type != EventUnknown, false
}

func parseSystemd(line string) (string, *time.Time, Severity, string, bool) {
	if !strings.Contains(line, "MESSAGE=") {
		return "", nil, SeverityUnknown, "", false
	}
	message := systemdField(line, "MESSAGE")
	if message == "" || len(message) > maxSystemdFieldLen {
		return "", nil, SeverityUnknown, "", false
	}
	timestamp := parseTimestamp(systemdField(line, "__REALTIME_TIMESTAMP"))
	severity := severityFromPriority(systemdField(line, "PRIORITY"))
	if severity == SeverityUnknown {
		severity = severityFromValue(systemdField(line, "LEVEL"))
	}
	source := systemdField(line, "SYSLOG_IDENTIFIER")
	if source == "" {
		source = systemdField(line, "_SYSTEMD_UNIT")
	}
	if source == "" {
		source = sourceFromText(message)
	}
	return message, timestamp, severity, source, true
}

func systemdField(line, key string) string {
	marker := key + "="
	start := strings.Index(line, marker)
	if start < 0 {
		return ""
	}
	value := line[start+len(marker):]
	for _, nextKey := range []string{"__REALTIME_TIMESTAMP", "PRIORITY", "LEVEL", "SYSLOG_IDENTIFIER", "_SYSTEMD_UNIT", "MESSAGE"} {
		if nextKey == key {
			continue
		}
		if index := strings.Index(value, " "+nextKey+"="); index >= 0 {
			value = value[:index]
		}
	}
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		value = strings.Trim(value, "\"")
	}
	return value
}

func stripCRI(line string) (*time.Time, string, bool) {
	parts := strings.SplitN(line, " ", 4)
	if len(parts) != 4 || (parts[1] != "stdout" && parts[1] != "stderr") {
		return nil, line, false
	}
	timestamp := parseTimestamp(parts[0])
	if timestamp == nil {
		return nil, line, false
	}
	return timestamp, strings.TrimSpace(parts[3]), true
}

func extractTextTimestamp(line string) (*time.Time, string) {
	matches := timestampPrefixPattern.FindStringSubmatch(line)
	if len(matches) != 2 {
		return nil, line
	}
	return parseTimestamp(matches[1]), strings.TrimSpace(line[len(matches[0]):])
}

func eventFromMessage(lineNumber int64, message string, timestamp *time.Time, severity Severity, source string, duration, revision, dbSize *int64) Event {
	if severity == SeverityUnknown {
		severity = severityFromText(message)
	}
	if source == "" {
		source = sourceFromText(message)
	}
	if duration == nil {
		duration = durationFromText(message)
	}
	if revision == nil {
		revision = revisionFromText(message)
	}
	if dbSize == nil {
		dbSize = dbSizeFromText(message)
	}
	status := "recognized"
	if timestamp == nil {
		status = "unknown_time"
	}
	return Event{
		LineNumber: lineNumber, ObservedAt: timestamp, Type: eventTypeFromMessage(message),
		Severity: severity, Source: source, DurationMS: duration, Revision: revision,
		DBSizeBytes: dbSize, ParseStatus: status, MessageFingerprint: fingerprint(message),
	}
}

func unknownEvent(lineNumber int64, status, message string) Event {
	return Event{
		LineNumber: lineNumber, Type: EventUnknown, Severity: SeverityUnknown, Source: "unknown",
		ParseStatus: status, MessageFingerprint: fingerprint(message),
	}
}

func eventTypeFromValue(value string) EventType {
	value = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(value, "-", "_")))
	for _, candidate := range AllEventTypes() {
		if value == string(candidate) {
			return candidate
		}
	}
	return EventUnknown
}

func eventTypeFromMessage(message string) EventType {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "database space exceeded"), strings.Contains(lower, "space quota exceeded"), strings.Contains(lower, "no space left"), strings.Contains(lower, "nospace"):
		return EventNoSpace
	case strings.Contains(lower, "quota exceeded"), strings.Contains(lower, "quotaexceeded"):
		return EventQuotaExceeded
	case strings.Contains(lower, "compaction"), strings.Contains(lower, "compacted revision"), strings.Contains(lower, "compact revision"):
		return EventCompaction
	case strings.Contains(lower, "defrag"), strings.Contains(lower, "defragment"):
		return EventDefrag
	case strings.Contains(lower, "fdatasync") && (strings.Contains(lower, "slow") || strings.Contains(lower, "took") || strings.Contains(lower, "latency")):
		return EventSlowFdatasync
	case strings.Contains(lower, "wal") && strings.Contains(lower, "fsync"):
		return EventWALFsync
	case strings.Contains(lower, "backend commit") && (strings.Contains(lower, "slow") || strings.Contains(lower, "took") || strings.Contains(lower, "latency")):
		return EventSlowBackendCommit
	case strings.Contains(lower, "apply request") && (strings.Contains(lower, "slow") || strings.Contains(lower, "took") || strings.Contains(lower, "latency")):
		return EventSlowApply
	case strings.Contains(lower, "leader changed"), strings.Contains(lower, "leader change"), strings.Contains(lower, "became leader"), strings.Contains(lower, "elected leader"):
		return EventLeaderChange
	case strings.Contains(lower, "request timed out"), strings.Contains(lower, "request timeout"), strings.Contains(lower, "deadline exceeded"), strings.Contains(lower, "timeout"):
		return EventRequestTimeout
	case strings.Contains(lower, "snapshot") && (strings.Contains(lower, "restore") || strings.Contains(lower, "restor")):
		return EventSnapshotRestore
	case strings.Contains(lower, "snapshot") && (strings.Contains(lower, "save") || strings.Contains(lower, "saved") || strings.Contains(lower, "saving")):
		return EventSnapshotSave
	case strings.Contains(lower, "lease") && (strings.Contains(lower, "revoke") || strings.Contains(lower, "revoked")):
		return EventLeaseRevoke
	case strings.Contains(lower, "corrupt"), strings.Contains(lower, "integrity check"), strings.Contains(lower, "corruption check"):
		return EventCorruptionCheck
	case strings.Contains(lower, "large request"), strings.Contains(lower, "request too large"), strings.Contains(lower, "max request size"):
		return EventLargeRequest
	case strings.Contains(lower, "backend commit"):
		return EventBackendCommit
	default:
		return EventUnknown
	}
}

func severityFromValue(value string) Severity {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0", "1", "2", "3", "error", "err", "fatal", "panic":
		return SeverityError
	case "4", "warn", "warning":
		return SeverityWarn
	case "5", "6", "7", "info", "information", "debug", "trace":
		return SeverityInfo
	default:
		return SeverityUnknown
	}
}

func severityFromPriority(value string) Severity {
	if value == "" {
		return SeverityUnknown
	}
	priority, err := strconv.Atoi(value)
	if err != nil || priority < 0 || priority > 7 {
		return SeverityUnknown
	}
	return severityFromValue(strconv.Itoa(priority))
}

func severityFromText(message string) Severity {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "fatal"), strings.Contains(lower, "panic"), strings.Contains(lower, "corrupt"), strings.Contains(lower, "error"), strings.Contains(lower, "failed"):
		return SeverityError
	case strings.Contains(lower, "warn"), strings.Contains(lower, "exceeded"), strings.Contains(lower, "timeout"), strings.Contains(lower, "too long"):
		return SeverityWarn
	case message != "":
		return SeverityInfo
	default:
		return SeverityUnknown
	}
}

func sourceFromText(message string) string {
	lower := strings.ToLower(message)
	for _, item := range []struct {
		name string
		part string
	}{
		{"mvcc", "mvcc"}, {"backend", "backend"}, {"wal", "wal"}, {"raft", "raft"},
		{"lease", "lease"}, {"etcdserver", "etcdserver"},
	} {
		if strings.Contains(lower, item.part) {
			return item.name
		}
	}
	return "unknown"
}

func firstString(fields map[string]json.RawMessage, names ...string) string {
	for _, name := range names {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) == nil {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstTime(fields map[string]json.RawMessage, names ...string) *time.Time {
	for _, name := range names {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) == nil {
			if timestamp := parseTimestamp(value); timestamp != nil {
				return timestamp
			}
		}
		if number, err := strconv.ParseInt(strings.Trim(string(raw), `"`), 10, 64); err == nil {
			return parseTimestamp(strconv.FormatInt(number, 10))
		}
	}
	return nil
}

func firstSafeInt(fields map[string]json.RawMessage, maximum int64, names ...string) *int64 {
	for _, name := range names {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		value, err := strconv.ParseInt(strings.Trim(strings.TrimSpace(string(raw)), `"`), 10, 64)
		if err == nil && value >= 0 && value <= maximum {
			return &value
		}
	}
	return nil
}

func parseTimestamp(value string) *time.Time {
	value = strings.TrimSpace(strings.Trim(value, `"`))
	if value == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			utc := parsed.UTC()
			return &utc
		}
	}
	if micros, err := strconv.ParseInt(value, 10, 64); err == nil && micros >= 0 {
		parsed := time.Unix(0, micros*1000).UTC()
		return &parsed
	}
	return nil
}

func durationFromText(message string) *int64 {
	matches := durationPattern.FindStringSubmatch(message)
	if len(matches) != 3 {
		return nil
	}
	value, err := strconv.ParseFloat(matches[1], 64)
	if err != nil || value < 0 {
		return nil
	}
	multiplier := float64(1)
	switch strings.ToLower(matches[2]) {
	case "s", "sec", "secs", "second", "seconds":
		multiplier = 1000
	case "us", "µs":
		multiplier = 0.001
	}
	result := int64(value * multiplier)
	if result < 0 || result > maxDurationMS {
		return nil
	}
	return &result
}

func revisionFromText(message string) *int64 {
	matches := revisionPattern.FindStringSubmatch(message)
	if len(matches) != 2 {
		return nil
	}
	value, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil || value < 0 || value > maxRevision {
		return nil
	}
	return &value
}

func dbSizeFromText(message string) *int64 {
	matches := dbSizePattern.FindStringSubmatch(message)
	if len(matches) != 2 {
		return nil
	}
	value, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil || value < 0 || value > maxDBSizeBytes {
		return nil
	}
	return &value
}

func fingerprint(message string) string {
	digest := sha256.Sum256([]byte(message))
	return hex.EncodeToString(digest[:])
}

func updateObservedRange(summary *Summary, observedAt *time.Time) {
	if observedAt == nil {
		return
	}
	if summary.FirstObservedAt == nil || observedAt.Before(*summary.FirstObservedAt) {
		value := observedAt.UTC()
		summary.FirstObservedAt = &value
	}
	if summary.LastObservedAt == nil || observedAt.After(*summary.LastObservedAt) {
		value := observedAt.UTC()
		summary.LastObservedAt = &value
	}
}
