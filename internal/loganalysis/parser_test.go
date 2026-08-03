package loganalysis_test

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/loganalysis"
)

func TestParseFileRecognizesJSONCRIAndEtcdText(t *testing.T) {
	path := writeLogFile(t, strings.Join([]string{
		`{"ts":"2026-08-03T10:00:00.123Z","level":"warn","caller":"mvcc","msg":"mvcc: database space exceeded","revision":42}`,
		`2026-08-03T10:01:00Z etcdserver: compacted revision 123`,
		`2026-08-03T10:02:00Z etcdserver: defragmentation finished, took=250ms`,
		`2026-08-03T10:03:00Z etcdserver: leader changed from 1 to 2`,
		`2026-08-03T10:04:00Z etcdserver: apply request took too long duration=1500ms`,
		`2026-08-03T10:05:00.000000000Z stdout F {"level":"info","caller":"raft","msg":"leader changed"}`,
		`__REALTIME_TIMESTAMP=2026-08-03T10:06:00Z PRIORITY=3 SYSLOG_IDENTIFIER=etcd MESSAGE=mvcc: database space exceeded`,
	}, "\n"))

	var events []loganalysis.Event
	summary, err := loganalysis.ParseFile(context.Background(), path, func(_ context.Context, event loganalysis.Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	if summary.TotalLines != 7 || len(events) != 7 {
		t.Fatalf("summary/events = %d/%d, want 7/7", summary.TotalLines, len(events))
	}
	if summary.RecognizedEvents != 7 || summary.UnknownLines != 0 || summary.ParseErrors != 0 {
		t.Fatalf("summary = %+v, want all lines recognized", summary)
	}
	if events[0].Type != loganalysis.EventNoSpace || events[0].Severity != loganalysis.SeverityWarn {
		t.Fatalf("JSON event = %+v, want NOSPACE/WARN", events[0])
	}
	if events[0].Revision == nil || *events[0].Revision != 42 || events[0].Source != "mvcc" {
		t.Fatalf("JSON fields = %+v, want revision 42/source mvcc", events[0])
	}
	if events[1].Type != loganalysis.EventCompaction || events[2].Type != loganalysis.EventDefrag {
		t.Fatalf("text event types = %s/%s, want compaction/defrag", events[1].Type, events[2].Type)
	}
	if events[2].DurationMS == nil || *events[2].DurationMS != 250 {
		t.Fatalf("defrag duration = %v, want 250", events[2].DurationMS)
	}
	if events[3].Type != loganalysis.EventLeaderChange || events[4].Type != loganalysis.EventSlowApply {
		t.Fatalf("leader/apply types = %s/%s", events[3].Type, events[4].Type)
	}
	if events[5].Type != loganalysis.EventLeaderChange || events[6].Type != loganalysis.EventNoSpace {
		t.Fatalf("CRI/systemd types = %s/%s", events[5].Type, events[6].Type)
	}
	if events[0].ObservedAt == nil || !events[0].ObservedAt.Equal(time.Date(2026, 8, 3, 10, 0, 0, 123000000, time.UTC)) {
		t.Fatalf("observed time = %v", events[0].ObservedAt)
	}
}

func TestParseFileDetectsGzipByMagicAndKeepsUnknownLinesRedacted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.data")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := gzip.NewWriter(file)
	secret := "unknown line contains bearer-token-secret"
	if _, err := io.WriteString(writer, secret+"\n"+secret+"\n"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	var events []loganalysis.Event
	summary, err := loganalysis.ParseFile(context.Background(), path, func(_ context.Context, event loganalysis.Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	if summary.TotalLines != 2 || summary.UnknownLines != 2 || summary.RecognizedEvents != 0 {
		t.Fatalf("summary = %+v, want two unknown lines", summary)
	}
	if len(events) != 2 || events[0].Type != loganalysis.EventUnknown || events[0].Severity != loganalysis.SeverityUnknown || events[0].Source != "unknown" {
		t.Fatalf("events = %+v, want redacted unknown events", events)
	}
	if events[0].MessageFingerprint == "" || events[0].MessageFingerprint != events[1].MessageFingerprint {
		t.Fatalf("fingerprints = %q/%q, want stable non-empty value", events[0].MessageFingerprint, events[1].MessageFingerprint)
	}
	expectedFingerprint := fmt.Sprintf("%x", sha256.Sum256([]byte(secret)))
	if len(events[0].MessageFingerprint) != 64 || events[0].MessageFingerprint != expectedFingerprint {
		t.Fatalf("fingerprint = %q, want sha256 %q", events[0].MessageFingerprint, expectedFingerprint)
	}
	if strings.Contains(events[0].MessageFingerprint, secret) || strings.Contains(fmt.Sprintf("%+v", events[0]), secret) {
		t.Fatalf("event contains raw log content: %+v", events[0])
	}
}

func TestParseFileCountsOverlongLinesAndHonorsCancellation(t *testing.T) {
	path := writeLogFile(t, strings.Repeat("x", 1<<20)+"\n"+`{"ts":"2026-08-03T10:00:00Z","msg":"compacted revision 9"}`+"\n")
	var events []loganalysis.Event
	summary, err := loganalysis.ParseFile(context.Background(), path, func(_ context.Context, event loganalysis.Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	if summary.TotalLines != 2 || summary.ParseErrors != 1 || len(events) != 2 {
		t.Fatalf("summary/events = %+v/%d, want two lines and one parse error", summary, len(events))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = loganalysis.ParseFile(ctx, path, func(_ context.Context, event loganalysis.Event) error {
		t.Fatalf("sink called after cancellation: %+v", event)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v, want context.Canceled", err)
	}
}

func TestParseFileRejectsOutOfRangeNumericFields(t *testing.T) {
	path := writeLogFile(t, `{"ts":"2026-08-03T10:00:00Z","msg":"backend commit","duration_ms":-1,"revision":-2,"db_size_bytes":9223372036854775807}`+"\n")
	var events []loganalysis.Event
	if _, err := loganalysis.ParseFile(context.Background(), path, func(_ context.Context, event loganalysis.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	if len(events) != 1 || events[0].Type != loganalysis.EventBackendCommit {
		t.Fatalf("events = %+v, want backend commit", events)
	}
	if events[0].DurationMS != nil || events[0].Revision != nil || events[0].DBSizeBytes != nil {
		t.Fatalf("out-of-range fields = %+v, want nil", events[0])
	}
}

func writeLogFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.log")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
