package auditanalysis

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A parser that keeps the full User-Agent, source IP, URI, or object body
// would make this test fail and would persist credentials or workload data.
func TestParseFileNormalizesWriteEventWithoutRawPayload(t *testing.T) {
	raw := `{"apiVersion":"audit.k8s.io/v1","kind":"Event","auditID":"id-1",` +
		`"stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:02:03Z",` +
		`"verb":"update","user":{"username":"system:serviceaccount:kube-system:controller"},` +
		`"userAgent":"kube-controller-manager/v1.30.2 (linux/amd64) secret-tail",` +
		`"sourceIPs":["10.2.3.44"],"requestURI":"/api/v1/namespaces/default/configmaps/cm?token=private",` +
		`"objectRef":{"apiVersion":"v1","resource":"configmaps","namespace":"default","name":"cm"},` +
		`"responseStatus":{"code":200},"requestObject":{"data":{"token":"private-token"}},` +
		`"responseObject":{"metadata":{"name":"cm"}}}`
	path := filepath.Join(t.TempDir(), "audit.log")
	if err := os.WriteFile(path, []byte(raw+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var events []Event
	summary, err := ParseFile(context.Background(), path, func(_ context.Context, event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events=%d, want 1", len(events))
	}
	got := events[0]
	if summary.WriteEvents != 1 || got.Verb != "update" || got.Username != "system:serviceaccount:kube-system:controller" ||
		got.UserAgent != "kube-controller-manager/v1.30.2" || got.SourceNetwork != "10.2.3.0/24" ||
		got.ObjectKeyHash == "" || got.ResponseCode != 200 || got.RequestObjectBytes == 0 || got.ResponseObjectBytes == 0 {
		t.Fatalf("summary=%+v event=%+v", summary, got)
	}
	encoded := fmt.Sprintf("%+v", got)
	for _, secret := range []string{"private-token", "10.2.3.44", "token=private", "secret-tail"} {
		if strings.Contains(encoded, secret) {
			t.Fatalf("leaked %q: %s", secret, encoded)
		}
	}
}

// Dropping stage rank would prevent the repository from deterministically
// replacing RequestReceived with the completed response without buffering.
func TestParseFileStreamsStagesWithRepositoryPreferenceRank(t *testing.T) {
	path := writeAuditFile(t, "audit.log", strings.Join([]string{
		`{"auditID":"same","stage":"RequestReceived","requestReceivedTimestamp":"2026-08-12T01:00:00Z","verb":"update","user":{"username":"alice"},"objectRef":{"apiVersion":"v1","resource":"configmaps","namespace":"default","name":"cm"}}`,
		`{"auditID":"same","stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:00:01Z","verb":"update","user":{"username":"alice"},"objectRef":{"apiVersion":"v1","resource":"configmaps","namespace":"default","name":"cm"},"responseStatus":{"code":204}}`,
	}, "\n")+"\n")

	var events []Event
	summary, err := ParseFile(context.Background(), path, func(_ context.Context, event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.TotalLines != 2 || summary.ValidEvents != 2 || summary.WriteEvents != 2 || len(events) != 2 ||
		events[0].StageRank >= events[1].StageRank || events[1].Stage != "ResponseComplete" || events[1].ResponseCode != 204 {
		t.Fatalf("summary=%+v events=%+v", summary, events)
	}
}

// Removing gzip magic detection would turn valid compressed evidence into an
// unknown line instead of producing an Audit event.
func TestParseFileDetectsGzipByMagic(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write([]byte(`{"auditID":"gzip","stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:00:00Z","verb":"patch","user":{"username":"bob"},"objectRef":{"apiVersion":"v1","resource":"pods","namespace":"default","name":"pod"}}` + "\n"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	path := writeAuditBytes(t, "audit.data", compressed.Bytes())
	var events []Event
	_, err := ParseFile(context.Background(), path, func(_ context.Context, event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil || len(events) != 1 || events[0].Verb != "patch" {
		t.Fatalf("err=%v events=%+v", err, events)
	}
}

// Removing identity redaction or using a different key construction than
// Snapshot analysis would expose a secret name or break exact matching.
func TestObjectKeyHashRedactsSensitiveObjectsAndMatchesSnapshotKey(t *testing.T) {
	gotHash, displayName := ObjectKeyHash("", "secrets", "default", "db-password")
	wantHash := hashString("/registry/secrets/default/db-password")
	if gotHash != wantHash || displayName != "redacted:"+wantHash[:12] || strings.Contains(displayName, "db-password") {
		t.Fatalf("hash=%q display=%q wantHash=%q", gotHash, displayName, wantHash)
	}
}

// Changing IPv6 normalization from /64 to a full address would persist a
// complete client address in task storage.
func TestParseFileMasksIPv6SourceAt64Bits(t *testing.T) {
	path := writeAuditFile(t, "audit.log", `{"auditID":"ipv6","stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:00:00Z","verb":"create","sourceIPs":["2001:db8:1:2::44"],"objectRef":{"apiVersion":"v1","resource":"pods","namespace":"default","name":"pod"}}`+"\n")
	var got Event
	_, err := ParseFile(context.Background(), path, func(_ context.Context, event Event) error { got = event; return nil })
	if err != nil || got.SourceNetwork != "2001:db8:1:2::/64" || strings.Contains(fmt.Sprintf("%+v", got), "2001:db8:1:2::44") {
		t.Fatalf("err=%v event=%+v", err, got)
	}
}

// Returning on one malformed record would hide valid evidence later in a
// partially damaged Audit export.
func TestParseFileCountsBadJSONAndContinues(t *testing.T) {
	path := writeAuditFile(t, "audit.log", "{bad json}\n"+`{"auditID":"good","stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:00:00Z","verb":"delete","objectRef":{"apiVersion":"v1","resource":"pods","namespace":"default","name":"pod"}}`+"\n")
	var events []Event
	summary, err := ParseFile(context.Background(), path, func(_ context.Context, event Event) error { events = append(events, event); return nil })
	if err != nil || summary.TotalLines != 2 || summary.ParseErrors != 1 || summary.UnknownLines != 1 || summary.ValidEvents != 1 || len(events) != 1 {
		t.Fatalf("err=%v summary=%+v events=%+v", err, summary, events)
	}
}

// A line over the explicit safety bound must not allocate without limit or
// prevent a later valid event from being parsed.
func TestParseFileCountsOverlongLineAndContinues(t *testing.T) {
	overlong := bytes.Repeat([]byte{'x'}, maxLineBytes+1)
	valid := []byte(`{"auditID":"after-limit","stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:00:00Z","verb":"update","objectRef":{"apiVersion":"v1","resource":"pods","namespace":"default","name":"pod"}}` + "\n")
	payload := append(append(overlong, '\n'), valid...)
	path := writeAuditBytes(t, "audit.log", payload)
	var events []Event
	summary, err := ParseFile(context.Background(), path, func(_ context.Context, event Event) error { events = append(events, event); return nil })
	if err != nil || summary.TotalLines != 2 || summary.ParseErrors != 1 || summary.UnknownLines != 1 || len(events) != 1 {
		t.Fatalf("err=%v summary=%+v events=%+v", err, summary, events)
	}
}

// Ignoring an already-cancelled context would start expensive I/O after the
// caller has abandoned the analysis task.
func TestParseFileHonorsCancelledContext(t *testing.T) {
	path := writeAuditFile(t, "audit.log", "{}\n")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ParseFile(ctx, path, nil); err != context.Canceled {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
}

// Removing the expanded-input limit would allow a tiny gzip file to consume
// unbounded CPU, memory bandwidth, and disk writes after decompression.
func TestParseReaderStopsAtExpandedInputLimit(t *testing.T) {
	payload := strings.Repeat("{}\n", 10)
	_, err := parseReaderLimited(context.Background(), strings.NewReader(payload), nil, 8)
	if err != ErrExpandedInputTooLarge {
		t.Fatalf("err=%v, want ErrExpandedInputTooLarge", err)
	}
}

// Hashing an empty audit ID for every ID-less event would make unrelated
// requests collide at the repository unique key.
func TestParseFileUsesEventFingerprintWhenAuditIDIsMissing(t *testing.T) {
	path := writeAuditFile(t, "audit.log", strings.Join([]string{
		`{"stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:00:00Z","verb":"create","objectRef":{"apiVersion":"v1","resource":"pods","namespace":"default","name":"one"}}`,
		`{"stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:00:01Z","verb":"create","objectRef":{"apiVersion":"v1","resource":"pods","namespace":"default","name":"two"}}`,
	}, "\n")+"\n")
	var events []Event
	_, err := ParseFile(context.Background(), path, func(_ context.Context, event Event) error { events = append(events, event); return nil })
	if err != nil || len(events) != 2 || events[0].AuditIDHash == events[1].AuditIDHash || events[0].AuditIDHash == hashString("") {
		t.Fatalf("err=%v events=%+v", err, events)
	}
}

// Generating a key hash for an incomplete object identity would incorrectly
// elevate resource-only evidence to a high-confidence exact-object match.
func TestObjectKeyHashRequiresResourceAndObjectName(t *testing.T) {
	for _, input := range []struct {
		resource string
		name     string
	}{{resource: "", name: "pod"}, {resource: "pods", name: ""}} {
		hash, display := ObjectKeyHash("", input.resource, "default", input.name)
		if hash != "" || display != input.name {
			t.Fatalf("input=%+v hash=%q display=%q", input, hash, display)
		}
	}
}

// Dropping subresource, line number, safe unknown defaults, or parse status
// would make stored evidence ambiguous and weaken later filtering.
func TestParseFilePopulatesSafeAuditMetadata(t *testing.T) {
	path := writeAuditFile(t, "audit.log", `{"auditID":"metadata","stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:00:00Z","verb":"patch","sourceIPs":["not-an-ip"],"objectRef":{"apiVersion":"apps/v1","resource":"deployments","subresource":"status","namespace":"default","name":"api"}}`+"\n")
	var got Event
	_, err := ParseFile(context.Background(), path, func(_ context.Context, event Event) error { got = event; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if got.LineNumber != 1 || got.ParseStatus != "parsed" || got.Username != "unknown" || got.SourceNetwork != "unknown" ||
		got.APIGroup != "apps" || got.Subresource != "status" || got.ObjectName != "api" || got.ObjectKeyHash != hashString("/registry/deployments/default/api") {
		t.Fatalf("event=%+v", got)
	}
}

func writeAuditFile(t *testing.T, name, content string) string {
	t.Helper()
	return writeAuditBytes(t, name, []byte(content))
}

func writeAuditBytes(t *testing.T, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
