package report

import (
	"bytes"
	"context"
	"strings"
	"testing"

	backend "etcd-analyzer/internal/backend/bbolt"
	"etcd-analyzer/internal/mvcc"
	"etcd-analyzer/internal/task"
)

func TestWriteHTMLEscapesKeysAndOmitsValue(t *testing.T) {
	var output bytes.Buffer
	err := WriteHTML(context.Background(), &output, Summary{
		Task:              task.Task{Name: "<script>alert(1)</script>", SourceSHA256: "abc123"},
		Physical:          backend.Summary{PhysicalFileSize: 4096, FreePageBytes: 1024},
		MVCC:              mvcc.Summary{SemanticAvailable: true, CurrentKeyCount: 1},
		TopHistoricalKeys: []mvcc.KeyRecord{{KeyText: "<script>alert(2)</script>", CurrentStoredBytes: 20}},
		TopPrefixes:       []mvcc.PrefixStat{{Prefix: "/safe", HistoricalBytes: 10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	html := output.String()
	if strings.Contains(html, "<script>") || !strings.Contains(html, "&lt;script&gt;") {
		t.Fatalf("unsafe report: %s", html)
	}
	if strings.Contains(strings.ToLower(html), "javascript") || strings.Contains(html, "super-secret-value") {
		t.Fatalf("report contains executable or raw value content: %s", html)
	}
	for _, expected := range []string{"ETCD DBSize Analyzer", "abc123", "/safe", "MVCC"} {
		if !strings.Contains(html, expected) {
			t.Fatalf("report missing %q", expected)
		}
	}
}

func TestWriteHTMLHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WriteHTML(ctx, &bytes.Buffer{}, Summary{}); err == nil {
		t.Fatal("expected cancellation error")
	}
}
