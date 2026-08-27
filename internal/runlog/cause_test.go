package runlog

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"etcd-analyzer/internal/apperr"
)

func TestSafeCausePreservesUnderlyingFilesystemErrorWithoutPath(t *testing.T) {
	err := &os.PathError{
		Op:   "open",
		Path: `C:\secret\customer\snapshot.db`,
		Err:  errors.New("sharing violation"),
	}

	got := SafeCause(err)

	if strings.Contains(got, `C:\secret\customer`) {
		t.Fatalf("SafeCause leaked path: %q", got)
	}
	if !strings.Contains(strings.ToLower(got), "open") || !strings.Contains(strings.ToLower(got), "sharing violation") {
		t.Fatalf("SafeCause=%q, want operation and underlying reason", got)
	}
}

func TestSafeCauseRemovesUnixAbsolutePath(t *testing.T) {
	got := SafeCause(fmt.Errorf("open /private/customer/snapshot.db: permission denied"))

	if strings.Contains(got, "/private/customer") {
		t.Fatalf("SafeCause leaked absolute path: %q", got)
	}
	if !strings.Contains(strings.ToLower(got), "permission denied") {
		t.Fatalf("SafeCause=%q, want underlying reason", got)
	}
}

func TestSafeCauseNormalizesLogControlsAndCapsLength(t *testing.T) {
	got := SafeCause(errors.New("open /private/customer/snapshot.db\npermission\tdenied\r" + strings.Repeat("x", 3000)))

	if strings.ContainsAny(got, "\r\n\t") {
		t.Fatalf("SafeCause kept log control characters: %q", got)
	}
	if strings.Contains(got, "/private/customer") {
		t.Fatalf("SafeCause leaked absolute path: %q", got)
	}
	if len(got) > 2048 {
		t.Fatalf("SafeCause length=%d, want <= 2048", len(got))
	}
}

func TestSafeCauseKeepsApplicationCodeAndSafeMessage(t *testing.T) {
	got := SafeCause(apperr.E("BBOLT_OPEN_FAILED", "unable to open database", errors.New("open /private/customer/snapshot.db: permission denied")))

	if got != "BBOLT_OPEN_FAILED unable to open database" {
		t.Fatalf("SafeCause=%q", got)
	}
}
