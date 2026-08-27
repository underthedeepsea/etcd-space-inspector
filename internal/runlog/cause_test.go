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

func TestSafeCauseRemovesSpacedPaths(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		leaks []string
		want  string
	}{
		{
			name:  "windows",
			err:   errors.New(`open C:\private customer\snapshot.db: permission denied`),
			leaks: []string{`C:\private customer\snapshot.db`, `customer\snapshot.db`},
			want:  "open [path] permission denied",
		},
		{
			name:  "unc",
			err:   errors.New(`open \\server\private customer\snapshot.db: permission denied`),
			leaks: []string{`\\server\private customer\snapshot.db`, `customer\snapshot.db`},
			want:  "open [path] permission denied",
		},
		{
			name:  "unix",
			err:   errors.New("open /private/customer data/snapshot.db: permission denied"),
			leaks: []string{"/private/customer data/snapshot.db", "data/snapshot.db"},
			want:  "open [path] permission denied",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := SafeCause(test.err)
			for _, leak := range test.leaks {
				if strings.Contains(got, leak) {
					t.Fatalf("SafeCause leaked path material %q: %q", leak, got)
				}
			}
			if !strings.Contains(got, "permission denied") {
				t.Fatalf("SafeCause=%q, want underlying reason", got)
			}
			if got != test.want {
				t.Fatalf("SafeCause=%q, want %q", got, test.want)
			}
		})
	}
}

func TestSafeCausePreservesOuterPathOperationBeforeApplicationError(t *testing.T) {
	err := &os.PathError{
		Op:   "open",
		Path: "/private/customer data/snapshot.db",
		Err:  apperr.E("BBOLT_OPEN_FAILED", "unable to open database", errors.New("private cause")),
	}

	if got := SafeCause(err); got != "open BBOLT_OPEN_FAILED unable to open database" {
		t.Fatalf("SafeCause=%q", got)
	}
}

func TestSafeCausePreservesOuterLinkOperationBeforeNestedPathError(t *testing.T) {
	err := &os.LinkError{
		Op:  "rename",
		Old: "/private/customer data/old.db",
		New: "/private/customer data/new.db",
		Err: &os.PathError{
			Op:   "open",
			Path: "/private/customer data/snapshot.db",
			Err:  errors.New("permission denied"),
		},
	}

	if got := SafeCause(err); got != "rename open permission denied" {
		t.Fatalf("SafeCause=%q", got)
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

func TestSafeCauseKeepsWrappedApplicationCodeAndSafeMessage(t *testing.T) {
	err := fmt.Errorf("worker operation: %w", apperr.E("BBOLT_OPEN_FAILED", "unable to open database", errors.New("private cause")))

	if got := SafeCause(err); got != "BBOLT_OPEN_FAILED unable to open database" {
		t.Fatalf("SafeCause=%q", got)
	}
}
