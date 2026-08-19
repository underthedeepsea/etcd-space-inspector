package worker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWorkerProtocolResultRoundTrip(t *testing.T) {
	taskDir := t.TempDir()
	want := Result{
		RunID:        "0123456789abcdef",
		Mode:         ModeAnalysis,
		Status:       "failed",
		ErrorCode:    "WORKER_PANIC",
		ErrorMessage: "analysis worker panicked",
		ExitCode:     1,
	}
	if err := WriteResult(taskDir, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadResult(taskDir, want.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunID != want.RunID || got.Mode != want.Mode || got.Status != want.Status || got.ErrorCode != want.ErrorCode || got.ErrorMessage != want.ErrorMessage || got.ExitCode != want.ExitCode {
		t.Fatalf("result=%+v, want=%+v", got, want)
	}
	if _, err := os.Stat(filepath.Join(taskDir, ResultFileName)); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerProtocolRejectsUnknownFieldsAndWrongRun(t *testing.T) {
	taskDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(taskDir, ResultFileName), []byte(`{"runId":"0123456789abcdef","mode":"analysis","status":"success","unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadResult(taskDir, "0123456789abcdef"); err == nil {
		t.Fatal("unknown result field was accepted")
	}
	if err := os.WriteFile(filepath.Join(taskDir, ResultFileName), []byte(`{"runId":"fedcba9876543210","mode":"analysis","status":"success"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadResult(taskDir, "0123456789abcdef"); err == nil {
		t.Fatal("wrong run result was accepted")
	}
}
