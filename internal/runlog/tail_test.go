package runlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestTailReturnsLastLinesWithinByteLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.log")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if _, err := fmt.Fprintf(file, "line-%03d\n", i); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	lines, err := Tail(path, 200, 256<<10)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 200 || lines[0] != "line-100" || lines[199] != "line-299" {
		t.Fatalf("tail=%d lines, first=%q last=%q", len(lines), lines[0], lines[len(lines)-1])
	}
}
