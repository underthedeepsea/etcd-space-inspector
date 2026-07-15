package apperr

import (
	"errors"
	"testing"
)

func TestErrorKeepsSafeMessageAndCause(t *testing.T) {
	cause := errors.New("disk detail")
	err := E("STORAGE_WRITE_FAILED", "unable to store task", cause)
	if err.Error() != "unable to store task" {
		t.Fatalf("message=%q", err.Error())
	}
	if !errors.Is(err, cause) {
		t.Fatal("cause was not preserved")
	}
	var coded *Error
	if !errors.As(err, &coded) || coded.Code != "STORAGE_WRITE_FAILED" {
		t.Fatalf("coded=%+v", coded)
	}
}
