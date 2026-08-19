package task

import "testing"

func TestTransitionRejectsCompletedToRunning(t *testing.T) {
	if err := ValidateTransition(StatusCompleted, StatusRunning); err == nil {
		t.Fatal("expected error")
	}
	if err := ValidateTransition(StatusPending, StatusRunning); err != nil {
		t.Fatalf("valid transition rejected: %v", err)
	}
}

func TestM12TaskTransitions(t *testing.T) {
	tests := []struct {
		from, to Status
	}{
		{StatusImporting, StatusPending},
		{StatusImporting, StatusFailed},
		{StatusImporting, StatusCancelled},
		{StatusPending, StatusRunning},
		{StatusRunning, StatusCompleted},
	}
	for _, test := range tests {
		if err := ValidateTransition(test.from, test.to); err != nil {
			t.Fatalf("%s -> %s: %v", test.from, test.to, err)
		}
	}
}
