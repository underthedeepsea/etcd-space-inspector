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
