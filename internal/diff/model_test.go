package diff

import "testing"

func TestValidateTransition(t *testing.T) {
	tests := []struct {
		name    string
		from    Status
		to      Status
		wantErr bool
	}{
		{name: "pending to running", from: StatusPending, to: StatusRunning},
		{name: "pending to cancelled", from: StatusPending, to: StatusCancelled},
		{name: "running to completed", from: StatusRunning, to: StatusCompleted},
		{name: "running to failed", from: StatusRunning, to: StatusFailed},
		{name: "completed is terminal", from: StatusCompleted, to: StatusRunning, wantErr: true},
		{name: "cannot skip running", from: StatusPending, to: StatusCompleted, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateTransition(test.from, test.to)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateTransition(%q, %q) error=%v wantErr=%v", test.from, test.to, err, test.wantErr)
			}
		})
	}
}
