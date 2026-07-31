package diff

import (
	"testing"
	"time"
)

func TestServiceCreateValidatesObservationTimes(t *testing.T) {
	service := NewService(t.TempDir())
	baseline := time.Date(2026, 7, 31, 10, 0, 0, 0, time.UTC)
	target := baseline.Add(2 * time.Hour)

	created, err := service.Create(CreateRequest{
		Name: "timed", BaselineTaskID: "base", TargetTaskID: "target",
		BaselineObservedAt: &baseline, TargetObservedAt: &target,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.BaselineObservedAt == nil || created.TargetObservedAt == nil ||
		!created.BaselineObservedAt.Equal(baseline) || !created.TargetObservedAt.Equal(target) {
		t.Fatalf("created=%+v", created)
	}
	reloaded, err := service.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.BaselineObservedAt == nil || reloaded.TargetObservedAt == nil ||
		!reloaded.BaselineObservedAt.Equal(baseline) || !reloaded.TargetObservedAt.Equal(target) {
		t.Fatalf("reloaded=%+v", reloaded)
	}

	for _, request := range []CreateRequest{
		{Name: "without-times", BaselineTaskID: "base", TargetTaskID: "target"},
		{Name: "only-baseline", BaselineTaskID: "base", TargetTaskID: "target", BaselineObservedAt: &baseline},
		{Name: "equal", BaselineTaskID: "base", TargetTaskID: "target", BaselineObservedAt: &baseline, TargetObservedAt: &baseline},
		{Name: "sub-second", BaselineTaskID: "base", TargetTaskID: "target", BaselineObservedAt: &baseline, TargetObservedAt: timePointer(baseline.Add(time.Nanosecond))},
		{Name: "reverse", BaselineTaskID: "base", TargetTaskID: "target", BaselineObservedAt: &target, TargetObservedAt: &baseline},
	} {
		_, err := service.Create(request)
		if request.Name == "without-times" && err != nil {
			t.Fatalf("request=%s err=%v", request.Name, err)
		}
		if request.Name != "without-times" && err == nil {
			t.Fatalf("request=%s accepted invalid observation times", request.Name)
		}
	}
}

func timePointer(value time.Time) *time.Time { return &value }
