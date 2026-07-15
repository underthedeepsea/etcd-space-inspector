package task

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRunnerCancelsStage(t *testing.T) {
	entered := make(chan struct{})
	repo := newFakeRunnerRepository(Task{ID: "t1", Status: StatusPending, CreatedAt: time.Now().UTC()})
	runner := NewRunner(repo, []Stage{{Name: "wait", Run: func(ctx context.Context, tc *Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}}})
	done := make(chan error, 1)
	go func() { done <- runner.Start(context.Background(), "t1") }()
	<-entered
	if err := runner.Cancel("t1"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	got, _ := repo.GetTask(context.Background(), "t1")
	if got.Status != StatusCancelled || got.CompletedAt == nil {
		t.Fatalf("task=%+v", got)
	}
}

func TestRunnerCompletesEmptyPipeline(t *testing.T) {
	repo := newFakeRunnerRepository(Task{ID: "t1", Status: StatusPending, CreatedAt: time.Now().UTC()})
	if err := NewRunner(repo, nil).Start(context.Background(), "t1"); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.GetTask(context.Background(), "t1")
	if got.Status != StatusCompleted || got.Progress != 1 || got.CompletedAt == nil {
		t.Fatalf("task=%+v", got)
	}
}

type fakeRunnerRepository struct {
	mu   sync.Mutex
	task Task
}

func newFakeRunnerRepository(item Task) *fakeRunnerRepository {
	return &fakeRunnerRepository{task: item}
}

func (r *fakeRunnerRepository) GetTask(context.Context, string) (Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.task, nil
}

func (r *fakeRunnerRepository) UpdateTask(_ context.Context, item Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.task = item
	return nil
}

func (r *fakeRunnerRepository) SaveCheckpoint(context.Context, string, string, time.Time) error {
	return nil
}
