package nursery

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNursery(t *testing.T) {
	err := Wait(context.Background())
	if err != nil {
		t.Error("Wait() error on non-nursery context:", err)
	}

	expected := errors.New("TEST")
	ctx := NewContext(context.Background())
	Go(ctx, func(ctx context.Context) error { return expected })
	err = Wait(ctx)
	if err != expected {
		t.Error("unexpected error:", err)
	}

	ch := make(chan int, 10)
	ctx = NewContext(context.Background())
	Go(ctx, Sequence(
		N(10, func(ctx context.Context) error {
			ch <- 1
			return nil
		}),
		N(10, func(ctx context.Context) error {
			<-ch
			return nil
		}),
	))
	err = Wait(ctx)
	if err != nil {
		t.Error("error from pipeline test:", err)
	}
}

func TestLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = NewContext(ctx)
	limited := NewLimit(10)

	// Test no ramp-up.
	cancelRamp := limited.RampUp(ctx, 0, time.Millisecond)
	cancelRamp()

	cancelRamp = limited.RampUp(ctx, 10, time.Millisecond)
	for i := 0; i < 100; i++ {
		limited.Go(ctx, func(ctx context.Context) error {
			return nil
		})
	}
	cancelRamp()

	// Test finished ramp-up
	limited = NewLimit(10)
	_ = limited.RampUp(ctx, 10, time.Millisecond)
	start := time.Now()
	for i := 0; i < 100; i++ {
		limited.Go(ctx, func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		})
	}
	if wait := time.Since(start); wait < 30*time.Millisecond {
		t.Error("not long enough:", wait)
	}
	cancel()

	// Test dead ramp-up.
	cancelRamp = limited.RampUp(ctx, 10, time.Millisecond)
	cancelRamp()

	for i := 0; i < 100; i++ {
		limited.Go(ctx, func(ctx context.Context) error {
			return nil
		})
	}

	err := Wait(ctx)
	if err != context.Canceled {
		t.Error("unexpected error:", err)
	}
}
