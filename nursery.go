/*
Package nursery provides support for structured concurrency
on top of the golang.org/x/sync/errgroup package.
See https://en.wikipedia.org/wiki/Structured_concurrency
*/
package nursery

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

// Func is the type of functions that run a nursery context.
type Func func(context.Context) error

type contextKey struct{}

// Context creates a new nursery context as a sub-context of ctx.
// The caller of NewContext is responsible for ensuring
// that Wait is eventually called to handle cleanup and errors.
func NewContext(ctx context.Context) context.Context {
	g, ctx := errgroup.WithContext(ctx)
	return context.WithValue(ctx, contextKey{}, g)
}

// Wait waits for the functions launched in the given nursery context
// and returns the first non-nil error from any one of them.
// If called with a non-nursery context, it returns nil immediately.
func Wait(ctx context.Context) error {
	g, ok := ctx.Value(contextKey{}).(*errgroup.Group)
	if !ok {
		return nil
	}
	return g.Wait()
}

// Go launches each of the given Funcs in parallel,
// and returns without waiting for any function to complete.
// If ctx is not a nursery context, Go will panic.
func Go(ctx context.Context, fns ...Func) {
	g, ok := ctx.Value(contextKey{}).(*errgroup.Group)
	if !ok {
		panic("nursery.Go called with a non-nursery context")
	}
	for _, fn := range fns {
		fn := fn // Shadow fn so it isn't overwritten on next iteration.
		g.Go(func() error {
			return fn(ctx)
		})
	}
}

// N returns a Func that launches N parallel copies of fn
// and waits for them all to complete before returning.
func N(n int, fn Func) Func {
	return func(ctx context.Context) error {
		ctx = NewContext(ctx)
		for i := 0; i < n; i++ {
			Go(ctx, fn)
		}
		return Wait(ctx)
	}
}

// Sequence returns a Func that runs each of the given functions sequentially.
func Sequence(fns ...Func) Func {
	return func(ctx context.Context) error {
		for i := range fns {
			err := fns[i](ctx)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// A Limit limits parallelism among a set of Goroutines.
// The limit is a fixed-size token bucket where
// each Goroutine running within the limit must hold a token.
type Limit struct {
	sem chan struct{}
}

// NewLimit returns a new Limit with the given max parallelism.
func NewLimit(max int) *Limit {
	l := &Limit{
		sem: make(chan struct{}, max),
	}
	for i := 0; i < max; i++ {
		l.sem <- struct{}{}
	}
	return l
}

// RampUp drains n tokens from l, then adds them back over time.
// The given context must be a nursery context.
// The function returns after draining,
// ramping up in another goroutine within the nursery.
// The duration is the time to wait before putting each drained token back in the bucket.
// The ramp-up may be canceled early by calling cancel.
func (l *Limit) RampUp(ctx context.Context, n int, d time.Duration) (cancel func()) {
	if n < 1 {
		return func() {}
	}
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return func() {}
		case <-l.sem:
		}
	}

	// In this function, don't return an error from context cancelation.
	// Otherwise, canceling the ramp will return an error
	// and make the whole nursery go into cancelation.
	ctx, cancel = context.WithCancel(ctx)
	Go(ctx, func(ctx context.Context) error {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for i := 0; i < n; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			select {
			case <-ctx.Done():
				return nil
			case l.sem <- struct{}{}:
			}
		}
		return nil
	})
	return cancel
}

// Go launches each of the given Funcs in parallel,
// but it will not launch a function if the maximum
// number of parallel functions for l are already running.
// Only functions launched directly by l.Go
// count toward the parallelism limit.
// Go returns only after all functions have been launched.
//
// If ctx is not a nursery context, Go will panic.
func (l *Limit) Go(ctx context.Context, fns ...Func) {
	for _, fn := range fns {
		fn := fn // shadow for goroutine
		select {
		case <-ctx.Done():
			return
		case <-l.sem:
		}
		Go(ctx, func(ctx context.Context) error {
			err := fn(ctx)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case l.sem <- struct{}{}:
				return err
			}
		})
	}
}
