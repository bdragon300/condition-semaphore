// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package conditional_semaphore_test

import (
	"context"
	"errors"
	condsema "github.com/bdragon300/conditional_semaphore"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

const maxSleep = 1 * time.Millisecond

func HammerWeighted(sem *condsema.Weighted, n int64, loops int) {
	for i := 0; i < loops; i++ {
		sem.Acquire(context.Background(), n)
		time.Sleep(time.Duration(rand.Int63n(int64(maxSleep/time.Nanosecond))) * time.Nanosecond)
		sem.Release(n)
	}
}

func TestWeighted(t *testing.T) {
	t.Parallel()

	n := runtime.GOMAXPROCS(0)
	loops := 10000 / n
	sem := condsema.NewWeighted(int64(n))
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			HammerWeighted(sem, int64(i), loops)
		}()
	}
	wg.Wait()	
}

func TestWeightedPanic(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("release of an unacquired weighted semaphore did not panic")
		}
	}()
	w := condsema.NewWeighted(1)
	w.Release(1)
}

func TestWeightedTryAcquire(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sem := condsema.NewWeighted(2)
	tries := []bool{}
	sem.Acquire(ctx, 1)
	tries = append(tries, sem.TryAcquire(1))
	tries = append(tries, sem.TryAcquire(1))

	sem.Release(2)

	tries = append(tries, sem.TryAcquire(1))
	sem.Acquire(ctx, 1)
	tries = append(tries, sem.TryAcquire(1))

	want := []bool{true, false, true, false}
	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d]: got %t, want %t", i, tries[i], want[i])
		}
	}
}

func TestWeightedAcquire(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sem := condsema.NewWeighted(2)
	tryAcquire := func(n int64) bool {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		return sem.Acquire(ctx, n) == nil
	}

	tries := []bool{}
	sem.Acquire(ctx, 1)
	tries = append(tries, tryAcquire(1))
	tries = append(tries, tryAcquire(1))

	sem.Release(2)

	tries = append(tries, tryAcquire(1))
	sem.Acquire(ctx, 1)
	tries = append(tries, tryAcquire(1))

	want := []bool{true, false, true, false}
	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d]: got %t, want %t", i, tries[i], want[i])
		}
	}
}

func TestWeightedDoesntBlockIfTooBig(t *testing.T) {
	t.Parallel()

	const n = 2
	sem := condsema.NewWeighted(n)
	{
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go sem.Acquire(ctx, n+1)
	}

	g, ctx := errgroup.WithContext(context.Background())
	for i := n * 3; i > 0; i-- {
		g.Go(func() error {
			err := sem.Acquire(ctx, 1)
			if err == nil {
				time.Sleep(1 * time.Millisecond)
				sem.Release(1)
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		t.Errorf("semaphore.NewWeighted(%v) failed to AcquireCtx(_, 1) with AcquireCtx(_, %v) pending", n, n+1)
	}
}

// TestLargeAcquireDoesntStarve times out if a large call to Acquire starves.
// Merely returning from the test function indicates success.
func TestLargeAcquireDoesntStarve(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	n := int64(runtime.GOMAXPROCS(0))
	sem := condsema.NewWeighted(n)
	running := true

	var wg sync.WaitGroup
	wg.Add(int(n))
	for i := n; i > 0; i-- {
		sem.Acquire(ctx, 1)
		go func() {
			defer func() {
				sem.Release(1)
				wg.Done()
			}()
			for running {
				time.Sleep(1 * time.Millisecond)
				sem.Release(1)
				sem.Acquire(ctx, 1)
			}
		}()
	}

	sem.Acquire(ctx, n)
	running = false
	sem.Release(n)
	wg.Wait()
}

// translated from https://github.com/zhiqiangxu/util/blob/master/mutex/crwmutex_test.go#L43
func TestAllocCancelDoesntStarve(t *testing.T) {
	sem := condsema.NewWeighted(10)

	// Block off a portion of the semaphore so that Acquire(_, 10) can eventually succeed.
	sem.Acquire(context.Background(), 1)

	// In the background, Acquire(_, 10).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sem.Acquire(ctx, 10)
	}()

	// Wait until the Acquire(_, 10) call blocks.
	for sem.TryAcquire(1) {
		sem.Release(1)
		runtime.Gosched()
	}

	// Now try to grab a read lock, and simultaneously unblock the Acquire(_, 10) call.
	// Both Acquire calls should unblock and return, in either order.
	go cancel()

	err := sem.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("Acquire(_, 1) failed unexpectedly: %v", err)
	}
	sem.Release(1)
}

func TestAwaitMoreOrEqual_ConditionTriggering(t *testing.T) {
	t.Parallel()
	const n = 5
	const count = 100
	sem := condsema.NewWeighted(n)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan int64)
	for i := 0; i < count; i++ {
		item := rand.Int63n(n + 1)  // [0, n+1)
		go func() {
			if err := sem.AwaitMoreOrEqual(ctx, item); err != nil {
				t.Fatalf("Error while awaiting for condition: %v", err)
			} else {
				ch <- item
			}
		}()
		runtime.Gosched()
	}

	// Check condition triggering on equal
	triggered := 0
	for i := 0; i <= n; i++ {
		carryOn := true
		for carryOn {
			select {
			case item := <-ch:
				if int(item) != i {
					t.Fatalf("Goroutine x>=%d triggered unexpectedly when x is %d", item, i)
				}
				triggered++
			case <-time.After(10 * time.Millisecond):
				carryOn = false // No more goroutines with x>=i
			}
		}
		if i < n {
			sem.Acquire(context.Background(), 1)
		}
	}
	if triggered != count {
		t.Fatalf("Triggered goroutines count is not equal to created count, %d != %d", triggered, count)
	}
	sem.Release(n)
}

func TestAwaitMoreOrEqual_ShouldAbortOnContextCancel(t *testing.T) {
	t.Parallel()
	const n = 5
	const count = 100
	sem := condsema.NewWeighted(n)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, count)
	for i := 0; i < count; i++ {
		item := rand.Int63n(n - 1) + 2 // [2, n+1)
		go func() {
			var err error
			if err = sem.AwaitMoreOrEqual(ctx, item); err == nil {
				t.Fatal("Semaphore was unexpectedly unblocked instead of return an error")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Unexpected error has been returned: %v", err)
			}
			ch <- err
		}()
		runtime.Gosched()
	}

	cancel()
	time.Sleep(10 * time.Millisecond)

	// Check condition triggering on equal
	if len(ch) != count {
		t.Fatalf("Canceled goroutines count is not equal to created count, %d != %d", len(ch), count)
	}
}

func TestAwaitLessOrEqual_ConditionTriggering(t *testing.T) {
	t.Parallel()
	const n = 5
	const count = 100
	sem := condsema.NewWeighted(n)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan int64)
	sem.Acquire(context.Background(), n)

	for i := 0; i < count; i++ {
		item := rand.Int63n(n + 1)  // [0, n+1)
		go func() {
			if err := sem.AwaitLessOrEqual(ctx, item); err != nil {
				t.Fatalf("Error while awaiting for condition: %v", err)
			} else {
				ch <- item
			}
		}()
		runtime.Gosched()
	}

	// Check condition triggering on equal
	triggered := 0
	for i := n; i >= 0; i-- {
		carryOn := true
		for carryOn {
			select {
			case item := <-ch:
				if int(item) != i {
					t.Fatalf("Goroutine x>=%d triggered unexpectedly when x is %d", item, i)
				}
				triggered++
			case <-time.After(10 * time.Millisecond):
				carryOn = false  // No more goroutines with x<=i
			}
		}
		if i > 0 {
			sem.Release(1)
		}
	}
	if triggered != count {
		t.Fatalf("Triggered goroutines count is not equal to created count, %d != %d", triggered, count)
	}
}

func TestAwaitLessOrEqual_ShouldAbortOnContextCancel(t *testing.T) {
	t.Parallel()
	const n = 5
	const count = 100
	sem := condsema.NewWeighted(n)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, count)
	sem.Acquire(context.Background(), n)
	for i := 0; i < count; i++ {
		item := rand.Int63n(n) // [0, n)
		go func() {
			var err error
			if err = sem.AwaitLessOrEqual(ctx, item); err == nil {
				t.Fatal("Semaphore was unexpectedly unblocked instead of return an error")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Unexpected error has been returned: %v", err)
			}
			ch <- err
		}()
		runtime.Gosched()
	}

	cancel()
	time.Sleep(10 * time.Millisecond)

	if len(ch) != count {
		t.Fatalf("Canceled goroutines count is not equal to created count, %d != %d", len(ch), count)
	}
}

func TestAwaitMoreOrEqual_ShouldReturnErrorIfAwaitMoreThanSize(t *testing.T) {
	t.Parallel()
	const n = 5
	sem := condsema.NewWeighted(n)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := sem.AwaitMoreOrEqual(ctx, n + 1); err == nil {
			t.Fatal("Semaphore was unexpectedly unblocked instead of return an error")
		}
	}()
	runtime.Gosched()
	cancel()
	runtime.Gosched()
}
