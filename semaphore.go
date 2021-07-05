// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Semaphore with awaiting methods ... TODO: write comment
 */
package condition_semaphore

import (
	"container/list"
	"context"
	"sync"
)

type waiter struct {
	n     int64
	ready chan<- struct{} // Closed when semaphore acquired.
}

type condWaiterKind int
const (
	moreOrEqual condWaiterKind = iota
	lessOrEqual
)

type condWaiter struct {
	condition int64
	ready     chan<- struct{}  // Closed when condition triggered
	kind condWaiterKind
}

func isCondWaiterTrue(kind condWaiterKind, cur, condition int64) bool {
	if kind == moreOrEqual {
		return cur >= condition
	}
	return cur <= condition
}

// NewWeighted creates a new weighted semaphore with the given
// maximum combined weight for concurrent access.
func NewWeighted(n int64) *Weighted {
	w := &Weighted{size: n}
	return w
}

// Weighted provides a way to bound concurrent access to a resource.
// The callers can request access with a given weight.
type Weighted struct {
	size        int64
	cur         int64
	mu          sync.Mutex
	waiters     list.List
	condWaiters list.List
}

// Acquire acquires the semaphore with a weight of n, blocking until resources
// are available or ctx is done. On success, returns nil. On failure, returns
// ctx.Err() and leaves the semaphore unchanged.
//
// If ctx is already done, Acquire may still succeed without blocking.
func (s *Weighted) Acquire(ctx context.Context, n int64) error {
	s.mu.Lock()
	if s.size-s.cur >= n && s.waiters.Len() == 0 {
		s.cur += n
		s.notifyCondWaiters(moreOrEqual)
		s.mu.Unlock()
		return nil
	}

	if n > s.size {
		// Don't make other Acquire calls block on one that's doomed to fail.
		s.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}

	ready := make(chan struct{})
	w := waiter{n: n, ready: ready}
	elem := s.waiters.PushBack(w)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		s.mu.Lock()
		select {
		case <-ready:
			// Acquired the semaphore after we were canceled.  Rather than trying to
			// fix up the queue, just pretend we didn't notice the cancelation.
			err = nil
		default:
			isFront := s.waiters.Front() == elem
			s.waiters.Remove(elem)
			// If we're at the front and there're extra tokens left, notify other waiters.
			if isFront && s.size > s.cur {
				s.notifyWaiters()
			}
		}
		s.mu.Unlock()
		return err

	case <-ready:
		s.notifyCondWaiters(moreOrEqual)
		return nil
	}
}

// TryAcquire acquires the semaphore with a weight of n without blocking.
// On success, returns true. On failure, returns false and leaves the semaphore unchanged.
func (s *Weighted) TryAcquire(n int64) bool {
	s.mu.Lock()
	success := s.size-s.cur >= n && s.waiters.Len() == 0
	if success {
		s.cur += n
		s.notifyCondWaiters(moreOrEqual)
	}
	s.mu.Unlock()
	return success
}

// Release releases the semaphore with a weight of n.
func (s *Weighted) Release(n int64) {
	s.mu.Lock()
	s.cur -= n
	if s.cur < 0 {
		s.mu.Unlock()
		panic("semaphore: released more than held")
	}
	s.notifyCondWaiters(lessOrEqual)  // Notify condWaiters before acquiring semaphore again by other waiters (if any)
	s.notifyWaiters()
	s.mu.Unlock()
}

func (s *Weighted) AwaitLessOrEqual(ctx context.Context, n int64) error {
	return s.conditionalAwait(ctx, n, lessOrEqual)
}

func (s *Weighted) AwaitMoreOrEqual(ctx context.Context, n int64) error {
	if n > s.size {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.conditionalAwait(ctx, n, moreOrEqual)
}

func (s *Weighted) notifyWaiters() {
	for {
		next := s.waiters.Front()
		if next == nil {
			break // No more waiters blocked.
		}

		w := next.Value.(waiter)
		if s.size-s.cur < w.n {
			// Not enough tokens for the next waiter.  We could keep going (to try to
			// find a waiter with a smaller request), but under load that could cause
			// starvation for large requests; instead, we leave all remaining waiters
			// blocked.
			//
			// Consider a semaphore used as a read-write lock, with N tokens, N
			// readers, and one writer.  Each reader can Acquire(1) to obtain a read
			// lock.  The writer can Acquire(N) to obtain a write lock, excluding all
			// of the readers.  If we allow the readers to jump ahead in the queue,
			// the writer will starve — there is always one token available for every
			// reader.
			break
		}

		s.cur += w.n
		s.waiters.Remove(next)
		close(w.ready)
	}
}


func (s *Weighted) conditionalAwait(ctx context.Context, n int64, condKind condWaiterKind) error {
	if n < 0 {
		panic("semaphore: cannot await for a negative n")
	}

	s.mu.Lock()
	if isCondWaiterTrue(condKind, s.cur, n) {
		// s.cur already meets to a condition
		s.mu.Unlock()
		return nil
	}
	ready := make(chan struct{})
	w := condWaiter{condition: n, ready: ready, kind: condKind}
	elem := s.condInsertSorted(&w)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		s.mu.Lock()
		select {
		case <-ready:
			// Condition has been triggered or already was true just after context cancellation.
			// Return as a condition was triggered normally.
			err = nil
		default:
			s.condWaiters.Remove(elem)
		}
		s.mu.Unlock()
		return err

	case <-ready:
		return nil
	}
}

func (s *Weighted) condInsertSorted(w *condWaiter) *list.Element {
	elem := s.condWaiters.Front()
	for elem != nil && elem.Value.(condWaiter).condition < w.condition {
		elem = elem.Next()
	}

	if elem != nil {
		return s.condWaiters.InsertBefore(*w, elem)
	}
	return s.condWaiters.PushBack(*w)
}

func (s *Weighted) notifyCondWaiters(kind condWaiterKind) {
	for {
		item := s.condWaiters.Front()
		if kind == lessOrEqual {
			item = s.condWaiters.Back()
		}
		if item == nil {
			break
		}

		w := item.Value.(condWaiter)
		if ! isCondWaiterTrue(kind, s.cur, w.condition) {
			break
		}

		s.condWaiters.Remove(item)
		close(w.ready)
	}
}
