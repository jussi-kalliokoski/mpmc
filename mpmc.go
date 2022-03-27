// Package mpmc provides abstractions for implementing communications where messages are broadcasted to multiple consumers, possibly produced by multiple producers.
package mpmc

import (
	"context"
	"sync"
)

// MPMC is a communication primitive for messages with Multiple Producers and Multiple Consumers. The zero value for an MPMC is ready for publishing and subscribing.
//
// An MPMC must not be copied after first use.
//
// Similar to locks, an MPMC is a duplex intended to be used as an implementation detail and for most use cases the methods should not be as is as a part of an API whose implementation uses MPMC.
//
// Recommended usage is that the publisher(s) own the MPMC and provide a fitting abstraction for subscriptions. For use cases with multiple publishers, it might even be preferable to just provide the Publish method to the publishers, and have the coordinator own the MPMC.
type MPMC[T any] struct {
	mu            sync.RWMutex
	subscriptions []*subscription[T]
}

type subscription[T any] struct {
	consume func(context.Context, T) error
}

// Publish calls all active subscribers with the provided ctx and msg.
//
// If a subscriber returns an error, Publish will not call more subscribers, but instead returns the error from the subscriber.
func (mpmc *MPMC[T]) Publish(ctx context.Context, msg T) error {
	mpmc.mu.RLock()
	defer mpmc.mu.RUnlock()
	for _, s := range mpmc.subscriptions {
		if err := s.consume(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// Subscribe adds the subscriber to the list of functions to be called when a message is published.
//
// The created subscription will eventually be removed after the provided ctx is canceled.
//
// The subscriber MUST gracefully handle incoming messages without deadlocking even if the ctx provided to Subscribe has already been canceled.
func (mpmc *MPMC[T]) Subscribe(ctx context.Context, subscriber func(context.Context, T) error) {
	mpmc.subscribe(ctx, subscriber, noop)
}

// ConsumeAndClose adds a subscriber that pushes to the published messages to consumerCh.
//
// The created subscription will eventually be removed after the provided ctx is canceled.
//
// The provided consumerCh will be closed once the subscription has been removed.
func (mpmc *MPMC[T]) ConsumeAndClose(ctx context.Context, consumerCh chan<- T) {
	mpmc.consume(ctx, consumerCh, func() {
		close(consumerCh)
	})
}

// Consume behaves like ConsumeAndClose except the consumerCh will not be closed.
//
// This allows for patterns like piping multiple MPMCs to the same consumerCh.
func (mpmc *MPMC[T]) Consume(ctx context.Context, consumerCh chan<- T) {
	mpmc.consume(ctx, consumerCh, noop)
}

func (mpmc *MPMC[T]) subscribe(ctx context.Context, subscriber func(context.Context, T) error, cleanup func()) {
	s := mpmc.addSubscriber(subscriber)
	if ctx.Done() != nil {
		go func() {
			defer cleanup()
			<-ctx.Done()
			mpmc.mu.Lock()
			defer mpmc.mu.Unlock()
			mpmc.subscriptions = remove(mpmc.subscriptions, s)
		}()
	}
}

func (mpmc *MPMC[T]) addSubscriber(subscriber func(context.Context, T) error) *subscription[T] {
	s := &subscription[T]{subscriber}
	mpmc.mu.Lock()
	defer mpmc.mu.Unlock()
	mpmc.subscriptions = append(mpmc.subscriptions, s)
	return s
}

func (mpmc *MPMC[T]) consume(ctx context.Context, consumerCh chan<- T, cleanup func()) {
	mpmc.subscribe(ctx, func(mctx context.Context, msg T) error {
		// prevent randomly writing to channel if contexts have been
		// canceled already and the channel is ready to be written to
		select {
		case <-mctx.Done():
			return mctx.Err()
		case <-ctx.Done():
			// subscription is no longer listening
			return nil
		default:
		}
		select {
		case <-mctx.Done():
			return mctx.Err()
		case <-ctx.Done():
			// subscription is no longer listening
			return nil
		case consumerCh <- msg:
			return nil
		}
	}, cleanup)
}

func remove[T comparable](haystack []T, needle T) []T {
	var empty T
	for i := range haystack {
		if haystack[i] == needle {
			copy(haystack[i:], haystack[i+1:])
			haystack[len(haystack)-1] = empty
			return haystack[:len(haystack)-1]
		}
	}
	return haystack
}

func noop() {}
