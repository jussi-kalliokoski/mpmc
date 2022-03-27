package mpmc_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jussi-kalliokoski/mpmc"
)

func TestMPMC(t *testing.T) {
	t.Run("Subscribe", func(t *testing.T) {
		t.Run("Publish cancel", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx, scancel := context.WithCancel(context.Background())
			defer scancel()
			var events mpmc.MPMC[int]
			var wg sync.WaitGroup
			wg.Add(1)
			events.Subscribe(sctx, func(mctx context.Context, msg int) error {
				defer wg.Done()
				<-mctx.Done()
				return mctx.Err()
			})

			// Act
			errCh := awaitErr(func() error { return events.Publish(pctx, 123) })
			pcancel()
			wg.Wait()
			err := <-errCh

			// Assert
			assertErrorIs(t, context.Canceled, err)
		})

		t.Run("cancel subscription", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx, scancel := context.WithCancel(context.Background())
			defer scancel()
			var events mpmc.MPMC[int]
			callCh := make(chan int, 1)
			events.Subscribe(sctx, func(mctx context.Context, msg int) error {
				callCh <- msg
				return nil
			})

			// Act
			assertNoError(t, events.Publish(pctx, 123))
			received := <-callCh
			scancel()
			for {
				assertNoError(t, events.Publish(pctx, 234))
				select {
				case <-callCh:
					continue
				default:
				}
				break
			}

			// Assert
			assertEquals(t, 123, received)
		})
	})

	t.Run("Consume", func(t *testing.T) {
		t.Run("cancel consumption", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx1, scancel1 := context.WithCancel(context.Background())
			defer scancel1()
			sctx2, scancel2 := context.WithCancel(context.Background())
			defer scancel2()
			var events1 mpmc.MPMC[int]
			var events2 mpmc.MPMC[int]
			msgsCh := make(chan int, 2)
			events1.Consume(sctx1, msgsCh)
			events2.Consume(sctx2, msgsCh)

			// Act
			assertNoError(t, events1.Publish(pctx, 123))
			assertNoError(t, events2.Publish(pctx, 234))
			v1, v2 := minMax(<-msgsCh, <-msgsCh)
			scancel2()
			assertNoError(t, events1.Publish(pctx, 345))
			assertNoError(t, events2.Publish(pctx, 456))
			v3 := <-msgsCh
			select {
			case <-msgsCh:
				t.Fatal("expected channel not to be published to after cancel")
			default:
			}

			// Assert
			assertEquals(t, v1, 123)
			assertEquals(t, v2, 234)
			assertEquals(t, v3, 345)
		})

		t.Run("cancel consumption async", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx, scancel := context.WithCancel(context.Background())
			defer scancel()
			var events mpmc.MPMC[int]
			waitCh := make(chan struct{})
			events.Subscribe(sctx, func(context.Context, int) error {
				close(waitCh)
				return nil
			})
			msgsCh := make(chan int)
			events.Consume(sctx, msgsCh)

			// Act
			errCh := awaitErr(func() error { return events.Publish(pctx, 123) })
			<-waitCh
			scancel()
			err := <-errCh
			select {
			case <-msgsCh:
				t.Fatal("expected channel not to be published to after message was canceled")
			default:
			}

			// Assert
			assertNoError(t, err)
		})

		t.Run("cancel publish", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx, scancel := context.WithCancel(context.Background())
			defer scancel()
			var events mpmc.MPMC[int]
			msgsCh := make(chan int, 1)
			events.Consume(sctx, msgsCh)

			// Act
			pcancel()
			err := events.Publish(pctx, 123)
			select {
			case <-msgsCh:
				t.Fatal("expected channel not to be published to after message was canceled")
			default:
			}

			// Assert
			assertErrorIs(t, context.Canceled, err)
		})

		t.Run("cancel publish async", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx, scancel := context.WithCancel(context.Background())
			defer scancel()
			var events mpmc.MPMC[int]
			waitCh := make(chan struct{})
			events.Subscribe(sctx, func(context.Context, int) error {
				close(waitCh)
				return nil
			})
			msgsCh := make(chan int)
			events.Consume(sctx, msgsCh)

			// Act
			errCh := awaitErr(func() error { return events.Publish(pctx, 123) })
			<-waitCh
			pcancel()
			err := <-errCh
			select {
			case <-msgsCh:
				t.Fatal("expected channel not to be published to after message was canceled")
			default:
			}

			// Assert
			assertErrorIs(t, context.Canceled, err)
		})
	})

	t.Run("ConsumeAndClose", func(t *testing.T) {
		t.Run("cancel consumption", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx, scancel := context.WithCancel(context.Background())
			defer scancel()
			var events1 mpmc.MPMC[int]
			var events2 mpmc.MPMC[int]
			msgsCh := make(chan int, 2)
			events1.ConsumeAndClose(sctx, msgsCh)

			// Act
			assertNoError(t, events1.Publish(pctx, 123))
			scancel()
			assertNoError(t, events2.Publish(pctx, 234))
			var v int
			for vv := range msgsCh {
				v = vv
			}

			// Assert
			assertEquals(t, v, 123)
		})

		t.Run("cancel consumption async", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx, scancel := context.WithCancel(context.Background())
			defer scancel()
			var events mpmc.MPMC[int]
			waitCh := make(chan struct{})
			events.Subscribe(sctx, func(context.Context, int) error {
				close(waitCh)
				return nil
			})
			msgsCh := make(chan int)
			events.ConsumeAndClose(sctx, msgsCh)

			// Act
			errCh := awaitErr(func() error { return events.Publish(pctx, 123) })
			<-waitCh
			scancel()
			err := <-errCh
			for range msgsCh {
				t.Fatal("expected channel not to be published to after message was canceled")
			}

			// Assert
			assertNoError(t, err)
		})

		t.Run("cancel publish", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx, scancel := context.WithCancel(context.Background())
			defer scancel()
			var events mpmc.MPMC[int]
			msgsCh := make(chan int, 1)
			events.ConsumeAndClose(sctx, msgsCh)

			// Act
			pcancel()
			err := events.Publish(pctx, 123)
			select {
			case <-msgsCh:
				t.Fatal("expected channel not to be published to after message was canceled")
			default:
			}

			// Assert
			assertErrorIs(t, context.Canceled, err)
		})

		t.Run("cancel publish async", func(t *testing.T) {
			// Arrange
			pctx, pcancel := context.WithCancel(context.Background())
			defer pcancel()
			sctx, scancel := context.WithCancel(context.Background())
			defer scancel()
			var events mpmc.MPMC[int]
			waitCh := make(chan struct{})
			events.Subscribe(sctx, func(context.Context, int) error {
				close(waitCh)
				return nil
			})
			msgsCh := make(chan int)
			events.ConsumeAndClose(sctx, msgsCh)

			// Act
			errCh := awaitErr(func() error { return events.Publish(pctx, 123) })
			<-waitCh
			pcancel()
			err := <-errCh
			select {
			case <-msgsCh:
				t.Fatal("expected channel not to be published to after message was canceled")
			default:
			}

			// Assert
			assertErrorIs(t, context.Canceled, err)
		})
	})
}

func ExampleMPMC() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subscribeCtx, subscribeCancel := context.WithCancel(ctx)
	defer subscribeCancel()
	var events mpmc.MPMC[string]
	events.Subscribe(subscribeCtx, func(mctx context.Context, msg string) error {
		// subscriptions must handle the cancelation of their own context,
		// as well as that of the publisher
		select {
		case <-mctx.Done():
			// the message publish context was canceled,
			// return cancellation to stop propagation
			return mctx.Err()
		case <-subscribeCtx.Done():
			// the subscription context was canceled,
			// return nil to allow the next subscriber
			// to be called
			return nil
		default:
			fmt.Println("msg:", msg)
			return nil
		}
	})
	if err := events.Publish(ctx, "abc"); err != nil {
		panic(err)
	}
	// Output: msg: abc
}

func ExampleMPMC_cancel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subscribeCtx, subscribeCancel := context.WithCancel(ctx)
	defer subscribeCancel()
	var events mpmc.MPMC[string]
	waitCancelCh := make(chan struct{})
	events.Subscribe(subscribeCtx, func(mctx context.Context, msg string) error {
		<-waitCancelCh
		// subscriptions must handle the cancelation of their own context,
		// as well as that of the publisher
		select {
		case <-mctx.Done():
			// the message publish context was canceled,
			// return cancellation to stop propagation
			return mctx.Err()
		case <-subscribeCtx.Done():
			// the subscription context was canceled,
			// return nil to allow the next subscriber
			// to be called
			fmt.Println("msg ignored")
			return nil
		default:
			fmt.Println("msg:", msg)
			return nil
		}
	})
	subscribeCancel()
	close(waitCancelCh)
	if err := events.Publish(ctx, "abc"); err != nil {
		panic(err)
	}
	// Output: msg ignored
}

func assertNoError(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatalf("expected no error, got `%#v`", err)
	}
}

func assertErrorIs(tb testing.TB, expected, received error) {
	tb.Helper()
	if expected != received {
		tb.Fatalf("expected `%#v`, got `%#v`", expected, received)
	}
}

func assertEquals[T comparable](tb testing.TB, expected, received T) {
	tb.Helper()
	if expected != received {
		tb.Fatalf("expected `%#v`, got `%#v`", expected, received)
	}
}

func awaitErr(fn func() error) <-chan error {
	ch := make(chan error)
	go func() {
		ch <- fn()
	}()
	return ch
}

func minMax[T ~int | ~int64 | ~uint | ~uint64](a, b T) (T, T) {
	if a > b {
		return b, a
	}
	return a, b
}
