package gom

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// splitIntoBatches splits a slice into batches of the specified size
func splitIntoBatches[T any](items []T, batchSize int) [][]T {
	if len(items) == 0 {
		return nil
	}

	batches := make([][]T, 0, (len(items)+batchSize-1)/batchSize)

	for batchSize < len(items) {
		items, batches = items[batchSize:], append(batches, items[0:batchSize:batchSize])
	}
	batches = append(batches, items)

	return batches
}

// processBatchesWithTimeout processes batches concurrently with timeout control
func processBatchesWithTimeout[T any](
	ctx context.Context,
	items []T,
	batchSize int,
	concurrency int,
	timeout time.Duration,
	processor func(context.Context, []T) error,
) error {
	if len(items) == 0 {
		return nil
	}

	// Create timeout context
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Split items into batches
	batches := splitIntoBatches(items, batchSize)

	// Create error channel and wait group
	errChan := make(chan error, concurrency)
	var wg sync.WaitGroup

	// Create semaphore for concurrency control
	sem := make(chan struct{}, concurrency)

	// Process batches
	for _, batch := range batches {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
			wg.Add(1)
			go func(batch []T) {
				defer func() {
					<-sem
					wg.Done()
				}()

				if err := processor(ctx, batch); err != nil {
					select {
					case errChan <- err:
					default:
					}
				}
			}(batch)
		}
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// retryWithBackoff retries an operation with exponential backoff
func retryWithBackoff(
	ctx context.Context,
	retryCount int,
	retryInterval time.Duration,
	operation func() error,
) error {
	var lastErr error

	for i := 0; i < retryCount; i++ {
		if err := operation(); err == nil {
			return nil
		} else {
			lastErr = err

			// Calculate backoff duration
			backoff := retryInterval * time.Duration(1<<uint(i))

			// Wait with context cancellation support
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return lastErr
}

// trackProgress tracks progress of batch operations
type progressTracker struct {
	total     int64
	processed int64
}

func newProgressTracker(total int64) *progressTracker {
	return &progressTracker{
		total:     total,
		processed: 0,
	}
}

func (p *progressTracker) increment(count int64) {
	atomic.AddInt64(&p.processed, count)
}

func (p *progressTracker) getProgress() (int64, float64) {
	processed := atomic.LoadInt64(&p.processed)
	percentage := float64(processed) / float64(p.total) * 100
	return processed, percentage
}
