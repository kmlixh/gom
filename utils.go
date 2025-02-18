package gom

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kmlixh/gom/v4/define"
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
	processor func(context.Context, []T) *define.Result,
) *define.Result {
	if len(items) == 0 {
		return nil
	}

	// Create timeout context
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Split items into batches
	batches := splitIntoBatches(items, batchSize)

	// Create error channel with buffer size 1 (only need first error)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	// Create semaphore with context awareness
	sem := make(chan struct{}, concurrency)

	// 初始化结果聚合
	var (
		totalResult = &define.Result{Error: nil}
		resultLock  sync.Mutex
	)

	// Process batches with context check
	for _, batch := range batches {
		// Check context before starting new batch
		if ctx.Err() != nil {
			return &define.Result{Error: ctx.Err()}
		}

		// Acquire semaphore with context awareness
		select {
		case <-ctx.Done():
			return &define.Result{Error: ctx.Err()}
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(batch []T) {
			defer func() {
				<-sem
				wg.Done()
			}()

			batchResult := processor(ctx, batch)

			resultLock.Lock()
			defer resultLock.Unlock()

			// 聚合结果
			if batchResult != nil {
				totalResult.Affected += batchResult.Affected
				if batchResult.Data != nil {
					// 处理不同类型的数据聚合
					totalResult.Data = append(totalResult.Data, batchResult.Data...)
				}
				// 保留第一个错误
				if batchResult.Error != nil && totalResult.Error == nil {
					totalResult.Error = batchResult.Error
				}
			}
		}(batch)
	}

	// Wait for all goroutines with context awareness
	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan: // Normal completion
	case <-ctx.Done(): // Timeout occurred
		return &define.Result{Error: ctx.Err()}
	}

	// Return first error or nil
	select {
	case err := <-errChan:
		return &define.Result{Error: err}
	default:
		return totalResult
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
