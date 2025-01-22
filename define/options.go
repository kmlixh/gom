package define

import "time"

// BatchOptions defines configuration options for batch operations
type BatchOptions struct {
	// BatchSize is the size of each batch
	BatchSize int

	// Concurrency is the number of concurrent goroutines for batch processing
	// If set to 0, defaults to 1 (no concurrency)
	Concurrency int

	// Timeout is the maximum duration for the entire batch operation
	// If set to 0, no timeout is applied
	Timeout time.Duration

	// RetryCount is the number of times to retry a failed batch
	// If set to 0, no retries are attempted
	RetryCount int

	// RetryInterval is the duration to wait between retries
	// If set to 0, defaults to 1 second
	RetryInterval time.Duration
}

// DefaultBatchOptions returns the default batch operation options
func DefaultBatchOptions() BatchOptions {
	return BatchOptions{
		BatchSize:     1000,
		Concurrency:   1,
		Timeout:       30 * time.Second,
		RetryCount:    3,
		RetryInterval: time.Second,
	}
}

// Validate validates the batch options and sets defaults if necessary
func (o *BatchOptions) Validate() error {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchOptions().BatchSize
	}
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultBatchOptions().Concurrency
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultBatchOptions().Timeout
	}
	if o.RetryCount < 0 {
		o.RetryCount = DefaultBatchOptions().RetryCount
	}
	if o.RetryInterval <= 0 {
		o.RetryInterval = DefaultBatchOptions().RetryInterval
	}
	return nil
}

// DBOptions defines database connection and pool configuration
type DBOptions struct {
	// MaxOpenConns is the maximum number of open connections to the database
	// If MaxOpenConns <= 0, then there is no limit on the number of open connections
	MaxOpenConns int

	// MaxIdleConns is the maximum number of connections in the idle connection pool
	// If MaxIdleConns <= 0, no idle connections are retained
	MaxIdleConns int

	// ConnMaxLifetime is the maximum amount of time a connection may be reused
	// If ConnMaxLifetime <= 0, connections are not closed due to their age
	ConnMaxLifetime time.Duration

	// ConnMaxIdleTime is the maximum amount of time a connection may be idle
	// If ConnMaxIdleTime <= 0, connections are not closed due to idle time
	ConnMaxIdleTime time.Duration

	// Debug enables debug logging of SQL queries
	Debug bool
}

// DefaultDBOptions returns the default database options
func DefaultDBOptions() DBOptions {
	return DBOptions{
		MaxOpenConns:    100,
		MaxIdleConns:    25,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 2 * time.Minute,
		Debug:           false,
	}
}

// Validate validates the database options and sets defaults if necessary
func (o *DBOptions) Validate() error {
	if o.MaxOpenConns < 0 {
		o.MaxOpenConns = DefaultDBOptions().MaxOpenConns
	}
	if o.MaxIdleConns < 0 {
		o.MaxIdleConns = DefaultDBOptions().MaxIdleConns
	}
	if o.ConnMaxLifetime < 0 {
		o.ConnMaxLifetime = DefaultDBOptions().ConnMaxLifetime
	}
	if o.ConnMaxIdleTime < 0 {
		o.ConnMaxIdleTime = DefaultDBOptions().ConnMaxIdleTime
	}
	return nil
}
