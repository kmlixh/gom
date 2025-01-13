package db

import (
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/kmlixh/gom/v4/define"
)

type ConnectionPoolImpl struct {
	mu          sync.RWMutex
	config      *define.ConnectionConfig
	db          *sql.DB
	retryConfig *define.RetryConfig
}

func NewConnectionPool(config *define.ConnectionConfig, retryConfig *define.RetryConfig) *ConnectionPoolImpl {
	return &ConnectionPoolImpl{
		config:      config,
		retryConfig: retryConfig,
	}
}

func (p *ConnectionPoolImpl) GetConnection() (*sql.DB, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.db == nil {
		return nil, errors.New("connection pool not initialized")
	}

	if err := p.db.Ping(); err != nil {
		return nil, p.retryConnection(err)
	}

	return p.db, nil
}

func (p *ConnectionPoolImpl) ReturnConnection(db *sql.DB) {
	// 由于使用的是连接池，这里不需要真正地"归还"连接
	// 连接会自动由sql.DB管理
}

func (p *ConnectionPoolImpl) HealthCheck() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.db == nil {
		return errors.New("connection pool not initialized")
	}

	return p.db.Ping()
}

func (p *ConnectionPoolImpl) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db != nil {
		err := p.db.Close()
		p.db = nil
		return err
	}

	return nil
}

func (p *ConnectionPoolImpl) retryConnection(originalErr error) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error = originalErr
	for i := 0; i < p.retryConfig.MaxRetries; i++ {
		// 指数退避重试延迟
		delay := time.Duration(i+1) * p.retryConfig.RetryDelay
		if delay > p.retryConfig.MaxDelay {
			delay = p.retryConfig.MaxDelay
		}
		time.Sleep(delay)

		if err := p.db.Ping(); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}

	return lastErr
}

func (p *ConnectionPoolImpl) Initialize(db *sql.DB) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if db == nil {
		return errors.New("db connection cannot be nil")
	}

	// 配置连接池参数
	db.SetMaxOpenConns(p.config.MaxOpenConns)
	db.SetMaxIdleConns(p.config.MaxIdleConns)
	db.SetConnMaxLifetime(p.config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(p.config.ConnMaxIdleTime)

	p.db = db
	return p.HealthCheck()
}
