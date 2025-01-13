package define

import (
	"database/sql"
	"time"
)

// ConnectionConfig 数据库连接配置
type ConnectionConfig struct {
	MaxOpenConns    int           // 最大打开连接数
	MaxIdleConns    int           // 最大空闲连接数
	ConnMaxLifetime time.Duration // 连接最大生命周期
	ConnMaxIdleTime time.Duration // 空闲连接最大生命周期
}

// ConnectionPool 连接池接口
type ConnectionPool interface {
	GetConnection() (*sql.DB, error)
	ReturnConnection(*sql.DB)
	HealthCheck() error
	Close() error
}

// ReplicationConfig 主从配置
type ReplicationConfig struct {
	Master *ConnectionConfig
	Slaves []*ConnectionConfig
}

// RetryConfig 重试配置
type RetryConfig struct {
	MaxRetries int           // 最大重试次数
	RetryDelay time.Duration // 重试延迟
	MaxDelay   time.Duration // 最大延迟
}
