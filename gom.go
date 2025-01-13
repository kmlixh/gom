package gom

import (
	"errors"
	"fmt"
	"time"

	"github.com/kmlixh/gom/v4/define"
)

// DBConfig 数据库配置
type DBConfig struct {
	DriverName      string        // 数据库驱动名称
	DSN             string        // 数据库连接字符串
	Debug           bool          // 是否开启调试模式
	MaxOpenConns    int           // 最大打开连接数
	MaxIdleConns    int           // 最大空闲连接数
	ConnMaxLifetime time.Duration // 连接最大生命周期
	ConnMaxIdleTime time.Duration // 空闲连接最大生命周期
	SlowThreshold   time.Duration // 慢查询阈值
}

// DefaultConfig 默认配置
func DefaultConfig(driverName, dsn string) *DBConfig {
	return &DBConfig{
		DriverName:      driverName,
		DSN:             dsn,
		Debug:           false,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Minute * 5,
		ConnMaxIdleTime: time.Minute * 3,
		SlowThreshold:   time.Second * 1,
	}
}

// OpenWithConfig 使用配置打开数据库连接
func OpenWithConfig(config *DBConfig) (*Chain, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	define.Debug = config.Debug
	factory, ok := define.GetFactory(config.DriverName)
	if !ok {
		return nil, fmt.Errorf("driver [%s] not found or factory not registered", config.DriverName)
	}

	db, err := factory.OpenDb(config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	// 测试连接
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Chain{
		id:      getGrouteId(),
		db:      db,
		factory: factory,
	}, nil
}

// Open 使用默认配置打开数据库连接
func Open(driverName string, dsn string, debug bool) (*Chain, error) {
	config := DefaultConfig(driverName, dsn)
	config.Debug = debug
	return OpenWithConfig(config)
}

// OpenWithOptions 使用选项打开数据库连接
func OpenWithOptions(driverName, dsn string, options ...func(*DBConfig)) (*Chain, error) {
	config := DefaultConfig(driverName, dsn)
	for _, option := range options {
		option(config)
	}
	return OpenWithConfig(config)
}

// 配置选项
func WithDebug(debug bool) func(*DBConfig) {
	return func(c *DBConfig) {
		c.Debug = debug
	}
}

func WithMaxOpenConns(n int) func(*DBConfig) {
	return func(c *DBConfig) {
		c.MaxOpenConns = n
	}
}

func WithMaxIdleConns(n int) func(*DBConfig) {
	return func(c *DBConfig) {
		c.MaxIdleConns = n
	}
}

func WithConnMaxLifetime(d time.Duration) func(*DBConfig) {
	return func(c *DBConfig) {
		c.ConnMaxLifetime = d
	}
}

func WithConnMaxIdleTime(d time.Duration) func(*DBConfig) {
	return func(c *DBConfig) {
		c.ConnMaxIdleTime = d
	}
}

func WithSlowThreshold(d time.Duration) func(*DBConfig) {
	return func(c *DBConfig) {
		c.SlowThreshold = d
	}
}
