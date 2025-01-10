package testutils

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"
)

// TestDBConfig 测试数据库配置
type TestDBConfig struct {
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

var defaultHost = "192.168.110.249"

// DefaultMySQLConfig 返回默认的MySQL测试配置
func DefaultMySQLConfig() TestDBConfig {
	return TestDBConfig{
		Driver:   "mysql",
		Host:     getEnvOrDefault("TEST_MYSQL_HOST", defaultHost),
		Port:     getEnvIntOrDefault("TEST_MYSQL_PORT", 3306),
		User:     getEnvOrDefault("TEST_MYSQL_USER", "remote"),
		Password: getEnvOrDefault("TEST_MYSQL_PASSWORD", "123456"),
		DBName:   getEnvOrDefault("TEST_MYSQL_DB", "test"),
	}
}

// DefaultPostgresConfig 返回默认的PostgreSQL测试配置
func DefaultPostgresConfig() TestDBConfig {
	return TestDBConfig{
		Driver:   "postgres",
		Host:     getEnvOrDefault("TEST_PG_HOST", defaultHost),
		Port:     getEnvIntOrDefault("TEST_PG_PORT", 5432),
		User:     getEnvOrDefault("TEST_PG_USER", "postgres"),
		Password: getEnvOrDefault("TEST_PG_PASSWORD", "yzy123"),
		DBName:   getEnvOrDefault("TEST_PG_DB", "test"),
	}
}

// DSN 返回数据库连接字符串
func (c TestDBConfig) DSN() string {
	// First check if there's a direct DSN override from environment
	envKey := fmt.Sprintf("TEST_%s_DSN", c.Driver)
	if dsn := os.Getenv(envKey); dsn != "" {
		return dsn
	}

	switch c.Driver {
	case "mysql":
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True",
			c.User, c.Password, c.Host, c.Port, c.DBName)
	case "postgres":
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
			c.User, c.Password, c.Host, c.Port, c.DBName)
	default:
		return ""
	}
}

// SetupTestDB 设置测试数据库
func SetupTestDB(config TestDBConfig) (*sql.DB, error) {
	db, err := sql.Open(config.Driver, config.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	return db, nil
}

// CleanupTestDB 清理测试数据库
func CleanupTestDB(db *sql.DB, tables ...string) error {
	if db == nil {
		return nil
	}

	for _, table := range tables {
		_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %v", table, err)
		}
	}
	return nil
}

// getEnvOrDefault 获取环境变量，如果不存在则返回默认值
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvIntOrDefault 获取环境变量的整数值，如果不存在则返回默认值
func getEnvIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func init() {
	// Set timezone
	os.Setenv("TZ", "UTC")
}
