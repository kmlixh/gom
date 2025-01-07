package testutils

import (
	"database/sql"
	"fmt"
	"os"
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

// DefaultMySQLConfig 返回默认的MySQL测试配置
func DefaultMySQLConfig() TestDBConfig {
	return TestDBConfig{
		Driver:   "mysql",
		Host:     getEnvOrDefault("TEST_MYSQL_HOST", "10.0.1.5"),
		Port:     getEnvIntOrDefault("TEST_MYSQL_PORT", 3306),
		User:     getEnvOrDefault("TEST_MYSQL_USER", "root"),
		Password: getEnvOrDefault("TEST_MYSQL_PASSWORD", "123456"),
		DBName:   getEnvOrDefault("TEST_MYSQL_DB", "test"),
	}
}

// DefaultPostgresConfig 返回默认的PostgreSQL测试配置
func DefaultPostgresConfig() TestDBConfig {
	return TestDBConfig{
		Driver:   "postgres",
		Host:     getEnvOrDefault("TEST_PG_HOST", "127.0.0.1"),
		Port:     getEnvIntOrDefault("TEST_PG_PORT", 5432),
		User:     getEnvOrDefault("TEST_PG_USER", "postgres"),
		Password: getEnvOrDefault("TEST_PG_PASSWORD", ""),
		DBName:   getEnvOrDefault("TEST_PG_DB", "test"),
	}
}

// DSN 返回数据库连接字符串
func (c TestDBConfig) DSN() string {
	switch c.Driver {
	case "mysql":
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
			c.User, c.Password, c.Host, c.Port, c.DBName)
	case "postgres":
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			c.Host, c.Port, c.User, c.Password, c.DBName)
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

// getEnvIntOrDefault 获取整数类型的环境变量，如果不存在则返回默认值
func getEnvIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var result int
		if _, err := fmt.Sscanf(value, "%d", &result); err == nil {
			return result
		}
	}
	return defaultValue
}
