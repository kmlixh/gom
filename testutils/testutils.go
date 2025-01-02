package testutils

import (
	"os"
)

var (
	// DefaultMySQLDSN default MySQL test connection string
	DefaultMySQLDSN = "root:123456@tcp(192.168.110.249:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	// DefaultPgDSN default PostgreSQL test connection string
	DefaultPgDSN = "postgres://postgres:yzy123@192.168.110.249:5432/test?sslmode=disable"
)

// TestMySQLDSN current MySQL test connection string
var TestMySQLDSN = DefaultMySQLDSN

// TestPgDSN current PostgreSQL test connection string
var TestPgDSN = DefaultPgDSN

func init() {
	// Get DSN from environment variables, use default if not set
	envMySQLDSN := os.Getenv("TEST_MYSQL_DSN")
	if envMySQLDSN != "" {
		TestMySQLDSN = envMySQLDSN
	}

	envPgDSN := os.Getenv("TEST_PG_DSN")
	if envPgDSN != "" {
		TestPgDSN = envPgDSN
	}

	// Set timezone
	os.Setenv("TZ", "UTC")
}
