package testutils

import (
	"os"
)

var (
	// DefaultPgDSN default PostgreSQL test connection string
	DefaultPgDSN = "postgres://postgres:yzy123@192.168.110.249:5432/test?sslmode=disable"
	// DefaultMySQLDSN default MySQL test connection string
	DefaultMySQLDSN = "root:123456@tcp(10.0.1.5:3306)/test?charset=utf8mb4&parseTime=True"
)

// TestPgDSN current PostgreSQL test connection string
var TestPgDSN = DefaultPgDSN

// TestMySQLDSN current MySQL test connection string
var TestMySQLDSN = DefaultMySQLDSN

func init() {
	// Get DSN from environment variables, use default if not set
	envPgDSN := os.Getenv("TEST_PG_DSN")
	if envPgDSN != "" {
		TestPgDSN = envPgDSN
	}

	envMySQLDSN := os.Getenv("TEST_MYSQL_DSN")
	if envMySQLDSN != "" {
		TestMySQLDSN = envMySQLDSN
	}

	// Set timezone
	os.Setenv("TZ", "UTC")
}
