package testutils

import (
	"database/sql"
	"os"
)

var (
	// DefaultMySQLDSN 默认的 MySQL 测试连接字符串
	DefaultMySQLDSN = "root:123456@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	// DefaultPgDSN 默认的 PostgreSQL 测试连接字符串
	DefaultPgDSN = "postgres://postgres:123456@localhost:5432/test?sslmode=disable"

	// TestMySQLDSN 当前使用的 MySQL 测试连接字符串
	TestMySQLDSN string
	// TestPgDSN 当前使用的 PostgreSQL 测试连接字符串
	TestPgDSN string
)

func init() {
	// 从环境变量获取 DSN，如果未设置则使用默认值
	TestMySQLDSN = os.Getenv("TEST_MYSQL_DSN")
	if TestMySQLDSN == "" {
		TestMySQLDSN = DefaultMySQLDSN
	}

	TestPgDSN = os.Getenv("TEST_PG_DSN")
	if TestPgDSN == "" {
		TestPgDSN = DefaultPgDSN
	}

	// 设置时区
	os.Setenv("TZ", "UTC")
}

// CreateTestTable 创建测试表
func CreateTestTable(db *sql.DB, driver string) error {
	var createTableSQL string
	if driver == "mysql" {
		createTableSQL = `
			CREATE TABLE IF NOT EXISTS test_users (
				id BIGINT PRIMARY KEY AUTO_INCREMENT,
				username VARCHAR(255),
				email VARCHAR(255),
				age INT,
				active BOOLEAN,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
			)
		`
	} else if driver == "postgres" {
		createTableSQL = `
			CREATE TABLE IF NOT EXISTS test_users (
				id BIGSERIAL PRIMARY KEY,
				username VARCHAR(255),
				email VARCHAR(255),
				age INT,
				active BOOLEAN,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
			)
		`
	}

	_, err := db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	// 清理测试数据
	_, err = db.Exec("TRUNCATE TABLE test_users")
	return err
}
