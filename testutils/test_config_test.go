package testutils

import (
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestDefaultConfigs(t *testing.T) {
	// 测试MySQL默认配置
	mysqlConfig := DefaultMySQLConfig()
	if mysqlConfig.Driver != "mysql" {
		t.Errorf("Expected MySQL driver, got %s", mysqlConfig.Driver)
	}
	if mysqlConfig.Port != 3306 {
		t.Errorf("Expected port 3306, got %d", mysqlConfig.Port)
	}

	// 测试PostgreSQL默认配置
	pgConfig := DefaultPostgresConfig()
	if pgConfig.Driver != "postgres" {
		t.Errorf("Expected PostgreSQL driver, got %s", pgConfig.Driver)
	}
	if pgConfig.Port != 5432 {
		t.Errorf("Expected port 5432, got %d", pgConfig.Port)
	}
}

func TestDSNGeneration(t *testing.T) {
	// Test MySQL DSN generation
	mysqlConfig := TestDBConfig{
		Driver:   "mysql",
		Host:     "localhost",
		Port:     3306,
		User:     "testuser",
		Password: "testpass",
		DBName:   "testdb",
	}

	expectedMySQLDSN := "testuser:testpass@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&multiStatements=true"
	if mysqlDSN := mysqlConfig.DSN(); mysqlDSN != expectedMySQLDSN {
		t.Errorf("Expected MySQL DSN %s, got %s", expectedMySQLDSN, mysqlDSN)
	}

	// Test PostgreSQL DSN generation
	pgConfig := TestDBConfig{
		Driver:   "postgres",
		Host:     "localhost",
		Port:     5432,
		User:     "testuser",
		Password: "testpass",
		DBName:   "testdb",
	}

	expectedPGDSN := "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable"
	if pgDSN := pgConfig.DSN(); pgDSN != expectedPGDSN {
		t.Errorf("Expected PostgreSQL DSN %s, got %s", expectedPGDSN, pgDSN)
	}
}

func TestEnvironmentVariables(t *testing.T) {
	// 测试环境变量覆盖
	os.Setenv("TEST_MYSQL_HOST", "testhost")
	os.Setenv("TEST_MYSQL_PORT", "3307")
	defer os.Unsetenv("TEST_MYSQL_HOST")
	defer os.Unsetenv("TEST_MYSQL_PORT")

	config := DefaultMySQLConfig()
	if config.Host != "testhost" {
		t.Errorf("Expected host testhost, got %s", config.Host)
	}
	if config.Port != 3307 {
		t.Errorf("Expected port 3307, got %d", config.Port)
	}

	// 测试无效的端口号
	os.Setenv("TEST_MYSQL_PORT", "invalid")
	config = DefaultMySQLConfig()
	if config.Port != 3306 {
		t.Errorf("Expected default port 3306 for invalid port value, got %d", config.Port)
	}
}

func TestDatabaseOperations(t *testing.T) {
	// 跳过实际的数据库操作，除非明确要求测试

	config := DefaultMySQLConfig()
	db, err := SetupTestDB(config)
	if err != nil {
		t.Fatalf("Failed to setup test database: %v", err)
	}
	defer db.Close()

	// 创建测试表
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// 测试清理
	err = CleanupTestDB(db, "test_table")
	if err != nil {
		t.Errorf("Failed to cleanup test database: %v", err)
	}
}
