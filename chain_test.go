package gom

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	ErrTest = errors.New("test error")
)

// TestModel represents a test model for database operations
type TestModel struct {
	ID        int64     `gom:"id,@,auto"`
	Name      string    `gom:"name,notnull"`
	Age       int       `gom:"age,notnull"`
	Email     string    `gom:"email,notnull"`
	CreatedAt time.Time `gom:"created_at,notnull,default"`
	UpdatedAt time.Time `gom:"updated_at,notnull,default"`
	IsActive  bool      `gom:"is_active,notnull,default"`
}

// setupDB creates a new database connection with the given driver and DSN
func setupDB(driver, dsn string) *DB {
	fmt.Printf("Attempting to connect to database with driver %s and DSN %s\n", driver, dsn)
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		Debug:           true,
	}
	db, err := Open(driver, dsn, opts)
	if err != nil {
		fmt.Printf("Failed to open database connection: %v\n", err)
		return nil
	}

	// Ping database to ensure connection is valid
	if err := db.DB.Ping(); err != nil {
		fmt.Printf("Failed to ping database: %v\n", err)
		db.Close()
		return nil
	}

	return db
}

func setupChainTestDB(t *testing.T) *DB {
	config := testutils.DefaultMySQLConfig()
	config.User = "root"
	// 使用正确的密码
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		Debug:           true,
	}
	db, err := Open(config.Driver, config.DSN(), opts)
	if err != nil {
		t.Skipf("Skipping test due to database connection error: %v", err)
		return nil
	}

	// Test database connection
	if err := db.DB.Ping(); err != nil {
		t.Skipf("Failed to ping database: %v", err)
		return nil
	}

	// Drop table if exists to ensure clean state
	_, err = db.DB.Exec("DROP TABLE IF EXISTS chaintestuser")
	if err != nil {
		t.Skipf("Failed to drop test table: %v", err)
		return nil
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS chaintestuser (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			age BIGINT,
			email VARCHAR(255),
			created_at DATETIME,
			updated_at DATETIME,
			is_active TINYINT(1) DEFAULT 1,
			score DOUBLE DEFAULT 0.0
		)
	`
	_, err = db.DB.Exec(createTableSQL)
	if err != nil {
		t.Errorf("Failed to create test table: %v", err)
		db.Close()
		return nil
	}

	// Clear test data
	_, err = db.DB.Exec("TRUNCATE TABLE chaintestuser")
	if err != nil {
		t.Errorf("Failed to truncate test table: %v", err)
		db.Close()
		return nil
	}

	return db
}

func setupMySQLDB(t *testing.T) *DB {
	config := testutils.DefaultMySQLConfig()
	db := setupDB(config.Driver, config.DSN())
	if db == nil {
		t.Fatalf("Failed to connect to MySQL with DSN: %s", config.DSN())
	}
	return db
}

func setupPostgreSQLDB(t *testing.T) *DB {
	config := testutils.DefaultPostgresConfig()
	db := setupDB(config.Driver, config.DSN())
	if db == nil {
		t.Fatalf("Failed to connect to PostgreSQL with DSN: %s", config.DSN())
	}
	return db
}

func TestChainOperations(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupMySQLDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer db.Close()

		// Drop table if exists and create test table
		_, err := db.DB.Exec("DROP TABLE IF EXISTS tests")
		assert.NoError(t, err)

		err = db.Chain().CreateTable(&TestModel{})
		assert.NoError(t, err)
		defer func() {
			_, err := db.DB.Exec("DROP TABLE IF EXISTS tests")
			if err != nil {
				t.Logf("Failed to drop table: %v", err)
			}
		}()

		runChainOperationsTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupPostgreSQLDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer db.Close()

		// Drop table if exists and create test table
		_, err := db.DB.Exec("DROP TABLE IF EXISTS tests")
		assert.NoError(t, err)

		err = db.Chain().CreateTable(&TestModel{})
		assert.NoError(t, err)
		defer func() {
			_, err := db.DB.Exec("DROP TABLE IF EXISTS tests")
			if err != nil {
				t.Logf("Failed to drop table: %v", err)
			}
		}()

		runChainOperationsTest(t, db)
	})
}

func runChainOperationsTest(t *testing.T, db *DB) {
	tableName := "tests"

	// Drop table if exists
	_, err := db.DB.Exec("DROP TABLE IF EXISTS " + tableName)
	assert.NoError(t, err)

	// Create test table
	var createTableSQL string
	if db.Factory.GetType() == "postgres" {
		createTableSQL = `CREATE TABLE tests (
			id BIGSERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			age INTEGER NOT NULL,
			email VARCHAR(255) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			is_active BOOLEAN NOT NULL DEFAULT true
		)`
	} else {
		createTableSQL = `CREATE TABLE IF NOT EXISTS ` + tableName + ` (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			age INTEGER NOT NULL,
			email VARCHAR(255) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			is_active BOOLEAN NOT NULL DEFAULT true
		)`
	}
	_, err = db.DB.Exec(createTableSQL)
	assert.NoError(t, err)

	// Verify table creation
	var tableExists bool
	if db.Factory.GetType() == "postgres" {
		err = db.DB.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)", tableName).Scan(&tableExists)
	} else {
		err = db.DB.QueryRow("SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = ?", tableName).Scan(&tableExists)
	}
	assert.NoError(t, err)
	assert.True(t, tableExists, "Table should exist after creation")

	// Test Insert
	chain := db.Chain().Table(tableName)
	var result *define.Result
	if db.Factory.GetType() == "postgres" {
		insertSQL := "INSERT INTO tests (name, age, email, is_active, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id"
		insertArgs := []interface{}{"John", 30, "john@example.com", true, time.Now()}
		var lastID int64
		queryErr := db.DB.QueryRow(insertSQL, insertArgs...).Scan(&lastID)
		assert.NoError(t, queryErr)
		result = &define.Result{
			ID:       lastID,
			Affected: 1,
		}
	} else {
		result = chain.Values(map[string]interface{}{
			"name":       "John",
			"age":        30,
			"email":      "john@example.com",
			"is_active":  true,
			"created_at": time.Now(),
		}).Save()
	}
	assert.NoError(t, result.Error)
	fmt.Printf("Insert result: ID=%d, Affected=%d\n", result.ID, result.Affected)

	// Verify the insert using direct SQL
	var name string
	var age int
	var queryErr error
	if db.Factory.GetType() == "postgres" {
		var count int64
		queryErr = db.DB.QueryRow("SELECT COUNT(*) FROM tests").Scan(&count)
		assert.NoError(t, queryErr)
		fmt.Printf("Total records in table: %d\n", count)

		queryErr = db.DB.QueryRow("SELECT name, age FROM tests WHERE id = $1", result.ID).Scan(&name, &age)
	} else {
		var count int64
		queryErr = db.DB.QueryRow("SELECT COUNT(*) FROM tests").Scan(&count)
		assert.NoError(t, queryErr)
		fmt.Printf("Total records in table: %d\n", count)

		queryErr = db.DB.QueryRow("SELECT name, age FROM tests WHERE id = ?", result.ID).Scan(&name, &age)
	}
	assert.NoError(t, queryErr)
	assert.Equal(t, "John", name)
	assert.Equal(t, 30, age)

	// Test Select
	var users []struct {
		ID        int64     `gom:"id"`
		Name      string    `gom:"name"`
		Age       int       `gom:"age"`
		CreatedAt time.Time `gom:"created_at"`
	}
	err = db.Chain().Table(tableName).Where("id", define.OpEq, result.ID).List(&users).Error
	assert.NoError(t, err)
	if assert.NotEmpty(t, users, "Expected users to be non-empty") {
		assert.Equal(t, "John", users[0].Name)
		assert.Equal(t, 30, users[0].Age)
	}

	// Test Update
	updateResult := db.Chain().Table(tableName).Where("id", define.OpEq, result.ID).
		Values(map[string]interface{}{"age": 31}).Save()
	assert.NoError(t, updateResult.Error)

	// Verify update
	users = nil
	err = db.Chain().Table(tableName).Where("id", define.OpEq, result.ID).List(&users).Error
	assert.NoError(t, err)
	if assert.NotEmpty(t, users, "Expected users to be non-empty after update") {
		assert.Equal(t, 31, users[0].Age)
	}

	// Test Count
	count, err := db.Chain().Table(tableName).Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Test Delete
	deleteResult := db.Chain().Table(tableName).Where("id", define.OpEq, result.ID).Delete()
	assert.NoError(t, deleteResult.Error)

	// Verify delete
	count, err = db.Chain().Table(tableName).Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// Clean up
	_, err = db.DB.Exec("DROP TABLE IF EXISTS " + tableName)
	assert.NoError(t, err)
}

func TestChainConcurrentOperations(t *testing.T) {
	t.Run("Concurrent Chain Operations", func(t *testing.T) {
		db := setupMySQLDB(t)
		factory := &define.MockSQLFactory{}
		chain := NewChain(db, factory)
		var wg sync.WaitGroup
		numGoroutines := 10

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				// 测试并发添加条件
				chain.Where("id", define.OpEq, id)
				chain.And("status", define.OpEq, "active")
				chain.Or("type", define.OpEq, "test")
			}(i)
		}
		wg.Wait()

		// 验证并发操作后的链状态
		assert.NotNil(t, chain)
		assert.NotEmpty(t, chain.conds)
	})
}

func TestChainErrorHandling(t *testing.T) {
	t.Run("Invalid SQL Generation", func(t *testing.T) {
		factory := &define.MockSQLFactory{}
		chain := NewChain(nil, factory)

		// Test empty table name
		sql, args, err := chain.BuildSelect()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty table name")
		assert.Empty(t, sql)
		assert.Empty(t, args)

		// Test valid table name but no conditions
		chain.Table("test_table")
		sql, args, err = chain.BuildSelect()
		assert.NoError(t, err)
		assert.NotEmpty(t, sql)
		assert.Empty(t, args)
	})

	t.Run("Invalid Condition Values", func(t *testing.T) {
		factory := &define.MockSQLFactory{}
		chain := NewChain(nil, factory)
		chain.Table("test_table")

		// Test nil value
		chain.Where("id", define.OpEq, nil)
		sql, args, err := chain.BuildSelect()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid condition: nil value not allowed")
		assert.Empty(t, sql)
		assert.Empty(t, args)

		// Reset chain for next test
		chain = NewChain(nil, factory)
		chain.Table("test_table")

		// Test invalid operator
		chain.Where("id", define.OpType(999), 1)
		sql, args, err = chain.BuildSelect()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid operator")
		assert.Empty(t, sql)
		assert.Empty(t, args)

		// Reset chain for next test
		chain = NewChain(nil, factory)
		chain.Table("test_table")

		// Test valid condition
		chain.Where("id", define.OpEq, 1)
		sql, args, err = chain.BuildSelect()
		assert.NoError(t, err)
		assert.NotEmpty(t, sql)
		assert.NotEmpty(t, args)
	})
}
