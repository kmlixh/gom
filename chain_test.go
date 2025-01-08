package gom

import (
	"errors"
	"fmt"
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
	CreatedAt time.Time `gom:"created_at,notnull,default"`
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

	// Verify database connection by executing a simple query
	var version string
	if err := db.DB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		fmt.Printf("Failed to get database version: %v\n", err)
		db.Close()
		return nil
	}
	fmt.Printf("Successfully connected to database version: %s\n", version)

	return db
}

func setupMySQLDB(t *testing.T) *DB {
	db := setupDB("mysql", testutils.TestMySQLDSN)
	if db == nil {
		t.Fatalf("Failed to connect to MySQL: %s", testutils.TestMySQLDSN)
	}
	return db
}

func setupPostgreSQLDB(t *testing.T) *DB {
	db := setupDB("postgres", testutils.TestPgDSN)
	if db == nil {
		t.Fatalf("Failed to connect to PostgreSQL: %s", testutils.TestPgDSN)
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
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`
	} else {
		createTableSQL = `CREATE TABLE tests (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			age INTEGER NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
	result := chain.Values(map[string]interface{}{
		"name":       "John",
		"age":        30,
		"created_at": time.Now(),
	}).Save()
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
