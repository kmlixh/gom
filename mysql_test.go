package gom

import (
	"database/sql"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/factory/mysql"
	_ "github.com/kmlixh/gom/v4/factory/mysql"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

func setupMySQLTestDB(t *testing.T) *DB {
	// Open MySQL connection
	mysqlDB, err := sql.Open("mysql", testutils.TestMySQLDSN)
	if err != nil {
		t.Skipf("Skipping MySQL tests: %v", err)
		return nil
	}

	// Test MySQL connection
	if err = mysqlDB.Ping(); err != nil {
		mysqlDB.Close()
		t.Skipf("Skipping MySQL tests: %v", err)
		return nil
	}

	db := &DB{
		DB:      mysqlDB,
		Factory: &mysql.Factory{},
	}

	// Create test tables
	err = db.Chain().CreateTable(&TestModel{})
	if err != nil {
		mysqlDB.Close()
		t.Fatalf("Failed to create test table: %v", err)
	}

	return db
}

func cleanupMySQLTestDB(t *testing.T, db *DB) {
	if db == nil || db.DB == nil {
		return
	}

	err := testutils.CleanupTestDB(db.DB, "tests", "test_details", "test_categories")
	if err != nil {
		t.Errorf("Failed to cleanup test database: %v", err)
	}

	db.Close()
}

func TestMySQLDBConnection(t *testing.T) {
	db, err := sql.Open("mysql", testutils.TestMySQLDSN)
	if err != nil {
		t.Skipf("Skipping MySQL test: %v", err)
		return
	}
	defer db.Close()

	err = db.Ping()
	assert.NoError(t, err)
}

func TestMySQLDBChain(t *testing.T) {
	db := setupMySQLTestDB(t)
	if db == nil {
		t.Skip("Skipping MySQL test due to database connection error")
		return
	}
	defer db.Close()

	chain := db.Chain()
	assert.NotNil(t, chain)
}

func TestMySQLDBTransaction(t *testing.T) {
	db := setupMySQLTestDB(t)
	if db == nil {
		t.Skip("Skipping MySQL test due to database connection error")
		return
	}
	defer db.Close()

	// Test transaction operations
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, tx)

	err = tx.Rollback()
	assert.NoError(t, err)
}

func TestMySQLDBTableInfo(t *testing.T) {
	db := setupMySQLTestDB(t)
	if db == nil {
		t.Skip("Skipping MySQL test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Get table info
	info, err := db.GetTableInfo("tests")
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, "tests", info.TableName)
}

func TestMySQLDBGetTables(t *testing.T) {
	db := setupMySQLTestDB(t)
	if db == nil {
		t.Skip("Skipping MySQL test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Get tables
	tables, err := db.GetTables("*")
	assert.NoError(t, err)
	assert.NotNil(t, tables)
	assert.Contains(t, tables, "tests")
}

func TestMySQLDBGenerateStruct(t *testing.T) {
	db := setupMySQLTestDB(t)
	if db == nil {
		t.Skip("Skipping MySQL test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Generate struct
	tempDir := t.TempDir()
	err = db.GenerateStruct("tests", tempDir, "models")
	assert.NoError(t, err)
}

func TestMySQLDBClose(t *testing.T) {
	db := setupMySQLTestDB(t)
	if db == nil {
		t.Skip("Skipping MySQL test due to database connection error")
		return
	}
	err := db.Close()
	assert.NoError(t, err)
}

func TestMySQLDBMetrics(t *testing.T) {
	db := setupMySQLTestDB(t)
	if db == nil {
		t.Skip("Skipping MySQL test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Execute some queries to generate metrics
	for i := 0; i < 5; i++ {
		result := db.Chain().Table("tests").Fields("id").List()
		assert.NoError(t, result.Error)
	}

	// Get metrics
	metrics := db.GetMetrics()
	assert.True(t, metrics.OpenConnections >= 0)
	assert.True(t, metrics.InUseConnections >= 0)
	assert.True(t, metrics.IdleConnections >= 0)
	assert.True(t, metrics.WaitCount >= 0)
}

func TestMySQLQueryStats(t *testing.T) {
	db := setupMySQLTestDB(t)
	if db == nil {
		t.Skip("Skipping MySQL test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Execute a query and check stats
	chain := db.Chain().Table("tests").Fields("id")
	result := chain.List()
	assert.NoError(t, result.Error)

	stats := chain.GetLastQueryStats()
	assert.NotNil(t, stats)
	assert.NotEmpty(t, stats.SQL)
	assert.True(t, stats.Duration > 0)
	assert.NotNil(t, stats.StartTime)
}

func TestMySQLNestedTransactionsWithSavepoints(t *testing.T) {
	db := setupMySQLTestDB(t)
	if db == nil {
		t.Skip("Skipping MySQL test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Start outer transaction
	tx, err := db.DB.Begin()
	assert.NoError(t, err)
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	// Insert in outer transaction
	model1 := &TestModel{
		Name:      "Test1",
		Age:       25,
		CreatedAt: time.Now(),
	}
	_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
		model1.Name, model1.Age, model1.CreatedAt)
	assert.NoError(t, err)

	// Create savepoint
	_, err = tx.Exec("SAVEPOINT sp1")
	assert.NoError(t, err)

	// Insert in savepoint
	model2 := &TestModel{
		Name:      "Test2",
		Age:       30,
		CreatedAt: time.Now(),
	}
	_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
		model2.Name, model2.Age, model2.CreatedAt)
	assert.NoError(t, err)

	// Verify both records exist
	var count int64
	err = tx.QueryRow("SELECT COUNT(*) FROM tests").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Rollback to savepoint
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	assert.NoError(t, err)

	// Verify only first record exists
	err = tx.QueryRow("SELECT COUNT(*) FROM tests").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Insert different second record
	model3 := &TestModel{
		Name:      "Test3",
		Age:       35,
		CreatedAt: time.Now(),
	}
	_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
		model3.Name, model3.Age, model3.CreatedAt)
	assert.NoError(t, err)

	// Commit transaction
	err = tx.Commit()
	assert.NoError(t, err)
	tx = nil

	// Verify final state
	var models []TestModel
	qr := db.Chain().Table("tests").OrderBy("age").List(&models)
	assert.NoError(t, qr.Error)
	assert.Equal(t, 2, len(models))
	assert.Equal(t, "Test1", models[0].Name)
	assert.Equal(t, "Test3", models[1].Name)
}
