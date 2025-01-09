package gom

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/kmlixh/gom/v4/define"
	_ "github.com/kmlixh/gom/v4/factory/postgres"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

func setupPostgreSQLTestDB(t *testing.T) *DB {
	config := testutils.DefaultPostgresConfig()
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		Debug:           true,
	}

	db, err := Open(config.Driver, config.DSN(), opts)
	if err != nil {
		if t != nil {
			t.Skipf("Skipping PostgreSQL tests: %v", err)
		}
		return nil
	}
	return db
}

func cleanupPostgreSQLTestDB(t *testing.T, db *DB) {
	if db == nil || db.DB == nil {
		return
	}

	err := testutils.CleanupTestDB(db.DB, "tests", "test_details", "test_categories")
	if err != nil {
		t.Errorf("Failed to cleanup test database: %v", err)
	}

	db.Close()
}

func TestPostgreSQLDBConnection(t *testing.T) {
	config := testutils.DefaultPostgresConfig()
	db, err := sql.Open("pgx", config.DSN())
	if err != nil {
		t.Skipf("Skipping PostgreSQL test: %v", err)
		return
	}
	defer db.Close()

	err = db.Ping()
	assert.NoError(t, err)
}

func TestPostgreSQLDBChain(t *testing.T) {
	db := setupPostgreSQLTestDB(t)
	if db == nil {
		t.Skip("Skipping PostgreSQL test due to database connection error")
		return
	}
	defer db.Close()

	chain := db.Chain()
	assert.NotNil(t, chain)
}

func TestPostgreSQLDBTransaction(t *testing.T) {
	db := setupPostgreSQLTestDB(t)
	if db == nil {
		t.Skip("Skipping PostgreSQL test due to database connection error")
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

func TestPostgreSQLDBTableInfo(t *testing.T) {
	db := setupPostgreSQLTestDB(t)
	if db == nil {
		t.Skip("Skipping PostgreSQL test due to database connection error")
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

func TestPostgreSQLDBGetTables(t *testing.T) {
	db := setupPostgreSQLTestDB(t)
	if db == nil {
		t.Skip("Skipping PostgreSQL test due to database connection error")
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
	assert.Contains(t, tables, "public.tests")
}

func TestPostgreSQLDBGenerateStruct(t *testing.T) {
	db := setupPostgreSQLTestDB(t)
	if db == nil {
		t.Skip("Skipping PostgreSQL test due to database connection error")
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

func TestPostgreSQLDBClose(t *testing.T) {
	db := setupPostgreSQLTestDB(t)
	if db == nil {
		t.Skip("Skipping PostgreSQL test due to database connection error")
		return
	}
	err := db.Close()
	assert.NoError(t, err)
}

func TestPostgreSQLDBMetrics(t *testing.T) {
	db := setupPostgreSQLTestDB(t)
	if db == nil {
		t.Skip("Skipping PostgreSQL test due to database connection error")
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

func TestPostgreSQLQueryStats(t *testing.T) {
	db := setupPostgreSQLTestDB(t)
	if db == nil {
		t.Skip("Skipping PostgreSQL test due to database connection error")
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

func TestPostgreSQLNestedTransactionsWithSavepoints(t *testing.T) {
	db := setupPostgreSQLTestDB(t)
	if db == nil {
		t.Skip("Skipping PostgreSQL test due to database connection error")
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
	_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES ($1, $2, $3)",
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
	_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES ($1, $2, $3)",
		model2.Name, model2.Age, model2.CreatedAt)
	assert.NoError(t, err)

	// Rollback to savepoint
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	assert.NoError(t, err)

	// Commit transaction
	err = tx.Commit()
	assert.NoError(t, err)
	tx = nil

	// Verify only first insert was committed
	var count int
	err = db.DB.QueryRow("SELECT COUNT(*) FROM tests").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}
