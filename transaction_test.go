package gom

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/factory/postgres"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

// TransactionTestModel represents a test model
type TransactionTestModel struct {
	ID        int64     `gom:"id,@,auto"`
	Name      string    `gom:"name"`
	Age       int       `gom:"age"`
	Email     string    `gom:"email"`
	CreatedAt time.Time `gom:"created_at"`
	UpdatedAt time.Time `gom:"updated_at"`
	IsActive  bool      `gom:"is_active"`
}

// TableName returns the table name
func (m *TransactionTestModel) TableName() string {
	return "tests"
}

// CreateSql returns the SQL to create the table
func (m *TransactionTestModel) CreateSql() string {
	return `CREATE TABLE IF NOT EXISTS tests (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		age INT NOT NULL,
		email VARCHAR(255) NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		is_active BOOLEAN NOT NULL DEFAULT true
	)`
}

func setupTestDB(t *testing.T) *DB {
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		Debug:           true,
	}

	// Try MySQL first
	mysqlConfig := testutils.DefaultMySQLConfig()
	db, err := Open(mysqlConfig.Driver, mysqlConfig.DSN(), opts)
	if err == nil {
		return db
	}

	// If MySQL fails, try PostgreSQL
	pgConfig := testutils.DefaultPostgresConfig()
	db, err = Open(pgConfig.Driver, pgConfig.DSN(), opts)
	if err == nil {
		return db
	}

	if t != nil {
		t.Logf("Failed to connect to both MySQL and PostgreSQL: %v", err)
	}
	return nil
}

// TestTransactionSavepoints 测试事务保存点操作
func TestTransactionSavepoints(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer func() {
		_ = testutils.CleanupTestDB(db.DB, "tests")
		db.Close()
	}()

	// Create test table
	err := db.Chain().CreateTable(&TransactionTestModel{})
	assert.NoError(t, err)

	// Start a transaction
	txChain, err := db.Chain().BeginChain()
	assert.NoError(t, err)

	now := time.Now()

	// Insert first record
	model1 := &TransactionTestModel{
		Name:      "First",
		Age:       25,
		Email:     "first@test.com",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  true,
	}
	result := txChain.Table("tests").Save(model1)
	assert.NoError(t, result.Error)

	// Create savepoint
	err = txChain.Savepoint("sp1")
	assert.NoError(t, err)

	// Insert second record
	model2 := &TransactionTestModel{
		Name:      "Second",
		Age:       30,
		Email:     "second@test.com",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  true,
	}
	result = txChain.Table("tests").Save(model2)
	assert.NoError(t, result.Error)

	// Rollback to savepoint
	err = txChain.RollbackTo("sp1")
	assert.NoError(t, err)

	// Insert different second record
	model3 := &TransactionTestModel{
		Name:      "Third",
		Age:       35,
		Email:     "third@test.com",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  true,
	}
	result = txChain.Table("tests").Save(model3)
	assert.NoError(t, result.Error)

	// Release savepoint
	err = txChain.ReleaseSavepoint("sp1")
	assert.NoError(t, err)

	// Commit transaction
	err = txChain.Commit()
	assert.NoError(t, err)

	// Verify results
	var models []TransactionTestModel
	qr := db.Chain().Table("tests").OrderBy("age").List(&models)
	assert.NoError(t, qr.Error)
	assert.Equal(t, 2, len(models), "应该有两条记录")

	if len(models) >= 2 {
		assert.Equal(t, "First", models[0].Name, "第一条记录应该是 First")
		assert.Equal(t, "Third", models[1].Name, "第二条记录应该是 Third")
	}
}

// TestTransactionIsolationLevels 测试事务隔离级别
func TestTransactionIsolationLevels(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()

		runTransactionIsolationLevelsTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runTransactionIsolationLevelsTest(t, db)
	})
}

func runTransactionIsolationLevelsTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TransactionTestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Test each isolation level
	levels := []define.IsolationLevel{
		define.LevelDefault,
		define.LevelReadUncommitted,
		define.LevelReadCommitted,
		define.LevelRepeatableRead,
		define.LevelSerializable,
	}

	for _, level := range levels {
		// Clean up before each test
		result := db.Chain().RawExecute("DELETE FROM tests")
		assert.NoError(t, result.Error)

		// Start a transaction with the current isolation level
		err = db.Chain().TransactionWithOptions(define.TransactionOptions{
			IsolationLevel: level,
		}, func(tx *Chain) error {
			var result define.Result
			if _, ok := db.Factory.(*postgres.Factory); ok {
				// PostgreSQL uses $1, $2, etc. for parameter placeholders
				result = tx.RawExecute(`
					INSERT INTO tests (name, age, email, created_at, is_active)
					VALUES ($1, $2, $3, $4, $5)
				`, "Test", 25, "test@example.com", time.Now(), true)
			} else {
				// MySQL uses ? for parameter placeholders
				result = tx.RawExecute(`
					INSERT INTO tests (name, age, email, created_at, is_active)
					VALUES (?, ?, ?, ?, ?)
				`, "Test", 25, "test@example.com", time.Now(), true)
			}
			if result.Error != nil {
				return result.Error
			}

			// Verify the record exists within the transaction
			count, err := tx.Table("tests").Count()
			if err != nil {
				return err
			}
			if count != 1 {
				return fmt.Errorf("expected count 1, got %d", count)
			}

			return nil
		})
		assert.NoError(t, err)

		// Verify the record exists after the transaction
		count, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count)
	}
}

// TestTransactionRollback 测试事务回滚
func TestTransactionRollback(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer func() {
		_ = testutils.CleanupTestDB(db.DB, "tests")
		db.Close()
	}()

	// Create test table
	err := db.Chain().CreateTable(&TransactionTestModel{})
	assert.NoError(t, err)

	now := time.Now()

	// Start a transaction
	txChain, err := db.Chain().BeginChain()
	assert.NoError(t, err)

	// Insert a record
	model := &TransactionTestModel{
		Name:      "Test",
		Age:       25,
		Email:     "",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  false,
	}
	result := txChain.Table("tests").Save(model)
	assert.Error(t, result.Error, "应该因为 email 为空而失败")

	// Rollback transaction
	err = txChain.Rollback()
	assert.NoError(t, err)

	// Verify no records were inserted
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count, "回滚后不应该有记录")
}

// TestNestedTransactions 测试嵌套事务
func TestNestedTransactions(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer func() {
		_ = testutils.CleanupTestDB(db.DB, "tests")
		db.Close()
	}()

	// Create test table
	err := db.Chain().CreateTable(&TransactionTestModel{})
	assert.NoError(t, err)

	now := time.Now()

	// Start transaction
	tx, err := db.Chain().BeginChain()
	assert.NoError(t, err)

	// Insert first record
	model1 := &TransactionTestModel{
		Name:      "Test1",
		Age:       25,
		Email:     "test1@example.com",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  true,
	}
	result := tx.Table("tests").Save(model1)
	assert.NoError(t, result.Error)

	// Create savepoint for nested transaction
	_, err = tx.tx.Exec("SAVEPOINT nested_tx")
	assert.NoError(t, err)

	// Insert second record
	model2 := &TransactionTestModel{
		Name:      "Test2",
		Age:       30,
		Email:     "test2@example.com",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  true,
	}
	result = tx.Table("tests").Save(model2)
	assert.NoError(t, result.Error)

	// Release savepoint
	_, err = tx.tx.Exec("RELEASE SAVEPOINT nested_tx")
	assert.NoError(t, err)

	// Commit transaction
	err = tx.Commit()
	assert.NoError(t, err)

	// Verify both records were inserted
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count, "应该有两条记录")

	var models []TransactionTestModel
	result = db.Chain().Table("tests").OrderBy("age").List(&models)
	assert.NoError(t, result.Error)
	assert.Equal(t, 2, len(models))
	if len(models) >= 2 {
		assert.Equal(t, "Test1", models[0].Name)
		assert.Equal(t, "Test2", models[1].Name)
	}
}

// TestTransactionErrorHandling 测试事务错误处理
func TestTransactionErrorHandling(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TransactionTestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	t.Run("Rollback_on_Error", func(t *testing.T) {
		tx, err := db.DB.Begin()
		assert.NoError(t, err)
		defer func() {
			if tx != nil {
				_ = tx.Rollback()
			}
		}()

		// Insert valid record
		_, err = tx.Exec("INSERT INTO tests (name, age, email, created_at, is_active) VALUES (?, ?, ?, ?, ?)",
			"Valid", 25, "valid@example.com", time.Now(), true)
		assert.NoError(t, err)

		// Try to insert invalid record (name too long)
		_, err = tx.Exec("INSERT INTO tests (name, age, email, created_at, is_active) VALUES (?, ?, ?, ?, ?)",
			strings.Repeat("x", 300), 30, "invalid@example.com", time.Now(), true)
		assert.Error(t, err)

		// Rollback transaction
		err = tx.Rollback()
		assert.NoError(t, err)
		tx = nil

		// Verify no records were inserted
		var count int64
		err = db.DB.QueryRow("SELECT COUNT(*) FROM tests").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("Savepoint_Error_Handling", func(t *testing.T) {
		tx, err := db.DB.Begin()
		assert.NoError(t, err)
		defer func() {
			if tx != nil {
				_ = tx.Rollback()
			}
		}()

		// Insert initial record
		_, err = tx.Exec("INSERT INTO tests (name, age, email, created_at, is_active) VALUES (?, ?, ?, ?, ?)",
			"Test", 25, "test@example.com", time.Now(), true)
		assert.NoError(t, err)

		// Create savepoint
		_, err = tx.Exec("SAVEPOINT sp1")
		assert.NoError(t, err)

		// Try to insert invalid record
		_, err = tx.Exec("INSERT INTO tests (name, age, email, created_at, is_active) VALUES (?, ?, ?, ?, ?)",
			strings.Repeat("x", 300), 30, "invalid@example.com", time.Now(), true)
		assert.Error(t, err)

		// Rollback to savepoint
		_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
		assert.NoError(t, err)

		// Rollback entire transaction
		err = tx.Rollback()
		assert.NoError(t, err)
		tx = nil

		// Verify no records were inserted
		var count int64
		err = db.DB.QueryRow("SELECT COUNT(*) FROM tests").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("Nested_Transaction_Error_Handling", func(t *testing.T) {
		// Start outer transaction
		tx1, err := db.DB.Begin()
		assert.NoError(t, err)
		defer func() {
			if tx1 != nil {
				_ = tx1.Rollback()
			}
		}()

		// Insert in outer transaction
		_, err = tx1.Exec("INSERT INTO tests (name, age, email, created_at, is_active) VALUES (?, ?, ?, ?, ?)",
			"Outer", 25, "outer@example.com", time.Now(), true)
		assert.NoError(t, err)

		// Create savepoint for nested transaction
		_, err = tx1.Exec("SAVEPOINT nested_tx")
		assert.NoError(t, err)

		// Try to insert invalid record in nested transaction
		_, err = tx1.Exec("INSERT INTO tests (name, age, email, created_at, is_active) VALUES (?, ?, ?, ?, ?)",
			strings.Repeat("x", 300), 30, "invalid@example.com", time.Now(), true)
		assert.Error(t, err)

		// Rollback to savepoint
		_, err = tx1.Exec("ROLLBACK TO SAVEPOINT nested_tx")
		assert.NoError(t, err)

		// Verify only outer transaction record exists
		var records []TransactionTestModel
		rows, err := tx1.Query("SELECT * FROM tests")
		assert.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var record TransactionTestModel
			err = rows.Scan(&record.ID, &record.Name, &record.Age, &record.Email, &record.CreatedAt, &record.UpdatedAt, &record.IsActive)
			assert.NoError(t, err)
			records = append(records, record)
		}
		assert.Equal(t, 1, len(records))
		assert.Equal(t, "Outer", records[0].Name)

		// Commit outer transaction
		err = tx1.Commit()
		assert.NoError(t, err)
		tx1 = nil
	})

	t.Run("Transaction_After_Close", func(t *testing.T) {
		tx, err := db.DB.Begin()
		assert.NoError(t, err)

		// Insert record
		_, err = tx.Exec("INSERT INTO tests (name, age, email, created_at, is_active) VALUES (?, ?, ?, ?, ?)",
			"Test", 25, "test@example.com", time.Now(), true)
		assert.NoError(t, err)

		// Close transaction
		err = tx.Commit()
		assert.NoError(t, err)

		// Try to use transaction after close
		_, err = tx.Exec("INSERT INTO tests (name, age, email, created_at, is_active) VALUES (?, ?, ?, ?, ?)",
			"Test2", 30, "test2@example.com", time.Now(), true)
		assert.Error(t, err)
	})
}

func TestTransactionPropagation(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer func() {
		_ = testutils.CleanupTestDB(db.DB, "tests")
		db.Close()
	}()

	// Create test table
	err := db.Chain().CreateTable(&TransactionTestModel{})
	assert.NoError(t, err)

	now := time.Now()

	// Start a transaction
	txChain, err := db.Chain().BeginChain()
	assert.NoError(t, err)

	// Insert first record
	model1 := &TransactionTestModel{
		Name:      "Test1",
		Age:       25,
		Email:     "test1@example.com",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  true,
	}
	result := txChain.Table("tests").Save(model1)
	assert.NoError(t, result.Error)

	// Test propagation by starting a new transaction
	err = txChain.TransactionWithOptions(define.TransactionOptions{
		PropagationMode: define.PropagationNested,
	}, func(nestedTx *Chain) error {
		// Insert second record
		model2 := &TransactionTestModel{
			Name:      "Test2",
			Age:       30,
			Email:     "test2@example.com",
			CreatedAt: now,
			UpdatedAt: now,
			IsActive:  true,
		}
		result := nestedTx.Table("tests").Save(model2)
		return result.Error
	})
	assert.NoError(t, err)

	// Commit the outer transaction
	err = txChain.Commit()
	assert.NoError(t, err)

	// Verify records
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count, "应该有两条记录")

	var models []TransactionTestModel
	result = db.Chain().Table("tests").OrderBy("age").List(&models)
	assert.NoError(t, result.Error)
	assert.Equal(t, 2, len(models))
	if len(models) >= 2 {
		assert.Equal(t, "Test1", models[0].Name)
		assert.Equal(t, "Test2", models[1].Name)
	}
}

func TestTransactionTimeout(t *testing.T) {
	for _, dbType := range []string{"MySQL", "PostgreSQL"} {
		t.Run(dbType, func(t *testing.T) {
			db := setupTestDB(t)
			if db == nil {
				t.Skip("Skipping test due to database connection error")
				return
			}
			defer db.Close()

			// Create test table
			query := `
				CREATE TABLE IF NOT EXISTS test_transaction_timeout (
					id SERIAL PRIMARY KEY,
					name VARCHAR(255),
					counter INT
				)
			`
			_, err := db.DB.Exec(query)
			assert.NoError(t, err)
			defer func() {
				_, err := db.DB.Exec("DROP TABLE IF EXISTS test_transaction_timeout")
				assert.NoError(t, err)
			}()

			// Insert initial data
			_, err = db.DB.Exec(`
				INSERT INTO test_transaction_timeout (name, counter)
				VALUES (?, 0)
			`, "test")
			assert.NoError(t, err)

			// Start first transaction
			tx1, err := db.DB.Begin()
			assert.NoError(t, err)
			defer func() {
				if tx1 != nil {
					_ = tx1.Rollback()
				}
			}()

			// Lock the row in first transaction
			var counter int
			err = tx1.QueryRow(`
				SELECT counter FROM test_transaction_timeout
				WHERE name = ? FOR UPDATE
			`, "test").Scan(&counter)
			assert.NoError(t, err)

			// Start second transaction
			tx2, err := db.DB.Begin()
			assert.NoError(t, err)
			defer func() {
				if tx2 != nil {
					_ = tx2.Rollback()
				}
			}()

			// Set a short timeout for the second transaction
			if db.Factory.GetType() == "mysql" {
				_, err = tx2.Exec("SET innodb_lock_wait_timeout = 1")
			} else {
				_, err = tx2.Exec("SET LOCAL lock_timeout = '1s'")
			}
			assert.NoError(t, err)

			// Try to update the same row in second transaction, should timeout
			errChan := make(chan error, 1)
			go func() {
				_, err := tx2.Exec(`
					UPDATE test_transaction_timeout
					SET counter = counter + 1
					WHERE name = ?
				`, "test")
				errChan <- err
			}()

			// Wait for timeout
			select {
			case err := <-errChan:
				assert.Error(t, err)
				errMsg := strings.ToLower(err.Error())
				if !strings.Contains(errMsg, "timeout") &&
					!strings.Contains(errMsg, "lock wait timeout") &&
					!strings.Contains(errMsg, "lock timeout") {
					t.Errorf("Expected timeout error, got: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("Test timed out waiting for transaction timeout")
			}

			// Cleanup
			err = tx1.Rollback()
			assert.NoError(t, err)
			tx1 = nil

			err = tx2.Rollback()
			assert.NoError(t, err)
			tx2 = nil
		})
	}
}

// Helper functions for creating and dropping test tables
func createTestTable(t *testing.T, db *DB, tableName string) {
	result := db.Chain().RawExecute(fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255)
		)
	`, tableName))
	if result.Error != nil {
		t.Fatalf("Failed to create test table: %v", result.Error)
	}
}

func dropTestTable(t *testing.T, db *DB, tableName string) {
	result := db.Chain().RawExecute(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if result.Error != nil {
		t.Fatalf("Failed to drop test table: %v", result.Error)
	}
}

// TestHelper encapsulates common test setup and cleanup
type TestHelper struct {
	t  *testing.T
	db *DB
}

// NewTestHelper creates a new test helper
func NewTestHelper(t *testing.T) *TestHelper {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
	}
	return &TestHelper{t: t, db: db}
}

// Cleanup performs cleanup after test
func (h *TestHelper) Cleanup() {
	if h.db != nil {
		_ = testutils.CleanupTestDB(h.db.DB, "tests", "test_details", "test_categories")
		h.db.Close()
	}
}

// SetupTestTable creates test table
func (h *TestHelper) SetupTestTable() {
	err := h.db.Chain().CreateTable(&TransactionTestModel{})
	assert.NoError(h.t, err)
}

// GetDB returns the database instance
func (h *TestHelper) GetDB() *DB {
	return h.db
}

func TestTransaction(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer func() {
		_ = testutils.CleanupTestDB(db.DB, "tests")
		db.Close()
	}()

	// Create test table
	err := db.Chain().CreateTable(&TransactionTestModel{})
	assert.NoError(t, err)

	now := time.Now()

	t.Run("Basic_Transaction", func(t *testing.T) {
		// Start a transaction
		txChain, err := db.Chain().BeginChain()
		assert.NoError(t, err)

		// Insert a record
		model := &TransactionTestModel{
			Name:      "test1",
			Age:       25,
			Email:     "test1@example.com",
			CreatedAt: now,
			UpdatedAt: now,
			IsActive:  true,
		}
		result := txChain.Table("tests").Save(model)
		assert.NoError(t, result.Error)

		// Commit transaction
		err = txChain.Commit()
		assert.NoError(t, err)

		// Verify record was inserted
		count, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count, "应该有一条记录")

		// Clean up
		result = db.Chain().Table("tests").Delete()
		assert.NoError(t, result.Error)
	})

	t.Run("Nested_Transaction", func(t *testing.T) {
		// Start outer transaction
		outerTx, err := db.Chain().BeginChain()
		assert.NoError(t, err)

		// Insert first record
		model1 := &TransactionTestModel{
			Name:      "test1",
			Age:       25,
			Email:     "test1@example.com",
			CreatedAt: now,
			UpdatedAt: now,
			IsActive:  true,
		}
		result := outerTx.Table("tests").Save(model1)
		assert.NoError(t, result.Error)

		// Start inner transaction
		_, err = outerTx.tx.Exec("SAVEPOINT nested_tx")
		assert.NoError(t, err)

		// Insert second record
		model2 := &TransactionTestModel{
			Name:      "test2",
			Age:       30,
			Email:     "test2@example.com",
			CreatedAt: now,
			UpdatedAt: now,
			IsActive:  true,
		}
		result = outerTx.Table("tests").Save(model2)
		assert.NoError(t, result.Error)

		// Release savepoint
		_, err = outerTx.tx.Exec("RELEASE SAVEPOINT nested_tx")
		assert.NoError(t, err)

		// Commit outer transaction
		err = outerTx.Commit()
		assert.NoError(t, err)

		// Verify both records were inserted
		count, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(2), count, "应该有两条记录")

		// Clean up
		result = db.Chain().Table("tests").Delete()
		assert.NoError(t, result.Error)
	})

	t.Run("Transaction_Isolation", func(t *testing.T) {
		// Start a transaction with serializable isolation
		txChain, err := db.Chain().BeginChain()
		assert.NoError(t, err)

		txChain.SetIsolationLevel(sql.LevelSerializable)

		// Insert a record
		model := &TransactionTestModel{
			Name:      "test1",
			Age:       25,
			Email:     "test1@example.com",
			CreatedAt: now,
			UpdatedAt: now,
			IsActive:  true,
		}
		result := txChain.Table("tests").Save(model)
		assert.NoError(t, result.Error)

		// Verify record exists in transaction
		count, err := txChain.Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count, "事务中应该有一条记录")

		// Verify record is not visible outside transaction
		countOutside, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), countOutside, "事务外不应该看到记录")

		// Commit transaction
		err = txChain.Commit()
		assert.NoError(t, err)

		// Verify record is now visible
		countAfter, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), countAfter, "提交后应该能看到记录")

		// Clean up
		result = db.Chain().Table("tests").Delete()
		assert.NoError(t, result.Error)
	})
}
