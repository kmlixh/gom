package gom

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/factory/postgres"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

func setupTestDB(t *testing.T) *DB {
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		Debug:           true,
	}

	// Try MySQL first
	db, err := Open("mysql", "root:123456@tcp(10.0.1.5:3306)/test?charset=utf8mb4&parseTime=True", opts)
	if err == nil {
		return db
	}

	// If MySQL fails, try PostgreSQL
	db, err = Open("postgres", "host=10.0.1.5 port=5432 user=postgres password=123456 dbname=test sslmode=disable", opts)
	if err == nil {
		return db
	}

	if t != nil {
		t.Logf("Failed to connect to both MySQL and PostgreSQL: %v", err)
	}
	return nil
}

func getTestDB() *DB {
	return setupTestDB(nil)
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
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Start a transaction
	txChain, err := db.Chain().BeginChain()
	assert.NoError(t, err)

	// Insert first record
	model1 := &TestModel{
		Name:      "First",
		Age:       25,
		CreatedAt: time.Now(),
	}
	result := txChain.Table("tests").From(model1).Save()
	assert.NoError(t, result.Error)

	// Create savepoint
	err = txChain.Savepoint("sp1")
	assert.NoError(t, err)

	// Insert second record
	model2 := &TestModel{
		Name:      "Second",
		Age:       30,
		CreatedAt: time.Now(),
	}
	result = txChain.Table("tests").From(model2).Save()
	assert.NoError(t, result.Error)

	// Rollback to savepoint
	err = txChain.RollbackTo("sp1")
	assert.NoError(t, err)

	// Insert different second record
	model3 := &TestModel{
		Name:      "Third",
		Age:       35,
		CreatedAt: time.Now(),
	}
	result = txChain.Table("tests").From(model3).Save()
	assert.NoError(t, result.Error)

	// Release savepoint
	err = txChain.ReleaseSavepoint("sp1")
	assert.NoError(t, err)

	// Commit transaction
	err = txChain.Commit()
	assert.NoError(t, err)

	// Verify results
	var models []TestModel
	qr := db.Chain().Table("tests").OrderBy("age").List(&models)
	assert.NoError(t, qr.Error)
	assert.Equal(t, 2, len(models))
	assert.Equal(t, "First", models[0].Name)
	assert.Equal(t, "Third", models[1].Name)
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
	err := db.Chain().CreateTable(&TestModel{})
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
					INSERT INTO tests (name, age, created_at)
					VALUES ($1, $2, $3)
				`, "Test", 25, time.Now())
			} else {
				// MySQL uses ? for parameter placeholders
				result = tx.RawExecute(`
					INSERT INTO tests (name, age, created_at)
					VALUES (?, ?, ?)
				`, "Test", 25, time.Now())
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
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 测试正常回滚
	chain, err := db.Chain().Begin()
	assert.NoError(t, err)

	model := &TestModel{Name: "Test", Age: 25, CreatedAt: time.Now()}
	result := chain.Table("tests").From(model).Save()
	assert.NoError(t, result.Error)

	err = chain.Rollback()
	assert.NoError(t, err)

	// 验证数据已回滚
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// 测试重复回滚
	err = chain.Rollback()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")

	// 测试提交已回滚的事务
	err = chain.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")
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
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Test nested transactions
	err = db.Chain().Transaction(func(tx1 *Chain) error {
		// Insert in outer transaction
		model1 := &TestModel{
			Name:      "Test1",
			Age:       25,
			CreatedAt: time.Now(),
		}
		result := tx1.Table("tests").From(model1).Save()
		if result.Error != nil {
			return result.Error
		}

		// Start nested transaction
		return tx1.Transaction(func(tx2 *Chain) error {
			// Insert in inner transaction
			model2 := &TestModel{
				Name:      "Test2",
				Age:       30,
				CreatedAt: time.Now(),
			}
			result := tx2.Table("tests").From(model2).Save()
			if result.Error != nil {
				return result.Error
			}

			// Verify both records exist in nested transaction
			count, err := tx2.Table("tests").Count()
			if err != nil {
				return err
			}
			if count != 2 {
				return fmt.Errorf("expected 2 records, got %d", count)
			}
			return nil
		})
	})
	assert.NoError(t, err)

	// Verify final state
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
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
	err := db.Chain().CreateTable(&TestModel{})
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
		_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
			"Valid", 25, time.Now())
		assert.NoError(t, err)

		// Try to insert invalid record (name too long)
		_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
			strings.Repeat("x", 300), 30, time.Now())
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
		_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
			"Test", 25, time.Now())
		assert.NoError(t, err)

		// Create savepoint
		_, err = tx.Exec("SAVEPOINT sp1")
		assert.NoError(t, err)

		// Try to insert invalid record
		_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
			strings.Repeat("x", 300), 30, time.Now())
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
		_, err = tx1.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
			"Outer", 25, time.Now())
		assert.NoError(t, err)

		// Create savepoint for nested transaction
		_, err = tx1.Exec("SAVEPOINT nested_tx")
		assert.NoError(t, err)

		// Try to insert invalid record in nested transaction
		_, err = tx1.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
			strings.Repeat("x", 300), 30, time.Now())
		assert.Error(t, err)

		// Rollback to savepoint
		_, err = tx1.Exec("ROLLBACK TO SAVEPOINT nested_tx")
		assert.NoError(t, err)

		// Verify only outer transaction record exists
		var records []TestModel
		rows, err := tx1.Query("SELECT * FROM tests")
		assert.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var record TestModel
			err = rows.Scan(&record.ID, &record.Name, &record.Age, &record.CreatedAt)
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
		_, err = tx.Exec("INSERT INTO tests (name, age, created_at) VALUES (?, ?, ?)",
			"Test", 25, time.Now())
		assert.NoError(t, err)

		// Close transaction
		err = tx.Commit()
		assert.NoError(t, err)

		// Try to use transaction after close
		_, err = tx.Exec("INSERT INTO tests (name, age) VALUES (?, ?)", "Test2", 30)
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
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Test transaction propagation with savepoints
	err = db.Chain().TransactionWithOptions(define.TransactionOptions{
		PropagationMode: define.PropagationRequired,
	}, func(tx1 *Chain) error {
		// Insert in outer transaction
		model1 := &TestModel{
			Name:      "Test1",
			Age:       25,
			CreatedAt: time.Now(),
		}
		result := tx1.Table("tests").From(model1).Save()
		if result.Error != nil {
			return result.Error
		}

		// Start nested transaction with savepoint
		err := tx1.TransactionWithOptions(define.TransactionOptions{
			PropagationMode: define.PropagationNested,
		}, func(tx2 *Chain) error {
			// Insert in inner transaction
			model2 := &TestModel{
				Name:      "Test2",
				Age:       30,
				CreatedAt: time.Now(),
			}
			result := tx2.Table("tests").From(model2).Save()
			if result.Error != nil {
				return result.Error
			}

			// Verify both records exist in nested transaction
			count, err := tx2.Table("tests").Count()
			if err != nil {
				return err
			}
			if count != 2 {
				return fmt.Errorf("expected 2 records in nested transaction, got %d", count)
			}
			return errors.New("rollback nested transaction")
		})

		// Verify nested transaction was rolled back but outer transaction remains
		count, err := tx1.Table("tests").Count()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected 1 record after nested rollback, got %d", count)
		}
		return nil
	})
	assert.NoError(t, err)

	// Verify final state
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
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
				CREATE TABLE test_transaction_timeout (
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
