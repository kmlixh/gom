package gom

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

func setupBatchTestDB(t *testing.T) *DB {
	config := testutils.DefaultMySQLConfig()
	config.User = "root"
	config.Password = "123456" // 使用正确的密码
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
	_, err = db.DB.Exec("DROP TABLE IF EXISTS batchtestuser")
	if err != nil {
		t.Skipf("Failed to drop test table: %v", err)
		return nil
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS batchtestuser (
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
	_, err = db.DB.Exec("TRUNCATE TABLE batchtestuser")
	if err != nil {
		t.Errorf("Failed to truncate test table: %v", err)
		db.Close()
		return nil
	}

	return db
}

func TestBatchOperations(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer db.Close()
		runBatchOperationsTest(t, db, "batch_test_mysql")
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer db.Close()
		runBatchOperationsTest(t, db, "batch_test_pg")
	})
}

func runBatchOperationsTest(t *testing.T, db *DB, tableName string) {
	// Create test table with unique name
	type BatchTestModel struct {
		ID        int64     `gom:"id"`
		Name      string    `gom:"name"`
		Age       int       `gom:"age"`
		CreatedAt time.Time `gom:"created_at"`
	}

	// Drop table if exists
	_ = testutils.CleanupTestDB(db.DB, tableName)

	// Create test table
	err := db.Chain().Table(tableName).CreateTable(&BatchTestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, tableName)

	// Prepare test data
	values := make([]map[string]interface{}, 0, 100)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       fmt.Sprintf("Test%d", i),
			"age":        20 + i,
			"created_at": time.Now(),
		})
	}

	// Test batch insert
	t.Run("BatchInsert", func(t *testing.T) {
		affected, err := db.Chain().Table(tableName).BatchValues(values).BatchInsert(10)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), affected)
	})

	// Test batch update
	t.Run("BatchUpdate", func(t *testing.T) {
		updateValues := make([]map[string]interface{}, 0)
		for i := 0; i < 100; i++ {
			updateValues = append(updateValues, map[string]interface{}{
				"id":   int64(i + 1), // Assuming auto-incrementing IDs
				"name": fmt.Sprintf("updated_%d", i),
				"age":  i + 100,
			})
		}
		affected, err := db.Chain().Table(tableName).BatchValues(updateValues).BatchUpdate(10)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), affected)

		var count int64
		count, err = db.Chain().Table(tableName).Where("age", define.OpGt, 99).Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(100), count)
	})

	// Test batch delete
	t.Run("BatchDelete", func(t *testing.T) {
		affected, err := db.Chain().Table(tableName).Where("age", define.OpGt, 50).BatchDelete(10)
		assert.NoError(t, err)
		assert.True(t, affected > 0)
	})
}

func TestBatchInsert(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchInsertTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchInsertTest(t, db)
	})
}

func runBatchInsertTest(t *testing.T, db *DB) {
	// Clean up test table first
	_ = testutils.CleanupTestDB(db.DB, "tests")

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Insert test data
	values := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "test",
			"age":        i,
			"created_at": time.Now(),
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// Verify inserted data
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), count)
}

func TestBatchUpdate(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchUpdateTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchUpdateTest(t, db)
	})
}

func runBatchUpdateTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Insert test data
	values := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "test",
			"age":        i,
			"created_at": time.Now(),
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// Test batch update
	updateValues := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		updateValues = append(updateValues, map[string]interface{}{
			"id":   int64(i + 1), // Assuming auto-incrementing IDs
			"name": fmt.Sprintf("updated_%d", i),
			"age":  i + 100,
		})
	}
	affected, err = db.Chain().Table("tests").BatchValues(updateValues).BatchUpdate(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	var count int64
	count, err = db.Chain().Table("tests").Where("age", define.OpGt, 99).Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), count)
}

func TestBatchDelete(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchDeleteTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchDeleteTest(t, db)
	})
}

func runBatchDeleteTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Insert test data
	values := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "test",
			"age":        i,
			"created_at": time.Now(),
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// Test batch delete
	result := db.Chain().Table("tests").
		Where("age", define.OpLt, 50).
		Delete()
	assert.NoError(t, result.Error)

	// Verify deleted data
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(50), count)
}

func TestBatchTransaction(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchTransactionTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchTransactionTest(t, db)
	})
}

func runBatchTransactionTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Test transaction
	err = db.Chain().Transaction(func(tx *Chain) error {
		// Insert test data in transaction
		values := make([]map[string]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, map[string]interface{}{
				"name":       "test",
				"age":        i,
				"created_at": time.Now(),
			})
		}
		affected, err := tx.Table("tests").BatchValues(values).BatchInsert(10)
		if err != nil {
			return err
		}
		if affected != int64(100) {
			return fmt.Errorf("unexpected affected rows: got %d, want %d", affected, 100)
		}
		return nil
	})
	assert.NoError(t, err)

	// Verify transaction was committed
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), count)
}

func TestBatchError(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchErrorTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupBatchTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runBatchErrorTest(t, db)
	})
}

func runBatchErrorTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Test batch insert with invalid data
	values := []map[string]interface{}{
		{
			"name":       strings.Repeat("x", 300), // Name too long
			"age":        30,
			"created_at": time.Now(),
		},
	}
	_, err = db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.Error(t, err)

	// Verify no data was inserted
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestBatchOperationsEdgeCases(t *testing.T) {
	db := setupBatchTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer cleanupTestDB(t, db)

	// Create test table first
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	t.Run("Batch_Size_Edge_Cases", func(t *testing.T) {
		// Test with invalid batch size
		_, err := db.Chain().BatchInsert(0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid batch size")

		// Test with valid batch size
		chain := db.Chain().Table("tests")
		values := make([]map[string]interface{}, 5)
		for i := 0; i < 5; i++ {
			values[i] = map[string]interface{}{
				"name": fmt.Sprintf("test_%d", i),
				"age":  20 + i,
			}
		}
		affected, err := chain.BatchValues(values).BatchInsert(2)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), affected)
	})

	t.Run("Null_Values", func(t *testing.T) {
		values := []map[string]interface{}{
			{
				"name": nil,
				"age":  25,
			},
		}
		_, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be null")

		// Test with non-null values
		values = []map[string]interface{}{
			{
				"name": "test_null",
				"age":  25,
			},
		}
		affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), affected)

		// Verify the record was inserted
		var results []TestModel
		err = db.Chain().Table("tests").Where("name", define.OpEq, "test_null").List(&results).Error
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, "test_null", results[0].Name)
	})

	t.Run("Large_Batch_Insert", func(t *testing.T) {
		// Clear the table first
		result := db.Chain().Table("tests").Delete()
		assert.NoError(t, result.Error)

		values := make([]map[string]interface{}, 1000)
		for i := 0; i < 1000; i++ {
			values[i] = map[string]interface{}{
				"name": fmt.Sprintf("large_test_%d", i),
				"age":  20 + i%50,
			}
		}
		affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(100)
		assert.NoError(t, err)
		assert.Equal(t, int64(1000), affected)

		// Verify the count
		count, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(1000), count)
	})

	t.Run("Invalid_Column_Name", func(t *testing.T) {
		values := []map[string]interface{}{
			{
				"invalid_column": "test",
				"age":            25,
				"created_at":     time.Now(),
			},
		}
		_, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
		assert.Error(t, err)
	})

	t.Run("Empty_Batch_Insert", func(t *testing.T) {
		values := make([]map[string]interface{}, 0)
		_, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no values to insert")
	})

	t.Run("Concurrent_Batch_Operations", func(t *testing.T) {
		// Clear the table first
		result := db.Chain().Table("tests").Delete()
		assert.NoError(t, result.Error)

		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				values := make([]map[string]interface{}, 0)
				for j := 0; j < 100; j++ {
					values = append(values, map[string]interface{}{
						"name":       fmt.Sprintf("concurrent_test_%d_%d", i, j),
						"age":        j,
						"created_at": time.Now(),
					})
				}
				affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
				assert.NoError(t, err)
				assert.Equal(t, int64(100), affected)
			}(i)
		}
		wg.Wait()

		count, err := db.Chain().Table("tests").Where("name", define.OpLike, "concurrent_test_%").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(500), count)
	})
}

func cleanupTestDB(t *testing.T, db *DB) {
	result := db.Chain().Table("tests").Delete()
	assert.NoError(t, result.Error)
}
