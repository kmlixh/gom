package gom

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

func TestBatchOperations(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer db.Close()

		runBatchOperationsTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer db.Close()

		runBatchOperationsTest(t, db)
	})
}

func runBatchOperationsTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Prepare test data
	values := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "test",
			"age":        i,
			"created_at": time.Now(),
		})
	}

	// Test batch insert
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// Verify inserted data
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), count)
}

func TestBatchInsert(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
		runBatchInsertTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
		runBatchInsertTest(t, db)
	})
}

func runBatchInsertTest(t *testing.T, db *DB) {
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
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
		runBatchUpdateTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
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
	result := db.Chain().Table("tests").
		Set("name", "updated").
		Where("name", define.OpEq, "test").
		Save()
	assert.NoError(t, result.Error)

	// Verify updated data
	count, err := db.Chain().Table("tests").Where("name", define.OpEq, "updated").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), count)
}

func TestBatchDelete(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
		runBatchDeleteTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
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
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
		runBatchTransactionTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
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
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
		runBatchErrorTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer cleanupTestDB(t, db)
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
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer db.Close()

		runBatchOperationsEdgeCasesTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer db.Close()

		runBatchOperationsEdgeCasesTest(t, db)
	})
}

func runBatchOperationsEdgeCasesTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	t.Run("Empty Batch Insert", func(t *testing.T) {
		values := make([]map[string]interface{}, 0)
		affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), affected)
	})

	t.Run("Invalid Column Name", func(t *testing.T) {
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

	t.Run("Batch Size Edge Cases", func(t *testing.T) {
		values := make([]map[string]interface{}, 0)
		for i := 0; i < 5; i++ {
			values = append(values, map[string]interface{}{
				"name":       "test",
				"age":        i,
				"created_at": time.Now(),
			})
		}

		// Test with batch size larger than data size
		affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), affected)

		// Test with batch size of 1
		affected, err = db.Chain().Table("tests").BatchValues(values).BatchInsert(1)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), affected)

		// Test with zero batch size (should use default)
		affected, err = db.Chain().Table("tests").BatchValues(values).BatchInsert(0)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), affected)
	})

	t.Run("Null Values", func(t *testing.T) {
		values := []map[string]interface{}{
			{
				"name":       nil,
				"age":        nil,
				"created_at": time.Now(),
			},
		}
		affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), affected)

		var result TestModel
		err = db.Chain().Table("tests").Where("name", define.OpIsNull, nil).First(&result).Error()
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Age)
	})

	t.Run("Large Batch Insert", func(t *testing.T) {
		values := make([]map[string]interface{}, 0)
		for i := 0; i < 1000; i++ {
			values = append(values, map[string]interface{}{
				"name":       "test" + strconv.Itoa(i),
				"age":        i % 100,
				"created_at": time.Now(),
			})
		}

		affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(50)
		assert.NoError(t, err)
		assert.Equal(t, int64(1000), affected)

		count, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(1000), count)
	})

	t.Run("Concurrent Batch Operations", func(t *testing.T) {
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
