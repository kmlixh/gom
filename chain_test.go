package gom

import (
	"errors"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	ErrTest = errors.New("test error")
)

func TestChainOperations(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer db.Close()

		runChainOperationsTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer db.Close()

		runChainOperationsTest(t, db)
	})
}

func runChainOperationsTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	t.Run("Chain Methods", func(t *testing.T) {
		// Test Table method
		chain := db.Chain().Table("tests")
		assert.NotNil(t, chain)

		// Test Fields method
		chain = chain.Fields("id", "name")
		assert.NotNil(t, chain)

		// Test Where method
		chain = chain.Where("age", define.OpGt, 20)
		assert.NotNil(t, chain)

		// Test OrderBy method
		chain = chain.OrderBy("age DESC")
		assert.NotNil(t, chain)

		// Test GroupBy method
		chain = chain.GroupBy("name")
		assert.NotNil(t, chain)

		// Test Having method
		chain = chain.Having("COUNT(*) > ?", 1)
		assert.NotNil(t, chain)

		// Test Limit and Offset methods
		chain = chain.Limit(10).Offset(5)
		assert.NotNil(t, chain)

		// Verify chain works by executing a query
		var result []TestModel
		err := chain.List(&result).Error()
		assert.NoError(t, err)
	})

	t.Run("Chain Query Execution", func(t *testing.T) {
		// Insert test data
		model := &TestModel{
			Name:      "test",
			Age:       25,
			CreatedAt: time.Now(),
		}
		result := db.Chain().Table("tests").From(model).Save()
		assert.NoError(t, result.Error)

		// Test First
		var firstResult TestModel
		err := db.Chain().Table("tests").First(&firstResult).Error()
		assert.NoError(t, err)
		assert.Equal(t, "test", firstResult.Name)

		// Test List
		var listResult []TestModel
		err = db.Chain().Table("tests").List(&listResult).Error()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(listResult))

		// Test Count
		count, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count)

		// Test Update
		result = db.Chain().Table("tests").
			Where("name", define.OpEq, "test").
			Set("age", 30).
			Save()
		assert.NoError(t, result.Error)

		// Verify update
		var updatedResult TestModel
		err = db.Chain().Table("tests").First(&updatedResult).Error()
		assert.NoError(t, err)
		assert.Equal(t, 30, updatedResult.Age)

		// Test Delete
		result = db.Chain().Table("tests").
			Where("name", define.OpEq, "test").
			Delete()
		assert.NoError(t, result.Error)

		// Verify delete
		count, err = db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("Chain Error Handling", func(t *testing.T) {
		// Test invalid table name
		result := db.Chain().Table("invalid_table").First(nil)
		assert.Error(t, result.Error())

		// Test invalid column name
		result = db.Chain().Table("tests").Fields("invalid_column").First(nil)
		assert.Error(t, result.Error())

		// Test invalid where condition
		result = db.Chain().Table("tests").
			Where("invalid_column", define.OpEq, "value").
			First(nil)
		assert.Error(t, result.Error())

		// Test invalid order by
		result = db.Chain().Table("tests").
			OrderBy("invalid_column").
			First(nil)
		assert.Error(t, result.Error())

		// Test invalid group by
		result = db.Chain().Table("tests").
			GroupBy("invalid_column").
			First(nil)
		assert.Error(t, result.Error())
	})

	t.Run("Chain Method Chaining", func(t *testing.T) {
		// Insert test data
		for i := 0; i < 10; i++ {
			model := &TestModel{
				Name:      "test",
				Age:       20 + i,
				CreatedAt: time.Now(),
			}
			result := db.Chain().Table("tests").From(model).Save()
			assert.NoError(t, result.Error)
		}

		// Test complex chain
		var results []TestModel
		err := db.Chain().
			Table("tests").
			Fields("id", "name", "age").
			Where("age", define.OpGt, 25).
			OrderBy("age DESC").
			GroupBy("name").
			Having("COUNT(*) > ?", 0).
			Limit(5).
			Offset(0).
			List(&results).
			Error()
		assert.NoError(t, err)
		assert.True(t, len(results) > 0)

		// Test chain reuse
		chain := db.Chain().
			Table("tests").
			Fields("id", "name", "age")

		// Use chain for count
		count, err := chain.Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(10), count)

		// Use chain for list
		err = chain.List(&results).Error()
		assert.NoError(t, err)
		assert.Equal(t, 10, len(results))
	})

	t.Run("Chain Transaction", func(t *testing.T) {
		// Test successful transaction
		err := db.Chain().Transaction(func(tx *Chain) error {
			model := &TestModel{
				Name:      "transaction_test",
				Age:       25,
				CreatedAt: time.Now(),
			}
			result := tx.Table("tests").From(model).Save()
			return result.Error
		})
		assert.NoError(t, err)

		// Verify transaction was committed
		count, err := db.Chain().Table("tests").
			Where("name", define.OpEq, "transaction_test").
			Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count)

		// Test failed transaction
		err = db.Chain().Transaction(func(tx *Chain) error {
			model := &TestModel{
				Name:      "failed_transaction",
				Age:       25,
				CreatedAt: time.Now(),
			}
			result := tx.Table("tests").From(model).Save()
			if result.Error != nil {
				return result.Error
			}
			return errors.New("rollback test")
		})
		assert.Error(t, err)

		// Verify transaction was rolled back
		count, err = db.Chain().Table("tests").
			Where("name", define.OpEq, "failed_transaction").
			Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})
}
