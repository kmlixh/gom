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

// TestModel represents a test model for database operations
type TestModel struct {
	ID        int64     `gom:"id"`
	Name      string    `gom:"name"`
	Age       int       `gom:"age"`
	CreatedAt time.Time `gom:"created_at"`
}

// setupDB creates a new database connection with the given driver and DSN
func setupDB(driver, dsn string) *DB {
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		Debug:           true,
	}
	db, err := Open(driver, dsn, opts)
	if err != nil {
		return nil
	}
	return db
}

func setupMySQLDB(t *testing.T) *DB {
	db := setupDB("mysql", "root:123456@tcp(10.0.1.5:3306)/test?charset=utf8mb4&parseTime=True")
	if db == nil {
		t.Logf("Failed to connect to MySQL")
	}
	return db
}

func setupPostgreSQLDB(t *testing.T) *DB {
	db := setupDB("postgres", "host=10.0.1.5 port=5432 user=postgres password=123456 dbname=test sslmode=disable")
	if db == nil {
		t.Logf("Failed to connect to PostgreSQL")
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
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runChainOperationsTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupPostgreSQLDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runChainOperationsTest(t, db)
	})
}

func runChainOperationsTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

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
		err := db.Chain().Table("tests").From(model).Save().Error
		assert.NoError(t, err)

		// Test First
		var firstResult TestModel
		err = db.Chain().Table("tests").First(&firstResult).Error()
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
		err = db.Chain().Table("tests").
			Where("name", define.OpEq, "test").
			Set("age", 30).
			Save().Error
		assert.NoError(t, err)

		// Verify update
		var updatedResult TestModel
		err = db.Chain().Table("tests").First(&updatedResult).Error()
		assert.NoError(t, err)
		assert.Equal(t, 30, updatedResult.Age)

		// Test Delete
		err = db.Chain().Table("tests").
			Where("name", define.OpEq, "test").
			Delete().Error
		assert.NoError(t, err)

		// Verify delete
		count, err = db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("Chain Transaction", func(t *testing.T) {
		// Test successful transaction
		err := db.Chain().Transaction(func(tx *Chain) error {
			model := &TestModel{
				Name:      "transaction_test",
				Age:       25,
				CreatedAt: time.Now(),
			}
			return tx.Table("tests").From(model).Save().Error
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
			err := tx.Table("tests").From(model).Save().Error
			if err != nil {
				return err
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
