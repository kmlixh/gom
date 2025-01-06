package gom

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestTransactionSavepoints 测试事务保存点操作
func TestTransactionSavepoints(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Start a transaction
	txChain, err := db.Chain().Begin()
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
	assert.NoError(t, qr.Error())
	assert.Equal(t, 2, len(models))
	assert.Equal(t, "First", models[0].Name)
	assert.Equal(t, "Third", models[1].Name)
}

// TestTransactionIsolationLevels 测试事务隔离级别
func TestTransactionIsolationLevels(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	isolationLevels := []sql.IsolationLevel{
		sql.LevelDefault,
		sql.LevelReadUncommitted,
		sql.LevelReadCommitted,
		sql.LevelWriteCommitted,
		sql.LevelRepeatableRead,
		sql.LevelSnapshot,
		sql.LevelSerializable,
		sql.LevelLinearizable,
	}

	for _, level := range isolationLevels {
		chain := db.Chain().SetIsolationLevel(level)
		txChain, err := chain.Begin()

		if err != nil {
			// Some isolation levels might not be supported by the database
			continue
		}

		model := &TestModel{
			Name:      "Test",
			Age:       25,
			CreatedAt: time.Now(),
		}

		result := txChain.Table("tests").From(model).Save()
		if result.Error != nil {
			txChain.Rollback()
			continue
		}

		err = txChain.Commit()
		if err != nil {
			txChain.Rollback()
			continue
		}

		// Verify the record was inserted
		count, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count)

		// Clean up for next iteration
		deleteResult := db.Chain().Table("tests").Delete()
		assert.NoError(t, deleteResult.Error)
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
	assert.Contains(t, err.Error(), "no transaction to rollback")

	// 测试提交已回滚的事务
	err = chain.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction to commit")
}

// TestNestedTransactions 测试嵌套事务
func TestNestedTransactions(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 开始外层事务
	outerChain, err := db.Chain().Begin()
	assert.NoError(t, err)

	// 尝试开始嵌套事务
	_, err = outerChain.Begin()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction already started")

	// 回滚外层事务
	err = outerChain.Rollback()
	assert.NoError(t, err)
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

	// Test transaction error handling
	err = db.Chain().Transaction(func(tx *Chain) error {
		// Insert test data
		testData := TestModel{
			Name:      "Test",
			Age:       25,
			CreatedAt: time.Now(),
		}
		result := tx.Table("tests").From(&testData).Save()
		assert.NoError(t, result.Error)

		// Return error to trigger rollback
		return errors.New("test error")
	})

	// Verify error was returned
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")

	// Verify rollback occurred
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}
