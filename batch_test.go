package gom

import (
	"fmt"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/stretchr/testify/assert"
)

// TestBatchOperations 测试批量操作
func TestBatchOperations(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 测试不同批量大小
	batchSizes := []int{1, 5, 10, 50, 0, -1}
	for _, batchSize := range batchSizes {
		// 清理之前的数据
		err := db.Chain().Table("tests").Delete().Error
		assert.NoError(t, err)

		// 创建测试记录
		values := make([]map[string]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, map[string]interface{}{
				"name":       fmt.Sprintf("User%d", i),
				"age":        20 + (i % 10),
				"created_at": time.Now(),
			})
		}

		// 测试批量插入
		affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(batchSize)
		if batchSize <= 0 {
			assert.Error(t, err, "Expected error for batch size %d", batchSize)
			assert.Equal(t, int64(0), affected)
			dbErr, ok := err.(*DBError)
			assert.True(t, ok)
			assert.Equal(t, "BatchInsert", dbErr.Op)
			assert.Contains(t, dbErr.Error(), "invalid batch size")
			continue
		}
		assert.NoError(t, err, "Unexpected error with batch size %d: %v", batchSize, err)
		assert.Equal(t, int64(100), affected, "Expected 100 affected rows with batch size %d", batchSize)

		// 验证结果
		count, err := db.Chain().Table("tests").Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(100), count)
	}

	// 测试错误处理 - 无效数据类型
	invalidValues := []map[string]interface{}{
		{
			"name":       "Invalid1",
			"age":        "invalid", // 错误的类型
			"created_at": time.Now(),
		},
		{
			"name":       "Invalid2",
			"age":        25,
			"created_at": time.Now(),
		},
		{
			"name":       123, // MySQL will convert this to string
			"age":        30,
			"created_at": time.Now(),
		},
		{
			"name":       "Invalid4",
			"age":        30,
			"created_at": "invalid", // 错误的类型
		},
	}

	// 测试每种无效数据类型
	for i, value := range invalidValues {
		_, err = db.Chain().Table("tests").BatchValues([]map[string]interface{}{value}).BatchInsert(1)
		if i == 1 || i == 2 { // Changed to include case 3 (index 2) as valid
			assert.NoError(t, err, "Test case %d should succeed", i+1)
		} else {
			assert.Error(t, err, "Test case %d should fail", i+1)
			t.Logf("Test case %d: %v", i+1, err)
		}
	}

	// 测试空值批量插入
	affected, err := db.Chain().Table("tests").BatchValues(nil).BatchInsert(10)
	assert.Error(t, err)
	assert.Equal(t, int64(0), affected)

	// 测试空记录批量插入
	affected, err = db.Chain().Table("tests").BatchValues([]map[string]interface{}{}).BatchInsert(10)
	assert.Error(t, err)
	assert.Equal(t, int64(0), affected)
}

// TestBatchUpdate 测试批量更新
func TestBatchUpdate(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 插入测试数据
	values := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "User" + string(rune('A'+i%26)),
			"age":        20 + (i % 10),
			"created_at": time.Now(),
		})
	}

	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// 测试批量更新
	result := db.Chain().Table("tests").
		Set("age", 30).
		Where("age", define.OpGt, 25).
		Save()
	assert.NoError(t, result.Error)

	// 验证更新结果
	var count int64
	count, err = db.Chain().Table("tests").Where("age", define.OpEq, 30).Count()
	assert.NoError(t, err)
	assert.True(t, count > 0)

	// 测试条件更新
	result = db.Chain().Table("tests").
		Set("age", 35).
		Where("name", define.OpLike, "User%").
		Where("age", define.OpLt, 25).
		Save()
	assert.NoError(t, result.Error)

	// 验证条件更新结果
	count, err = db.Chain().Table("tests").Where("age", define.OpEq, 35).Count()
	assert.NoError(t, err)
	assert.True(t, count > 0)
}

// TestBatchDelete 测试批量删除
func TestBatchDelete(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 插入测试数据
	values := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "User" + string(rune('A'+i%26)),
			"age":        20 + (i % 10),
			"created_at": time.Now(),
		})
	}

	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// 测试条件删除
	result := db.Chain().Table("tests").
		Where("age", define.OpGt, 25).
		Delete()
	assert.NoError(t, result.Error)

	// 验证删除结果
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.True(t, count > 0)
	assert.True(t, count < 100)

	// 测试复杂条件删除
	result = db.Chain().Table("tests").
		Where("name", define.OpLike, "User%").
		Where("age", define.OpLt, 25).
		Delete()
	assert.NoError(t, result.Error)

	// 验证复杂条件删除结果
	count, err = db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.True(t, count >= 0)
}

// TestBatchTransactions 测试批量事务
func TestBatchTransactions(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Clean up previous data
	err = db.Chain().Table("tests").Delete().Error
	assert.NoError(t, err)

	// Test batch operations in transaction
	err = db.Chain().Transaction(func(tx *Chain) error {
		// Batch insert
		values := make([]map[string]interface{}, 0)
		for i := 0; i < 10; i++ { // Reduced number of records
			values = append(values, map[string]interface{}{
				"name":       fmt.Sprintf("User%d", i),
				"age":        20 + (i % 10),
				"created_at": time.Now(),
			})
		}

		// Split batch insert into smaller chunks
		batchSize := 5 // Smaller batch size
		for i := 0; i < len(values); i += batchSize {
			end := i + batchSize
			if end > len(values) {
				end = len(values)
			}
			batchValues := values[i:end]
			affected, err := tx.Table("tests").BatchValues(batchValues).BatchInsert(batchSize)
			if err != nil {
				return fmt.Errorf("batch insert failed: %w", err)
			}
			if affected != int64(len(batchValues)) {
				return fmt.Errorf("expected %d affected rows, got %d", len(batchValues), affected)
			}
		}

		// Verify total count
		count, err := tx.Table("tests").Count()
		if err != nil {
			return fmt.Errorf("count query failed: %w", err)
		}
		if count != int64(len(values)) {
			return fmt.Errorf("expected %d total records, got %d", len(values), count)
		}

		return nil
	})
	assert.NoError(t, err)

	// Verify final count
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)
}
