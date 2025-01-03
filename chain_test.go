package gom

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	ErrTest = errors.New("test error")
	testDSN = "user:password@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=True"
)

// TestModel 测试用的模型结构
type TestModel struct {
	ID        int64     `gom:"id,@"`
	Name      string    `gom:"name"`
	Age       int       `gom:"age"`
	CreatedAt time.Time `gom:"created_at"`
}

func (m *TestModel) TableName() string {
	return "test_models"
}

// setupTestDB 设置测试数据库连接
func setupTestDB(t *testing.T) *DB {
	db, err := Open("mysql", testDSN, true)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// TestTransaction 测试事务处理
func TestTransaction(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 测试成功的事务
	err = db.Chain().Transaction(func(tx *Chain) error {
		model := &TestModel{
			Name:      "Test1",
			Age:       25,
			CreatedAt: time.Now(),
		}
		_, err := tx.Table("test_models").Values(map[string]interface{}{
			"name":       model.Name,
			"age":        model.Age,
			"created_at": model.CreatedAt,
		}).Save()
		return err
	})
	assert.NoError(t, err)

	// 测试失败的事务
	err = db.Chain().Transaction(func(tx *Chain) error {
		model := &TestModel{
			Name:      "Test2",
			Age:       30,
			CreatedAt: time.Now(),
		}
		_, err := tx.Table("test_models").Values(map[string]interface{}{
			"name":       model.Name,
			"age":        model.Age,
			"created_at": model.CreatedAt,
		}).Save()
		if err != nil {
			return err
		}
		return ErrTest // 返回错误触发回滚
	})
	assert.Error(t, err)
}

// TestBatchInsert 测试批量插入
func TestBatchInsert(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 准备测试数据
	batchValues := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		batchValues = append(batchValues, map[string]interface{}{
			"name":       fmt.Sprintf("User%d", i),
			"age":        20 + i%10,
			"created_at": time.Now(),
		})
	}

	// 执行批量插入
	affected, err := db.Chain().Table("test_models").BatchValues(batchValues).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// 验证插入结果
	count, err := db.Chain().Table("test_models").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), count)
}

// TestCRUD 测试基本的CRUD操作
func TestCRUD(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 测试插入
	model := &TestModel{
		Name:      "CRUD Test",
		Age:       25,
		CreatedAt: time.Now(),
	}
	result, err := db.Chain().Table("test_models").Values(map[string]interface{}{
		"name":       model.Name,
		"age":        model.Age,
		"created_at": model.CreatedAt,
	}).Save()
	assert.NoError(t, err)
	assert.True(t, result.ID > 0)

	// 测试查询
	var found TestModel
	err = db.Chain().Table("test_models").Eq("id", result.ID).First(&found)
	assert.NoError(t, err)
	assert.Equal(t, model.Name, found.Name)

	// 测试更新
	updateResult, err := db.Chain().Table("test_models").
		Eq("id", result.ID).
		Set("name", "Updated Name").
		Set("age", 26).
		Update()
	assert.NoError(t, err)
	affected, _ := updateResult.RowsAffected()
	assert.Equal(t, int64(1), affected)

	// 测试删除
	deleteResult, err := db.Chain().Table("test_models").Eq("id", result.ID).Delete()
	assert.NoError(t, err)
	affected, _ = deleteResult.RowsAffected()
	assert.Equal(t, int64(1), affected)
}

// TestConditions 测试条件查询
func TestConditions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 准备测试数据
	names := []string{"Alice", "Bob", "Charlie"}
	for _, name := range names {
		_, err := db.Chain().Table("test_models").Values(map[string]interface{}{
			"name":       name,
			"age":        25,
			"created_at": time.Now(),
		}).Save()
		assert.NoError(t, err)
	}

	// 测试 Like 查询
	var results []TestModel
	err := db.Chain().Table("test_models").
		Like("name", "A%").
		Into(&results)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, "Alice", results[0].Name)

	// 测试 In 查询
	results = nil
	err = db.Chain().Table("test_models").
		In("name", []string{"Alice", "Bob"}).
		Into(&results)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))

	// 测试 Between 查询
	results = nil
	err = db.Chain().Table("test_models").
		Between("age", 20, 30).
		Into(&results)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
}
