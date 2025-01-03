package gom

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	ErrTest = errors.New("test error")
)

// TestModel 测试用的模型结构
type TestModel struct {
	ID        int64     `gom:"id,@,auto"`
	Name      string    `gom:"name,notnull"`
	Age       int       `gom:"age,notnull"`
	CreatedAt time.Time `gom:"created_at,notnull,default"`
}

func (m *TestModel) TableName() string {
	return "tests"
}

// setupTestDB 设置测试数据库连接
func setupTestDB(t *testing.T) *DB {
	db, err := Open("mysql", testutils.TestMySQLDSN, true)
	if err != nil {
		t.Fatal(err)
	}

	// 清理测试表
	_, err = db.DB.Exec("DROP TABLE IF EXISTS tests")
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
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 测试事务
	err = db.Chain().Transaction(func(tx *Chain) error {
		model := &TestModel{
			Name:      "Test1",
			Age:       25,
			CreatedAt: time.Now(),
		}
		result := tx.Table("tests").From(model).Values(map[string]interface{}{
			"name":       model.Name,
			"age":        model.Age,
			"created_at": model.CreatedAt,
		}).Save()
		if result.Error != nil {
			t.Logf("Transaction failed: %v", result.Error)
			return result.Error
		}
		t.Logf("Transaction succeeded with ID: %v", result.ID)
		return nil
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}
}

// TestBatchInsert 测试批量插入
func TestBatchInsert(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 测试空批量插入
	affected, err := db.Chain().Table("tests").BatchInsert(10)
	assert.Error(t, err)
	assert.Equal(t, int64(0), affected)
	dbErr, ok := err.(*DBError)
	assert.True(t, ok)
	assert.Equal(t, "BatchInsert", dbErr.Op)
	assert.Contains(t, dbErr.Error(), "no values to insert")

	// 准备测试数据
	batchValues := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		batchValues = append(batchValues, map[string]interface{}{
			"name":       fmt.Sprintf("User%d", i),
			"age":        20 + i%10,
			"created_at": time.Now(),
		})
	}

	// 测试批量插入
	affected, err = db.Chain().Table("tests").BatchValues(batchValues).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// 验证插入结果
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), count)

	// 测试无效的批量大小
	affected, err = db.Chain().Table("tests").BatchValues(batchValues).BatchInsert(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)
}

// TestCRUD 测试基本的CRUD操作
func TestCRUD(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 测试插入空模型
	result := db.Chain().Table("tests").Save()
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "no fields to update")

	// 测试插入
	model := &TestModel{
		Name:      "CRUD Test",
		Age:       25,
		CreatedAt: time.Now(),
	}
	result = db.Chain().Table("tests").From(model).Save()
	assert.NoError(t, result.Error)
	assert.True(t, result.ID > 0)
	model.ID = result.ID

	// 测试查询
	var found []TestModel
	qr := db.Chain().Table("tests").Eq("id", model.ID).List(&found)
	assert.NoError(t, qr.Error())
	assert.Equal(t, 1, len(found))
	assert.Equal(t, model.Name, found[0].Name)

	// 测试更新
	model.Name = "Updated Name"
	model.Age = 26
	updateResult := db.Chain().Table("tests").Eq("id", model.ID).From(model).Save()
	assert.NoError(t, updateResult.Error)
	assert.Equal(t, int64(1), updateResult.Affected)

	// 测试更新不存在的记录
	updateResult = db.Chain().Table("tests").Eq("id", 999999).From(model).Save()
	assert.NoError(t, updateResult.Error)
	assert.Equal(t, int64(0), updateResult.Affected)

	// 测试删除
	deleteResult := db.Chain().Table("tests").Eq("id", model.ID).Delete()
	assert.NoError(t, deleteResult.Error)
	assert.Equal(t, int64(1), deleteResult.Affected)

	// 测试删除不存在的记录
	deleteResult = db.Chain().Table("tests").Eq("id", 999999).Delete()
	assert.NoError(t, deleteResult.Error)
	assert.Equal(t, int64(0), deleteResult.Affected)
}

// TestConditions 测试条件查询
func TestConditions(t *testing.T) {
	// Setup test data
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create table if not exists
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// Delete all existing records
	deleteResult := db.Chain().Table("tests").Delete()
	assert.NoError(t, deleteResult.Error)

	// Insert test data
	testData := []TestModel{
		{Name: "Alice", Age: 20},
		{Name: "Bob", Age: 25},
		{Name: "Charlie", Age: 30},
		{Name: "David", Age: 35},
		{Name: "Eve", Age: 40},
	}

	for _, data := range testData {
		saveResult := db.Chain().Table("tests").From(&data).Save()
		assert.Nil(t, saveResult.Error)
	}

	// Test basic conditions
	var results []TestModel
	qr := db.Chain().Table("tests").
		Where2(define.Eq("name", "Alice")).
		List(&results)
	assert.NoError(t, qr.Error())
	assert.Equal(t, 1, len(results))
	if len(results) > 0 {
		assert.Equal(t, "Alice", results[0].Name)
	}

	// Test AND condition
	results = nil
	qr = db.Chain().Table("tests").
		Where2(define.Gt("age", 25)).
		Where2(define.Lt("age", 40)).
		List(&results)
	assert.NoError(t, qr.Error())
	assert.Equal(t, 2, len(results))
	if len(results) >= 2 {
		names := []string{results[0].Name, results[1].Name}
		assert.Contains(t, names, "Charlie")
		assert.Contains(t, names, "David")
	}

	// Test OR condition
	results = nil
	qr = db.Chain().Table("tests").
		Where2(define.Eq("name", "Alice")).
		Or(define.Eq("name", "Eve")).
		List(&results)
	assert.NoError(t, qr.Error())
	assert.Equal(t, 2, len(results))
	if len(results) >= 2 {
		names := []string{results[0].Name, results[1].Name}
		assert.Contains(t, names, "Alice")
		assert.Contains(t, names, "Eve")
	}

	// Test complex conditions
	results = nil
	qr = db.Chain().Table("tests").
		Where2(define.Lt("age", 30)).
		Or(define.Gt("age", 35)).
		List(&results)
	assert.NoError(t, qr.Error())
	assert.Equal(t, 3, len(results))
	if len(results) >= 3 {
		names := []string{results[0].Name, results[1].Name, results[2].Name}
		assert.Contains(t, names, "Alice")
		assert.Contains(t, names, "Bob")
		assert.Contains(t, names, "Eve")
	}

	// Test order by
	results = nil
	qr = db.Chain().Table("tests").
		OrderByDesc("age").
		List(&results)
	assert.NoError(t, qr.Error())
	assert.Equal(t, 5, len(results))
	if len(results) >= 5 {
		assert.Equal(t, 40, results[0].Age)
		assert.Equal(t, 20, results[len(results)-1].Age)
	}
}

// TestPageInfo 测试分页查询
func TestPageInfo(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 准备测试数据
	for i := 0; i < 25; i++ {
		model := &TestModel{
			Name:      fmt.Sprintf("User%d", i),
			Age:       20 + i%10,
			CreatedAt: time.Now(),
		}
		result := db.Chain().Table("tests").From(model).Save()
		assert.NoError(t, result.Error)
	}

	// 测试第一页
	pageInfo, err := db.Chain().Table("tests").
		OrderBy("id").
		Page(1, 10).
		PageInfo(&TestModel{})
	assert.NoError(t, err)
	assert.Equal(t, 1, pageInfo.PageNum)
	assert.Equal(t, 10, pageInfo.PageSize)
	assert.Equal(t, int64(25), pageInfo.Total)
	assert.Equal(t, 3, pageInfo.Pages)
	assert.False(t, pageInfo.HasPrev)
	assert.True(t, pageInfo.HasNext)
	assert.True(t, pageInfo.IsFirstPage)
	assert.False(t, pageInfo.IsLastPage)

	// 测试中间页
	pageInfo, err = db.Chain().Table("tests").
		OrderBy("id").
		Page(2, 10).
		PageInfo(&TestModel{})
	assert.NoError(t, err)
	assert.Equal(t, 2, pageInfo.PageNum)
	assert.Equal(t, 10, pageInfo.PageSize)
	assert.Equal(t, int64(25), pageInfo.Total)
	assert.Equal(t, 3, pageInfo.Pages)
	assert.True(t, pageInfo.HasPrev)
	assert.True(t, pageInfo.HasNext)
	assert.False(t, pageInfo.IsFirstPage)
	assert.False(t, pageInfo.IsLastPage)

	// 测试最后一页
	pageInfo, err = db.Chain().Table("tests").
		OrderBy("id").
		Page(3, 10).
		PageInfo(&TestModel{})
	assert.NoError(t, err)
	assert.Equal(t, 3, pageInfo.PageNum)
	assert.Equal(t, 10, pageInfo.PageSize)
	assert.Equal(t, int64(25), pageInfo.Total)
	assert.Equal(t, 3, pageInfo.Pages)
	assert.True(t, pageInfo.HasPrev)
	assert.False(t, pageInfo.HasNext)
	assert.False(t, pageInfo.IsFirstPage)
	assert.True(t, pageInfo.IsLastPage)

	// 测试无效页码
	pageInfo, err = db.Chain().Table("tests").
		OrderBy("id").
		Page(0, 10).
		PageInfo(&TestModel{})
	assert.NoError(t, err)
	assert.Equal(t, 1, pageInfo.PageNum)

	// 测试无效页大小
	pageInfo, err = db.Chain().Table("tests").
		OrderBy("id").
		Page(1, 0).
		PageInfo(&TestModel{})
	assert.NoError(t, err)
	assert.Equal(t, 10, pageInfo.PageSize)
}

// TestAggregation 测试聚合函数
func TestAggregation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)

	// 准备测试数据
	ages := []int{20, 25, 30, 35, 40}
	for i, age := range ages {
		model := &TestModel{
			Name:      fmt.Sprintf("User%d", i),
			Age:       age,
			CreatedAt: time.Now(),
		}
		result := db.Chain().Table("tests").From(model).Save()
		assert.NoError(t, result.Error)
	}

	// 测试 Count
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)

	// 测试 Sum
	sum, err := db.Chain().Table("tests").Sum("age")
	assert.NoError(t, err)
	assert.Equal(t, float64(150), sum)

	// 测试 Avg
	avg, err := db.Chain().Table("tests").Avg("age")
	assert.NoError(t, err)
	assert.Equal(t, float64(30), avg)

	// 测试空表的聚合函数
	deleteResult := db.Chain().Table("tests").Delete()
	assert.NoError(t, deleteResult.Error)

	count, err = db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	sum, err = db.Chain().Table("tests").Sum("age")
	assert.NoError(t, err)
	assert.Equal(t, float64(0), sum)

	avg, err = db.Chain().Table("tests").Avg("age")
	assert.NoError(t, err)
	assert.Equal(t, float64(0), avg)
}

func cleanupTestDB(t *testing.T, db *DB) {
	// Cleanup function to be called after tests
	t.Cleanup(func() {
		db.Close()
	})
}
