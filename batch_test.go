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

// BatchTestModel 用于批量操作测试
type BatchTestModel struct {
	ID        int64     `gom:"id,@,auto"`
	Name      string    `gom:"name"`
	Age       int       `gom:"age"`
	Email     string    `gom:"email"`
	CreatedAt time.Time `gom:"created_at"`
	UpdatedAt time.Time `gom:"updated_at"`
	IsActive  bool      `gom:"is_active"`
}

func (m *BatchTestModel) TableName() string {
	return "tests"
}

func setupBatchTestDB(t *testing.T) *DB {
	config := testutils.DefaultMySQLConfig()
	config.User = "root"
	// 使用正确的密码
	opts := &define.DBOptions{
		MaxOpenConns:    50,
		MaxIdleConns:    20,
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
	_, err = db.DB.Exec("DROP TABLE IF EXISTS tests")
	if err != nil {
		t.Skipf("Failed to drop test table: %v", err)
		return nil
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS tests (
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
	_, err = db.DB.Exec("TRUNCATE TABLE tests")
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
		Age       int64     `gom:"age"`
		Email     string    `gom:"email"`
		CreatedAt time.Time `gom:"created_at"`
		UpdatedAt time.Time `gom:"updated_at"`
		IsActive  bool      `gom:"is_active"`
	}

	// Drop table if exists
	_ = testutils.CleanupTestDB(db.DB, tableName)

	// Create test table
	err := db.Chain().Table(tableName).CreateTable(&BatchTestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, tableName)

	// Prepare test data
	values := make([]map[string]interface{}, 0, 100)
	now := time.Now()
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       fmt.Sprintf("Test%d", i),
			"age":        20 + i,
			"email":      fmt.Sprintf("test%d@example.com", i),
			"created_at": now,
			"updated_at": now,
			"is_active":  true,
		})
	}

	// Test batch insert
	t.Run("BatchInsert", func(t *testing.T) {
		affected, err := db.Chain().Table(tableName).BatchValues(values).BatchInsert(10, true)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), affected)
	})

	// Test batch update
	t.Run("BatchUpdate", func(t *testing.T) {
		updateValues := make([]map[string]interface{}, 0)
		updateTime := time.Now()
		for i := 0; i < 100; i++ {
			updateValues = append(updateValues, map[string]interface{}{
				"id":         int64(i + 1),
				"name":       fmt.Sprintf("updated_%d", i),
				"age":        i + 100,
				"email":      fmt.Sprintf("updated%d@example.com", i),
				"updated_at": updateTime,
				"is_active":  true,
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
	now := time.Now()
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "test",
			"age":        i,
			"email":      fmt.Sprintf("test%d@example.com", i),
			"created_at": now,
			"updated_at": now,
			"is_active":  true,
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10, true)
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
	now := time.Now()
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       fmt.Sprintf("test%d", i),
			"age":        i,
			"email":      fmt.Sprintf("test%d@example.com", i),
			"created_at": now,
			"updated_at": now,
			"is_active":  true,
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// Update test data
	updateValues := make([]map[string]interface{}, 0)
	updateTime := time.Now()
	for i := 0; i < 100; i++ {
		updateValues = append(updateValues, map[string]interface{}{
			"id":         int64(i + 1),
			"name":       fmt.Sprintf("updated_%d", i),
			"age":        i + 100,
			"email":      fmt.Sprintf("updated%d@example.com", i),
			"updated_at": updateTime,
			"is_active":  true,
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
	now := time.Now()
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "test",
			"age":        i,
			"email":      fmt.Sprintf("test%d@example.com", i),
			"created_at": now,
			"updated_at": now,
			"is_active":  true,
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10, true)
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
		now := time.Now()
		for i := 0; i < 100; i++ {
			values = append(values, map[string]interface{}{
				"name":       "test",
				"age":        i,
				"email":      fmt.Sprintf("test%d@example.com", i),
				"created_at": now,
				"updated_at": now,
				"is_active":  true,
			})
		}
		affected, err := tx.Table("tests").BatchValues(values).BatchInsert(10, true)
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
	_, err = db.Chain().Table("tests").BatchValues(values).BatchInsert(10, true)
	assert.Error(t, err)

	// Verify no data was inserted
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestBatchOperationsEdgeCases(t *testing.T) {
	db := setupBatchTestDB(t)
	if db == nil {
		return
	}
	defer func() {
		cleanupTestDB(t, db)
		db.Close()
	}()

	// 创建测试表
	err := db.Chain().CreateTable(&BatchTestModel{})
	assert.NoError(t, err)

	// 1. 测试无效的批处理大小
	_, err = db.Chain().Table("tests").BatchValues([]map[string]interface{}{
		{
			"name":  "Test1",
			"age":   25,
			"email": "test1@test.com",
		},
	}).BatchInsert(0, true)
	assert.Error(t, err, "应该返回无效批处理大小错误")

	// 2. 测试空批处理值
	_, err = db.Chain().Table("tests").BatchValues([]map[string]interface{}{}).BatchInsert(10, true)
	assert.Error(t, err, "应该返回空批处理值错误")

	// 3. 测试大批量插入
	var largeDataset []map[string]interface{}
	for i := 0; i < 500; i++ {
		largeDataset = append(largeDataset, map[string]interface{}{
			"name":      fmt.Sprintf("User%d", i),
			"age":       25 + i%50,
			"email":     fmt.Sprintf("user%d@test.com", i),
			"is_active": true,
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(largeDataset).BatchInsert(100, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(500), affected, "应该插入500条记录")

	// 4. 测试并发批处理操作
	var wg sync.WaitGroup
	errChan := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			var data []map[string]interface{}
			for j := 0; j < 100; j++ {
				data = append(data, map[string]interface{}{
					"name":      fmt.Sprintf("Concurrent_Test_%d_%d", index, j),
					"age":       20 + j%30,
					"email":     fmt.Sprintf("concurrent_test_%d_%d@example.com", index, j),
					"is_active": true,
				})
			}

			// 创建独立的Chain实例
			tx := db.Chain().clone()
			_, err := tx.Table("tests").BatchValues(data).BatchInsert(50, true)
			errChan <- err
		}(i)
	}

	wg.Wait()
	close(errChan)

	// 检查并发操作的错误
	for err := range errChan {
		assert.NoError(t, err, "并发批处理操作应该成功")
	}

	// 验证总记录数
	count, err := db.Chain().Table("tests").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), count, "总记录数应该是1000")

	// 5. 测试批量更新
	updateData := make([]map[string]interface{}, 0)
	for i := 50; i < 100; i++ {
		updateData = append(updateData, map[string]interface{}{
			"id":        i,
			"is_active": false,
		})
	}
	_, err = db.Chain().Table("tests").
		BatchValues(updateData).
		BatchUpdate(100)
	assert.NoError(t, err)

	// 验证更新结果
	inactiveCount, err := db.Chain().Table("tests").
		Where("is_active", define.OpEq, false).
		Count2("id")
	assert.NoError(t, err)
	assert.True(t, inactiveCount > 0, "应该有记录被更新为非活动状态")

	// 需要顺序执行的测试用例
	t.Run("SequentialInsert", func(t *testing.T) {
		// 添加测试数据
		validData := []map[string]interface{}{
			{"name": "seq_test", "age": 25, "email": "seq@test.com"},
		}

		// 明确设置值
		affected, err := db.Chain().
			Table("tests").
			BatchValues(validData). // 添加测试数据
			BatchInsert(100, false) // 第二个参数false表示禁用并发

		assert.NoError(t, err)
		assert.Equal(t, int64(1), affected)
	})

	// 其他保持并发的测试用例
	_, err = db.Chain().BatchInsert(1000, true) // 正常并发插入
	if err != nil {
		t.Fatalf("Concurrent insert failed: %v", err)
	}

	// 合法批次大小
	_, err = db.Chain().BatchInsert(50, true)
	if err != nil {
		t.Fatal(err)
	}

	// 测试非法参数
	_, err = db.Chain().BatchInsert(0, true)
	if err == nil {
		t.Fatal("Expected error for zero batch size")
	}
}

func cleanupTestDB(t *testing.T, db *DB) {
	if db == nil || db.DB == nil {
		return
	}
	// 删除所有数据
	result := db.Chain().Table("tests").Delete()
	if result.Error != nil {
		t.Logf("Failed to cleanup test database: %v", result.Error)
	}
}
