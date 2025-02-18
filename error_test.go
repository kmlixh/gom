package gom

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/security"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

// ErrorTestUser represents a test user for error handling tests
type ErrorTestUser struct {
	ID        int64     `gom:"id,@,auto"`
	Name      string    `gom:"name,notnull"`
	Age       int       `gom:"age,notnull"`
	Email     string    `gom:"email,notnull"`
	Version   int       `gom:"version,notnull,default"`
	CreatedAt time.Time `gom:"created_at,notnull,default"`
	UpdatedAt time.Time `gom:"updated_at,notnull,default"`
}

func (t *ErrorTestUser) TableName() string {
	return "error_test_user"
}

func setupErrorTestDB(t *testing.T) *DB {
	config := testutils.DefaultMySQLConfig()
	opts := &define.DBOptions{
		MaxOpenConns:    1, // 只允许1个连接
		MaxIdleConns:    1,
		ConnMaxLifetime: 50 * time.Millisecond, // 非常短的连接生命周期
		ConnMaxIdleTime: 50 * time.Millisecond,
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

	// 确保表不存在
	result := db.Chain().Raw("DROP TABLE IF EXISTS error_test_user_details").Exec()
	if result.Error != nil {
		t.Errorf("删除关联表失败: %v", result.Error)
		return nil
	}

	result = db.Chain().Raw("DROP TABLE IF EXISTS error_test_user").Exec()
	if result.Error != nil {
		t.Errorf("删除旧表失败: %v", result.Error)
		return nil
	}

	// 创建测试表
	createTableSQL := `
	CREATE TABLE error_test_user (
		id BIGINT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(20) NOT NULL,
		age INT NOT NULL CHECK (age BETWEEN 0 AND 150),
		email VARCHAR(255),
		version INT NOT NULL DEFAULT 1,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY unique_email (email)
	)`
	result = db.Chain().Raw(createTableSQL).Exec()
	if result.Error != nil {
		t.Errorf("创建测试表失败: %v", result.Error)
		return nil
	}

	return db
}

func TestDatabaseConnectionErrors(t *testing.T) {
	// 测试无效的数据库连接
	_, err := Open("mysql", "invalid:invalid@tcp(localhost:3306)/nonexistent", nil)
	assert.Error(t, err, "应该返回连接错误")

	// 测试无效的数据库驱动
	_, err = Open("invalid_driver", "dsn", nil)
	assert.Error(t, err, "应该返回驱动错误")

	// 测试空DSN
	_, err = Open("mysql", "", nil)
	assert.Error(t, err, "应该返回DSN错误")
}

func TestDataValidationErrors(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	// 1. 测试字段长度超限
	longNameUser := &ErrorTestUser{
		Name:  "ThisNameIsTooLongForTheField",
		Age:   25,
		Email: "long@test.com",
	}
	result := db.Chain().Table("error_test_user").Save(longNameUser)
	assert.Error(t, result.Error, "应该返回字段长度错误")

	// 2. 测试唯一约束冲突
	user1 := &ErrorTestUser{
		Name:  "User1",
		Age:   25,
		Email: "same@test.com",
	}
	result = db.Chain().Table("error_test_user").Save(user1)
	assert.NoError(t, result.Error)

	user2 := &ErrorTestUser{
		Name:  "User2",
		Age:   30,
		Email: "same@test.com", // 相同的邮箱
	}
	result = db.Chain().Table("error_test_user").Save(user2)
	assert.Error(t, result.Error, "应该返回唯一约束错误")

	// 3. 测试CHECK约束违反
	invalidAgeUser := &ErrorTestUser{
		Name:  "Invalid",
		Age:   200, // 超出年龄限制
		Email: "invalid@test.com",
	}
	result = db.Chain().Table("error_test_user").Save(invalidAgeUser)
	assert.Error(t, result.Error, "应该返回CHECK约束错误")

	// 4. 测试必填字段缺失
	result = db.Chain().Table("error_test_user").Values(map[string]interface{}{
		"age":   25,
		"email": "noname@test.com",
		// 故意不提供必填的name字段
	}).Save()
	assert.Error(t, result.Error, "应该返回必填字段错误")
}

func TestQueryErrors(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	// 1. 测试无效的表名
	var users []ErrorTestUser
	result := db.Chain().Table("nonexistent_table").List(&users)
	assert.Error(t, result.Error, "应该返回表不存在错误")

	// 2. 测试无效的字段名
	result = db.Chain().Table("error_test_user").
		Where("nonexistent_field", define.OpEq, "value").
		List(&users)
	assert.Error(t, result.Error, "应该返回字段不存在错误")

	// 3. 测试无效的SQL语法
	result = db.Chain().Table("error_test_user").
		Fields("INVALID SQL SYNTAX").
		List(&users)
	assert.Error(t, result.Error, "应该返回SQL语法错误")

	// 4. 测试类型不匹配
	result = db.Chain().Table("error_test_user").
		Values(map[string]interface{}{
			"age": "not_a_number",
		}).
		Save()
	assert.Error(t, result.Error, "应该返回类型不匹配错误")
}

func TestTransactionErrors(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	// 1. 测试事务中的错误
	tx, err := db.BeginChain()
	assert.NoError(t, err)

	// 在事务中执行一些操作
	user := &ErrorTestUser{
		Name:  "TxUser",
		Age:   25,
		Email: "tx@test.com",
	}
	result := tx.Table("error_test_user").Save(user)
	assert.NoError(t, result.Error)

	// 制造错误（违反唯一约束）
	duplicateUser := &ErrorTestUser{
		Name:  "TxUser2",
		Age:   30,
		Email: "tx@test.com", // 相同的邮箱
	}
	result = tx.Table("error_test_user").Save(duplicateUser)
	assert.Error(t, result.Error, "应该返回唯一约束错误")

	// 回滚事务
	err = tx.Rollback()
	assert.NoError(t, err)

	// 验证数据已回滚
	var count int64
	count, err = db.Chain().Table("error_test_user").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count, "事务应该已回滚")

	// 2. 测试在已提交的事务上操作
	tx, err = db.BeginChain()
	assert.NoError(t, err)
	err = tx.Commit()
	assert.NoError(t, err)

	// 尝试在已提交的事务上操作
	result = tx.Table("error_test_user").Save(user)
	assert.Error(t, result.Error, "应该返回事务已结束错误")

	// 3. 测试在已回滚的事务上操作
	tx, err = db.BeginChain()
	assert.NoError(t, err)
	err = tx.Rollback()
	assert.NoError(t, err)

	result = tx.Table("error_test_user").Save(user)
	assert.Error(t, result.Error, "应该返回事务已结束错误")
}

func TestConcurrencyErrors(t *testing.T) {
	db := setupErrorTestDB(t)

	// 创建测试用户
	user := &ErrorTestUser{
		Name:    "Original",
		Age:     25,
		Email:   "original@test.com",
		Version: 1,
	}

	// 保存用户并确保获取到自增ID
	result := db.Chain().Table("error_test_user").Save(user)
	assert.NoError(t, result.Error)
	lastID, err := result.LastInsertId()
	assert.NoError(t, err)
	user.ID = lastID
	assert.Greater(t, user.ID, int64(0), "Expected user ID to be set after save")

	// 事务1读取数据
	var users1 []ErrorTestUser
	result = db.Chain().Table("error_test_user").
		Where("id", define.OpEq, user.ID).
		List(&users1)
	assert.NoError(t, result.Error)
	assert.Len(t, users1, 1)
	user1 := users1[0]

	// 事务2读取相同的数据
	var users2 []ErrorTestUser
	result = db.Chain().Table("error_test_user").
		Where("id", define.OpEq, user.ID).
		List(&users2)
	assert.NoError(t, result.Error)
	assert.Len(t, users2, 1)
	user2 := users2[0]

	// 事务1更新数据
	result = db.Chain().Table("error_test_user").Raw(
		"UPDATE error_test_user SET name = ?, version = ? WHERE id = ? AND version = ?",
		"Updated1", user1.Version+1, user.ID, user1.Version,
	).Exec()
	assert.NoError(t, result.Error)
	affected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected, "First update should affect 1 row")

	// 事务2尝试更新相同的数据（应该失败，因为版本已经改变）
	result = db.Chain().Table("error_test_user").Raw(
		"UPDATE error_test_user SET name = ?, version = ? WHERE id = ? AND version = ?",
		"Updated2", user2.Version+1, user.ID, user2.Version,
	).Exec()
	// 这里不应该返回错误，但应该显示没有行被更新
	assert.NoError(t, result.Error)
	affected, err = result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), affected, "Second update should affect 0 rows due to version mismatch")
}

func TestInvalidOperations(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	// 1. 测试无效的操作符
	var users []ErrorTestUser
	result := db.Chain().Table("error_test_user").
		Where("age", define.OpCustom, "INVALID_OPERATOR").
		List(&users)
	assert.Error(t, result.Error, "应该返回无效操作符错误")

	// 2. 测试无效的排序
	result = db.Chain().Table("error_test_user").
		OrderBy("age INVALID_DIRECTION").
		List(&users)
	assert.Error(t, result.Error, "应该返回无效排序错误")

	// 3. 测试无效的分组
	result = db.Chain().Table("error_test_user").
		GroupBy("COUNT(*)").
		List(&users)
	assert.Error(t, result.Error, "应该返回无效分组错误")

	// 4. 测试无效的聚合函数
	result = db.Chain().Table("error_test_user").
		Fields("INVALID_FUNCTION(age) as invalid_result").
		List(&users)
	assert.Error(t, result.Error, "应该返回无效函数错误")
}

func TestBatchOperationErrors(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	// 设置测试超时
	t.Parallel()
	testTimeout := 2 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// 使用包含无效数据的测试用例
	invalidData := []map[string]interface{}{
		{"name": "Valid1", "age": 25, "email": "valid1@test.com"},
		{"name": "Invalid", "age": "invalid", "email": "invalid@test.com"}, // 错误数据
		{"name": "Valid2", "age": 30, "email": "valid2@test.com"},
	}

	// 预期捕获特定错误
	_, err := db.Chain().WithContext(ctx).Table("error_test").BatchValues(invalidData).BatchInsert(2, true)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	// 验证错误类型
	var dbErr *define.DBError
	if !errors.As(err, &dbErr) {
		t.Fatalf("Unexpected error type: %T", err)
	}
}

func TestEncryptionErrors(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		t.Skip("跳过加密测试：数据库连接失败")
		return
	}
	defer db.Close()

	// 1. 测试加密配置错误
	chain := db.Chain().Table("error_test_user")
	chain.sensitiveFields = map[string]SensitiveOptions{
		"email": {
			Type: SensitiveEncrypted,
			Encryption: &security.EncryptionConfig{
				Algorithm: "invalid_algorithm",
				KeySource: "env",
				KeySourceConfig: map[string]string{
					"key_name": "TEST_ENCRYPTION_KEY",
				},
			},
		},
	}
	result := chain.Save(&ErrorTestUser{
		Name:  "EncryptTest",
		Age:   25,
		Email: "encrypt@test.com",
	})
	if result.Error == nil {
		t.Error("应该返回加密配置错误")
		return
	}
	if dbErr, ok := result.Error.(*security.DBError); ok {
		assert.Equal(t, security.ErrConfiguration, dbErr.Code, "错误代码应该是配置错误")
		assert.Contains(t, dbErr.Message, "invalid algorithm", "错误信息应该包含算法无效的提示")
	} else {
		t.Error("错误类型应该是 DBError")
	}

	// 2. 测试密钥获取错误
	chain = db.Chain().Table("error_test_user")
	chain.sensitiveFields = map[string]SensitiveOptions{
		"email": {
			Type: SensitiveEncrypted,
			Encryption: &security.EncryptionConfig{
				Algorithm: "AES256",
				KeySource: "invalid_source",
				KeySourceConfig: map[string]string{
					"key_name": "NONEXISTENT_KEY",
				},
			},
		},
	}
	result = chain.Save(&ErrorTestUser{
		Name:  "KeyTest",
		Age:   25,
		Email: "key@test.com",
	})
	if result.Error == nil {
		t.Error("应该返回密钥获取错误")
		return
	}
	if dbErr, ok := result.Error.(*security.DBError); ok {
		assert.Equal(t, security.ErrConfiguration, dbErr.Code, "错误代码应该是配置错误")
		assert.Contains(t, dbErr.Message, "unsupported key source", "错误信息应该包含密钥源无效的提示")
	} else {
		t.Error("错误类型应该是 DBError")
	}
}

func TestConnectionPoolTimeout(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	// Set extremely strict connection pool limits
	db.DB.SetMaxOpenConns(2) // Only allow 2 connections
	db.DB.SetMaxIdleConns(1)
	db.DB.SetConnMaxLifetime(1 * time.Microsecond)
	db.DB.SetConnMaxIdleTime(1 * time.Microsecond)

	// Create test table
	err := db.Chain().CreateTable(&ErrorTestUser{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "error_test_user")

	// Set a very low lock wait timeout
	_, err = db.DB.Exec("SET SESSION innodb_lock_wait_timeout = 1")
	assert.NoError(t, err)

	// Insert test data
	for i := 0; i < 100; i++ {
		user := &ErrorTestUser{
			Name:  fmt.Sprintf("User%d", i),
			Age:   25 + i,
			Email: fmt.Sprintf("timeout_test_user%d@test.com", i),
		}
		result := db.Chain().Table("error_test_user").Save(user)
		if result.Error != nil {
			t.Fatal(result.Error)
		}
	}

	// Create a long-running transaction that holds locks
	lockTx, err := db.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback()

	// Lock multiple rows with SELECT FOR UPDATE
	_, err = lockTx.Exec("SELECT * FROM error_test_user WHERE id <= 50 FOR UPDATE")
	if err != nil {
		t.Fatal(err)
	}

	// Run concurrent transactions with a more complex query
	var wg sync.WaitGroup
	errChan := make(chan error, 1000)

	// Create a context with a very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start goroutines with increasing delays
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Create an extremely short timeout for each query
			queryCtx, queryCancel := context.WithTimeout(ctx, 5*time.Millisecond)
			defer queryCancel()

			// Start a transaction with context
			tx, err := db.DB.BeginTx(queryCtx, nil)
			if err != nil {
				errChan <- err
				return
			}
			defer tx.Rollback()

			// Try to update locked rows with a complex query
			_, err = tx.ExecContext(queryCtx,
				`UPDATE error_test_user SET age = age + 1, 
				 email = CONCAT('updated_', email),
				 updated_at = NOW()
				 WHERE id <= 50 AND age > 30`)
			if err != nil {
				errChan <- err
				return
			}

			// Add a second query to increase resource usage
			_, err = tx.ExecContext(queryCtx,
				`SELECT * FROM error_test_user 
				 WHERE id <= 50 
				 AND age > 25 
				 FOR UPDATE`)
			if err != nil {
				errChan <- err
				return
			}

			if err = tx.Commit(); err != nil {
				errChan <- err
			}
		}(i)

		// Add small delays every 10 goroutines to create more connection pressure
		if i%10 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Wait for all goroutines or context timeout
	doneChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	select {
	case <-ctx.Done():
		t.Log("Test context timeout reached")
	case <-doneChan:
		t.Log("All goroutines completed")
	}

	// Release the locks
	err = lockTx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	close(errChan)

	// Check for timeout errors
	timeoutFound := false
	for err := range errChan {
		if err != nil && (strings.Contains(strings.ToLower(err.Error()), "timeout") ||
			strings.Contains(strings.ToLower(err.Error()), "context deadline exceeded") ||
			strings.Contains(strings.ToLower(err.Error()), "connection reset") ||
			strings.Contains(strings.ToLower(err.Error()), "broken pipe") ||
			strings.Contains(strings.ToLower(err.Error()), "bad connection") ||
			strings.Contains(strings.ToLower(err.Error()), "connection refused") ||
			strings.Contains(strings.ToLower(err.Error()), "too many connections") ||
			strings.Contains(strings.ToLower(err.Error()), "lock wait timeout")) {
			timeoutFound = true
			t.Logf("Found expected timeout error: %v", err)
			break
		}
	}

	assert.True(t, timeoutFound, "应该出现连接超时错误")
}

func TestConcurrentTransactionConflicts(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	// 准备测试数据
	user := &ErrorTestUser{
		Name:    "Original",
		Age:     25,
		Email:   "concurrent@test.com",
		Version: 1,
	}

	result := db.Chain().Table("error_test_user").Save(user)
	assert.NoError(t, result.Error)

	// 模拟并发更新冲突
	var wg sync.WaitGroup
	conflicts := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// 读取当前版本
			var users []ErrorTestUser
			result := db.Chain().Table("error_test_user").
				Where("email", define.OpEq, "concurrent@test.com").
				List(&users)

			if result.Error != nil || len(users) == 0 {
				conflicts <- false
				return
			}

			currentUser := users[0]

			// 尝试更新记录
			result = db.Chain().Table("error_test_user").Raw(
				"UPDATE error_test_user SET name = ?, version = ? WHERE email = ? AND version = ?",
				fmt.Sprintf("Updated%d", i),
				currentUser.Version+1,
				"concurrent@test.com",
				currentUser.Version,
			).Exec()

			// 检查是否有行被更新
			affected, _ := result.RowsAffected()
			conflicts <- affected == 0
		}(i)
	}

	wg.Wait()
	close(conflicts)

	// 统计并发冲突次数
	conflictCount := 0
	for conflict := range conflicts {
		if conflict {
			conflictCount++
		}
	}

	// 由于乐观锁，应该有一些更新失败
	assert.True(t, conflictCount > 0, "应该检测到并发更新冲突")

	// 验证最终版本号
	var finalUser ErrorTestUser
	result = db.Chain().Table("error_test_user").
		Where("email", define.OpEq, "concurrent@test.com").
		First(&finalUser)
	assert.NoError(t, result.Error)
	assert.True(t, finalUser.Version > 1, "版本号应该已更新")
}

func TestTimeoutHandling(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// 明确设置测试数据
	data := []map[string]interface{}{
		{"name": "timeout_test", "age": 30, "email": "timeout@test.com"},
	}

	_, err := db.Chain().
		WithContext(ctx).
		Table("tests").
		BatchValues(data).
		BatchInsert(10, true) // 启用并发

	// 验证具体错误类型
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Expected context deadline exceeded, got: %v", err)
	}
}
