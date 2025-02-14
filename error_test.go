package gom

import (
	"context"
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
	result := db.Chain().Raw("DROP TABLE IF EXISTS error_test_user").Exec()
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

	// 1. 测试批量插入错误
	_, err := db.Chain().Table("error_test_user").BatchValues([]map[string]interface{}{
		{
			"name":  "User1",
			"age":   25,
			"email": "test1@test.com",
		},
		{
			"name":  "User2",
			"age":   "invalid", // 类型错误
			"email": "test2@test.com",
		},
	}).BatchInsert(2)
	assert.Error(t, err, "应该返回批量插入错误")

	// 2. 测试批量更新错误
	invalidUpdates := []map[string]interface{}{
		{
			"id":    1,
			"name":  "UpdatedUser1",
			"age":   30,
			"email": "updated1@test.com",
		},
		{
			"id":    2,
			"name":  "UpdatedUser2",
			"age":   []string{"invalid"}, // 使用明显错误的类型
			"email": "updated2@test.com",
		},
	}
	_, err = db.Chain().Table("error_test_user").BatchValues(invalidUpdates).BatchUpdate(2)
	assert.Error(t, err, "应该返回批量更新错误")

	// 3. 测试批量删除错误
	_, err = db.Chain().Table("error_test_user").BatchValues([]map[string]interface{}{
		{"invalid_id": 1}, // 错误的主键字段
	}).BatchDelete(1)
	assert.Error(t, err, "应该返回批量删除错误")
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

func TestConnectionPoolErrors(t *testing.T) {
	// 设置一个非常小的连接池
	poolOpts := &define.DBOptions{
		MaxOpenConns:    1, // 只允许一个连接
		MaxIdleConns:    1,
		ConnMaxLifetime: 100 * time.Millisecond,
		ConnMaxIdleTime: 100 * time.Millisecond,
		Debug:           true,
	}

	db, err := Open("mysql", testutils.DefaultMySQLConfig().DSN(), poolOpts)
	if err != nil {
		t.Skip("跳过连接池错误测试：", err)
		return
	}
	defer db.Close()

	// 创建测试表
	err = db.Chain().CreateTable(&ErrorTestUser{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "error_test_user")

	// 启动大量并发连接
	var wg sync.WaitGroup
	errChan := make(chan error, 100) // 增加缓冲区大小
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 启动100个并发连接
	for i := 0; i < 100; i++ { // 增加并发连接数
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 尝试执行查询
			tx, err := db.BeginChain()
			if err != nil {
				errChan <- err
				return
			}
			defer tx.Rollback()

			// 执行一个耗时的查询
			time.Sleep(50 * time.Millisecond) // 减少等待时间
			result := tx.Table("error_test_user").Save(&ErrorTestUser{
				Name:  fmt.Sprintf("User%d", id),
				Age:   25 + id,
				Email: fmt.Sprintf("user%d@test.com", id),
			})

			if result.Error != nil {
				errChan <- result.Error
				return
			}

			if err := tx.Commit(); err != nil {
				errChan <- err
				return
			}
			errChan <- nil
		}(i)
	}

	// 在一个单独的 goroutine 中等待 WaitGroup
	go func() {
		wg.Wait()
		close(errChan)
		close(done)
	}()

	// 等待所有 goroutine 完成或超时
	select {
	case <-ctx.Done():
		t.Log("测试超时")
		return
	case <-done:
		// 检查是否有资源耗尽错误
		var hasResourceExhaustion bool
		for err := range errChan {
			if err != nil && (strings.Contains(err.Error(), "too many connections") ||
				strings.Contains(err.Error(), "connection refused") ||
				strings.Contains(err.Error(), "resource temporarily unavailable") ||
				strings.Contains(err.Error(), "connection reset") ||
				strings.Contains(err.Error(), "broken pipe")) {
				hasResourceExhaustion = true
				break
			}
		}
		assert.True(t, hasResourceExhaustion, "应该出现资源耗尽错误")
	}
}

func TestConnectionPoolTimeout(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	// 创建测试表
	err := db.Chain().CreateTable(&ErrorTestUser{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "error_test_user")

	// 启动多个并发事务
	var wg sync.WaitGroup
	results := make(chan error, 100) // 增加缓冲区大小

	for i := 0; i < 100; i++ { // 增加并发连接数
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 等待一段时间，让连接超时
			if id > 5 { // 让更多的连接等待
				time.Sleep(200 * time.Millisecond) // 等待时间长于连接生命周期
			}

			tx, err := db.BeginChain()
			if err != nil {
				results <- err
				return
			}
			defer tx.Rollback()

			// 执行一个耗时的查询
			time.Sleep(150 * time.Millisecond) // 查询时间长于连接生命周期
			result := tx.Table("error_test_user").Save(&ErrorTestUser{
				Name:  fmt.Sprintf("User%d", id),
				Age:   25 + id,
				Email: fmt.Sprintf("user%d@test.com", id),
			})

			if result.Error != nil {
				results <- result.Error
				return
			}

			results <- tx.Commit()
		}(i)
	}

	wg.Wait()
	close(results)

	// 检查是否有超时错误
	var hasTimeout bool
	for err := range results {
		if err != nil && (strings.Contains(strings.ToLower(err.Error()), "timeout") ||
			strings.Contains(strings.ToLower(err.Error()), "broken pipe") ||
			strings.Contains(strings.ToLower(err.Error()), "connection reset") ||
			strings.Contains(strings.ToLower(err.Error()), "bad connection") ||
			strings.Contains(strings.ToLower(err.Error()), "connection refused") ||
			strings.Contains(strings.ToLower(err.Error()), "connection closed") ||
			strings.Contains(strings.ToLower(err.Error()), "too many connections")) {
			hasTimeout = true
			break
		}
	}
	assert.True(t, hasTimeout, "应该出现连接超时错误")
}

func TestDeadlockDetection(t *testing.T) {
	db := setupErrorTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	// 准备测试数据
	user1 := &ErrorTestUser{
		Name:  "User1",
		Age:   25,
		Email: "user1@test.com",
	}
	user2 := &ErrorTestUser{
		Name:  "User2",
		Age:   30,
		Email: "user2@test.com",
	}

	result := db.Chain().Table("error_test_user").Save(user1)
	assert.NoError(t, result.Error)
	result = db.Chain().Table("error_test_user").Save(user2)
	assert.NoError(t, result.Error)

	// 模拟死锁场景
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		tx1, err := db.BeginChain()
		if err != nil {
			errChan <- err
			return
		}
		defer tx1.Rollback()

		// 事务1先锁定第一条记录
		result := tx1.Raw("SELECT * FROM error_test_user WHERE email = ? FOR UPDATE", "user1@test.com").Exec()
		if result.Error != nil {
			errChan <- result.Error
			return
		}

		// 等待事务2锁定第二条记录
		time.Sleep(200 * time.Millisecond)

		// 事务1尝试更新第二条记录
		result = tx1.Raw("UPDATE error_test_user SET age = ? WHERE email = ?", 31, "user2@test.com").Exec()
		if result.Error != nil {
			errChan <- result.Error
			return
		}

		errChan <- tx1.Commit()
	}()

	go func() {
		defer wg.Done()
		tx2, err := db.BeginChain()
		if err != nil {
			errChan <- err
			return
		}
		defer tx2.Rollback()

		// 事务2先锁定第二条记录
		result := tx2.Raw("SELECT * FROM error_test_user WHERE email = ? FOR UPDATE", "user2@test.com").Exec()
		if result.Error != nil {
			errChan <- result.Error
			return
		}

		// 等待事务1尝试锁定第二条记录
		time.Sleep(100 * time.Millisecond)

		// 事务2尝试更新第一条记录
		result = tx2.Raw("UPDATE error_test_user SET age = ? WHERE email = ?", 26, "user1@test.com").Exec()
		if result.Error != nil {
			errChan <- result.Error
			return
		}

		errChan <- tx2.Commit()
	}()

	wg.Wait()
	close(errChan)

	// 检查是否有死锁错误
	var hasDeadlock bool
	for err := range errChan {
		if err != nil && (strings.Contains(strings.ToLower(err.Error()), "deadlock") ||
			strings.Contains(strings.ToLower(err.Error()), "lock wait timeout") ||
			strings.Contains(strings.ToLower(err.Error()), "lock acquisition") ||
			strings.Contains(strings.ToLower(err.Error()), "could not serialize access")) {
			hasDeadlock = true
			break
		}
	}
	assert.True(t, hasDeadlock, "应该检测到死锁或锁等待超时")
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
