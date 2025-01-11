package gom

import (
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

type ErrorTestUser struct {
	ID        int64     `gom:"id,@" sql:"id,pk,auto_increment"`
	Name      string    `gom:"name" sql:"name"`
	Age       int       `gom:"age" sql:"age"`
	Email     string    `gom:"email" sql:"email"`
	Version   int       `gom:"version" sql:"version"`
	CreatedAt time.Time `gom:"created_at" sql:"created_at"`
	UpdatedAt time.Time `gom:"updated_at" sql:"updated_at"`
}

func (t *ErrorTestUser) TableName() string {
	return "error_test_user"
}

func setupErrorTestDB(t *testing.T) *DB {
	config := testutils.DefaultMySQLConfig()
	config.User = "root"
	config.Password = "123456"
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
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
		name VARCHAR(10) NOT NULL,
		age INT NOT NULL CHECK (age > 0 AND age < 150),
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
	if db == nil {
		return
	}
	defer db.Close()

	// 准备测试数据
	user := &ErrorTestUser{
		Name:    "Original",
		Age:     25,
		Email:   "original@test.com",
		Version: 1,
	}
	result := db.Chain().Table("error_test_user").Save(user)
	assert.NoError(t, result.Error)

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
	rowsAffected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// 事务2尝试使用旧版本号更新数据（应该失败）
	result = db.Chain().Table("error_test_user").Raw(
		"UPDATE error_test_user SET name = ?, version = ? WHERE id = ? AND version = ?",
		"Updated2", user2.Version+1, user.ID, user2.Version,
	).Exec()
	assert.NoError(t, result.Error)
	rowsAffected, err = result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rowsAffected, "应该没有行被更新")

	// 验证数据是否正确更新
	var finalUsers []ErrorTestUser
	result = db.Chain().Table("error_test_user").
		Where("id", define.OpEq, user.ID).
		List(&finalUsers)
	assert.NoError(t, result.Error)
	assert.Len(t, finalUsers, 1)
	assert.Equal(t, "Updated1", finalUsers[0].Name)
	assert.Equal(t, user1.Version+1, finalUsers[0].Version)
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
