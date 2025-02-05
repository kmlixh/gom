package gom

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	_ "github.com/kmlixh/gom/v4/factory/mysql"
	_ "github.com/kmlixh/gom/v4/factory/postgres"
	"github.com/stretchr/testify/assert"
)

// 测试用的模型结构
type TestUser struct {
	Id        int64     `db:"id" gom:"@,id"`
	Name      string    `db:"name" gom:"#,name"`
	Age       int       `db:"age" gom:"#,age"`
	Email     string    `db:"email" gom:"#,email"`
	CreatedAt time.Time `db:"created_at" gom:"#,created_at"`
	UpdatedAt time.Time `db:"updated_at" gom:"#,updated_at"`
}

func (u TestUser) TableName() string {
	return "test_users"
}

var ip = "192.168.110.249"

//var ip = "10.0.1.5"

// 数据库连接配置
var (
	mysqlDSN    = "root:123456@tcp(" + ip + ":3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	postgresDSN = "postgres://postgres:yzy123@" + ip + ":5432/test?sslmode=disable"
)

// 创建测试表的SQL语句
var (
	mysqlCreateTableSQL = `
	DROP TABLE IF EXISTS test_users;
	CREATE TABLE test_users (
		id BIGINT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL,
		age INT NOT NULL,
		email VARCHAR(100) NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`

	postgresCreateTableSQL = `
	DROP TABLE IF EXISTS test_users;
	CREATE TABLE test_users (
		id BIGSERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		age INT NOT NULL,
		email VARCHAR(100) NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	`
)

// 测试辅助函数
func setupTestDB(t *testing.T, driver string, dsn string, createTableSQL string) *Chain {
	db, err := Open(driver, dsn, true)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// 创建测试表
	for _, sql := range strings.Split(createTableSQL, ";") {
		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}
		rs := db.Chain().Raw(nil, sql)
		assert.NoError(t, rs.Error())
	}

	return db.Chain()
}

func cleanupTestDB(t *testing.T, db *Chain) {
	rs := db.Raw(nil, "DROP TABLE IF EXISTS test_users")
	assert.NoError(t, rs.Error())
}

// 基本CRUD操作测试
func TestBasicCRUD(t *testing.T) {
	// MySQL测试
	t.Run("mysql", func(t *testing.T) {
		db := setupTestDB(t, "mysql", mysqlDSN, mysqlCreateTableSQL)
		defer cleanupTestDB(t, db)
		testBasicCRUD(t, db)
	})

	// PostgreSQL测试
	t.Run("postgres", func(t *testing.T) {
		db := setupTestDB(t, "postgres", postgresDSN, postgresCreateTableSQL)
		defer cleanupTestDB(t, db)
		testBasicCRUD(t, db)
	})
}

func testBasicCRUD(t *testing.T, db *Chain) {
	// 测试插入
	user := TestUser{
		Name:  "Test User",
		Age:   25,
		Email: "test@example.com",
	}
	result := db.Insert(&user)
	assert.NoError(t, result.Error())
	assert.NotNil(t, result)

	// 测试查询
	var queryUser TestUser
	dd := db.Table("test_users").First(&queryUser)
	assert.NotNil(t, dd)
	assert.NoError(t, dd.Error())
	assert.Equal(t, user.Name, queryUser.Name)
	assert.Equal(t, user.Age, queryUser.Age)
	assert.Equal(t, user.Email, queryUser.Email)

	// 测试更新
	queryUser.Age = 26
	result = db.Fields("age").Update(&queryUser)
	assert.NoError(t, result.Error())
	affected := result.RowsAffected()
	assert.NoError(t, result.Error())
	assert.Equal(t, int64(1), affected)

	// 测试删除
	result = db.Delete(&queryUser)
	assert.NoError(t, result.Error())
	affected = result.RowsAffected()
	assert.Equal(t, int64(1), affected)
}

// 条件查询测试
func TestConditions(t *testing.T) {
	// MySQL测试
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t, "mysql", mysqlDSN, mysqlCreateTableSQL)
		defer cleanupTestDB(t, db)
		testConditions(t, db)
	})

	// PostgreSQL测试
	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t, "postgres", postgresDSN, postgresCreateTableSQL)
		defer cleanupTestDB(t, db)
		testConditions(t, db)
	})
}

func testConditions(t *testing.T, chain *Chain) {
	// 插入测试数据
	users := []TestUser{
		{Name: "User1", Age: 20, Email: "user1@example.com"},
		{Name: "User2", Age: 25, Email: "user2@example.com"},
		{Name: "User3", Age: 30, Email: "user3@example.com"},
	}

	for _, user := range users {
		rs := chain.Insert(&user)
		assert.NoError(t, rs.Error())
	}

	// 测试Eq条件
	var result []TestUser
	rs := chain.Table("test_users").Eq("age", 25).Select(&result)
	assert.NoError(t, rs.Error())
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "User2", result[0].Name)

	// 测试Gt条件
	result = make([]TestUser, 0)
	rs = chain.Table("test_users").Gt("age", 25).Select(&result)
	assert.NoError(t, rs.Error())
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "User3", result[0].Name)

	// 测试Like条件
	result = nil
	rs = chain.Table("test_users").Like("name", "User%").Select(&result)
	assert.NoError(t, rs.Error())
	assert.Equal(t, 3, len(result))
}

// 事务测试
func TestTransaction(t *testing.T) {
	// MySQL测试
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t, "mysql", mysqlDSN, mysqlCreateTableSQL)
		defer cleanupTestDB(t, db)
		testTransaction(t, db)
	})

	// PostgreSQL测试
	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t, "postgres", postgresDSN, postgresCreateTableSQL)
		defer cleanupTestDB(t, db)
		testTransaction(t, db)
	})
}

func testTransaction(t *testing.T, db *Chain) {

	// 测试成功的事务
	_, er := db.DoTransaction(func(tx *Chain) (interface{}, error) {
		user1 := TestUser{Name: "TxUser1", Age: 20, Email: "tx1@example.com"}
		user2 := TestUser{Name: "TxUser2", Age: 25, Email: "tx2@example.com"}

		rs := tx.Insert(&user1)
		if rs.Error() != nil {
			return nil, rs.Error()
		}

		rs = tx.Insert(&user2)
		if rs.Error() != nil {
			return nil, rs.Error()
		}

		return nil, nil
	})
	assert.NoError(t, er)

	// 验证事务结果
	var count int64
	rsz := db.Table("test_users").Count("id")
	count = rsz.Data().(int64)
	assert.NoError(t, rsz.Error())
	assert.Equal(t, int64(2), count)

	// 测试失败的事务
	_, er = db.DoTransaction(func(tx *Chain) (interface{}, error) {
		user := TestUser{Name: "TxUser3", Age: 30, Email: "tx3@example.com"}
		rs := tx.Insert(&user)
		if rs.Error() != nil {
			return nil, rs.Error()
		}

		return nil, fmt.Errorf("rollback test")
	})
	assert.Error(t, er)

	// 验证回滚结果
	counts := db.Table("test_users").Count("id")
	assert.NoError(t, counts.Error())
	assert.Equal(t, int64(2), counts.Data())
}

// 批量操作测试
func TestBatchOperations(t *testing.T) {
	// MySQL测试
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t, "mysql", mysqlDSN, mysqlCreateTableSQL)
		defer cleanupTestDB(t, db)
	})

	// PostgreSQL测试
	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t, "postgres", postgresDSN, postgresCreateTableSQL)
		defer cleanupTestDB(t, db)
	})
}

// Fields方法测试
func TestFields(t *testing.T) {
	// MySQL测试
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t, "mysql", mysqlDSN, mysqlCreateTableSQL)
		defer cleanupTestDB(t, db)
		testFields(t, db)
	})

	// PostgreSQL测试
	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t, "postgres", postgresDSN, postgresCreateTableSQL)
		defer cleanupTestDB(t, db)
		testFields(t, db)
	})
}

func testFields(t *testing.T, chain *Chain) {
	// 插入测试数据
	user := TestUser{
		Name:      "Fields Test",
		Age:       30,
		Email:     "fields@example.com",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	rs := chain.Insert(&user)
	if rs.RowsAffected() == 1 && user.Id == 0 {
		user.Id = rs.LastInsertId()
	}
	assert.NoError(t, rs.Error())

	// 测试Fields限制查询字段
	var result TestUser
	rs = chain.Table("test_users").Fields("id", "name", "age").Eq("id", user.Id).First(&result)
	assert.NoError(t, rs.Error())
	assert.NotEmpty(t, result.Name)
	assert.NotZero(t, result.Age)
	assert.Empty(t, result.Email) // email字段未包含在Fields中

	// 测试Fields限制更新字段
	result.Name = "Updated Name"
	result.Age = 31
	result.Email = "updated@example.com"
	rs = chain.Fields("name").Update(&result)
	assert.NoError(t, rs.Error())

	// 验证只有name字段被更新
	var updated TestUser
	rs = chain.Table("test_users").First(&updated)
	assert.NoError(t, rs.Error())
	assert.Equal(t, "Updated Name", updated.Name)
	assert.Equal(t, 30, updated.Age)                     // age未被更新
	assert.Equal(t, "fields@example.com", updated.Email) // email未被更新
}

// 并发安全性测试
func TestConcurrentOperations(t *testing.T) {
	//t.Run("MySQL", func(t *testing.T) {
	//	db := setupTestDB(t, "mysql", mysqlDSN, mysqlCreateTableSQL)
	//	defer cleanupTestDB(t, db)
	//
	//	t.Run("ConcurrentInsert", func(t *testing.T) {
	//		testConcurrentOperations(t, db)
	//	})
	//
	//	t.Run("ConcurrentRead", func(t *testing.T) {
	//		testConcurrentRead(t, db)
	//	})
	//})
	//
	//t.Run("PostgreSQL", func(t *testing.T) {
	//	db := setupTestDB(t, "postgres", postgresDSN, postgresCreateTableSQL)
	//	defer cleanupTestDB(t, db)
	//
	//	t.Run("ConcurrentInsert", func(t *testing.T) {
	//		testConcurrentOperations(t, db)
	//	})
	//
	//	t.Run("ConcurrentRead", func(t *testing.T) {
	//		testConcurrentRead(t, db)
	//	})
	//})
}

func testConcurrentOperations(t *testing.T, db *Chain) {
	const numGoroutines = 10
	done := make(chan bool)
	errors := make(chan error, numGoroutines)

	// 并发插入
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer func() {
				done <- true
			}()

			// 使用事务来保证并发安全
			_, err := db.DoTransaction(func(tx *Chain) (interface{}, error) {
				user := TestUser{
					Name:  fmt.Sprintf("ConcurrentUser%d", idx),
					Age:   20 + idx,
					Email: fmt.Sprintf("user%d@example.com", idx),
				}
				result := tx.Insert(&user)
				if result.Error() != nil {
					return nil, result.Error()
				}
				return user.Id, nil
			})

			if err != nil {
				errors <- err
			}
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
	close(done)
	close(errors)

	// 检查是否有错误发生
	for err := range errors {
		assert.NoError(t, err)
	}

	// 验证插入结果
	var users []TestUser
	result := db.Table("test_users").Select(&users)
	assert.NoError(t, result.Error())
	assert.Equal(t, numGoroutines, len(users))

	// 验证每个用户都被正确插入
	userMap := make(map[string]bool)
	for _, user := range users {
		userMap[user.Name] = true
	}
	for i := 0; i < numGoroutines; i++ {
		name := fmt.Sprintf("ConcurrentUser%d", i)
		assert.True(t, userMap[name], "User %s should exist", name)
	}
}

// 添加更多并发测试场景
func testConcurrentRead(t *testing.T, db *Chain) {
	// 先插入一些测试数据
	for i := 0; i < 5; i++ {
		user := TestUser{
			Name:  fmt.Sprintf("ReadUser%d", i),
			Age:   20 + i,
			Email: fmt.Sprintf("read%d@example.com", i),
		}
		result := db.Insert(&user)
		assert.NoError(t, result.Error())
	}

	const numReaders = 20
	done := make(chan bool)
	errors := make(chan error, numReaders)

	// 并发读取
	for i := 0; i < numReaders; i++ {
		go func() {
			defer func() {
				done <- true
			}()

			var users []TestUser
			result := db.Table("test_users").Select(&users)
			if result.Error() != nil {
				errors <- result.Error()
				return
			}
			if len(users) != 5 {
				errors <- fmt.Errorf("expected 5 users, got %d", len(users))
			}
		}()
	}

	// 等待所有读取操作完成
	for i := 0; i < numReaders; i++ {
		<-done
	}
	close(done)
	close(errors)

	// 检查是否有错误发生
	for err := range errors {
		assert.NoError(t, err)
	}
}

// 复杂条件查询测试
func TestComplexConditions(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t, "mysql", mysqlDSN, mysqlCreateTableSQL)
		defer cleanupTestDB(t, db)
		testComplexConditions(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t, "postgres", postgresDSN, postgresCreateTableSQL)
		defer cleanupTestDB(t, db)
		testComplexConditions(t, db)
	})
}

func testComplexConditions(t *testing.T, db *Chain) {
	// 插入测试数据
	users := []TestUser{
		{Name: "Alice", Age: 25, Email: "alice@example.com"},
		{Name: "Bob", Age: 30, Email: "bob@example.com"},
		{Name: "Charlie", Age: 35, Email: "charlie@example.com"},
		{Name: "David", Age: 40, Email: "david@example.com"},
	}

	for _, user := range users {
		result := db.Insert(&user)
		assert.NoError(t, result.Error())
	}

	// 测试复杂AND条件
	var result []TestUser
	rs := db.Table("test_users").
		Ge("age", 30).
		And("email", define.Like, "%example.com").
		Select(&result)
	assert.NoError(t, rs.Error())
	assert.Equal(t, 3, len(result))

	// 测试OR条件
	result = nil
	rs = db.Table("test_users").
		Eq("age", 25).
		Or("age", define.Eq, 40).
		Select(&result)
	assert.NoError(t, rs.Error())
	assert.Equal(t, 2, len(result))

	// 测试复合条件
	result = nil
	rs = db.Table("test_users").
		Where2("(age >= ? AND age <= ?) OR email LIKE ?", 30, 35, "%bob%").
		Select(&result)
	assert.NoError(t, rs.Error())
	assert.Equal(t, 2, len(result))
}

// 错误处理测试
func TestErrorHandling(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t, "mysql", mysqlDSN, mysqlCreateTableSQL)
		defer cleanupTestDB(t, db)
		testErrorHandling(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t, "postgres", postgresDSN, postgresCreateTableSQL)
		defer cleanupTestDB(t, db)
		testErrorHandling(t, db)
	})
}

func testErrorHandling(t *testing.T, db *Chain) {
	// 测试无效的表名
	result := db.Table("non_existent_table").Count("id")
	assert.Error(t, result.Error())

	// 测试无效的列名
	result = db.Table("test_users").Count("non_existent_column")
	assert.Error(t, result.Error())

	// 测试无效的SQL语法
	result = db.Raw(nil, "SELECT * FROMM test_users")
	assert.Error(t, result.Error())

	// 测试事务回滚
	_, err := db.DoTransaction(func(tx *Chain) (interface{}, error) {
		user := TestUser{Name: "ErrorUser", Age: -1, Email: "error@example.com"}
		rs := tx.Insert(&user)
		if rs.Error() != nil {
			return nil, rs.Error()
		}
		return nil, fmt.Errorf("forced error for testing")
	})
	assert.Error(t, err)

	// 验证数据是否已回滚
	var count int64
	result = db.Table("test_users").Count("id")
	count = result.Data().(int64)
	assert.Equal(t, int64(0), count)
}

// 资源清理测试
func TestResourceCleanup(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t, "mysql", mysqlDSN, mysqlCreateTableSQL)
		defer cleanupTestDB(t, db)
		testResourceCleanup(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t, "postgres", postgresDSN, postgresCreateTableSQL)
		defer cleanupTestDB(t, db)
		testResourceCleanup(t, db)
	})
}

func testResourceCleanup(t *testing.T, db *Chain) {
	// 测试CleanDb
	db.Table("test_users").
		Page(1, 10).
		OrderByAsc("id").
		Fields("name", "age")

	db.CleanDb()
	assert.Nil(t, db.table)
	assert.Nil(t, db.orderBys)
	assert.Empty(t, db.fields)

	// 测试事务资源清理
	err := db.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, db.tx)

	db.Rollback()
	assert.Nil(t, db.tx)

	// 测试重复提交/回滚
	db.Commit()
	assert.Nil(t, db.tx)

	db.Rollback()
	assert.Nil(t, db.tx)
}
