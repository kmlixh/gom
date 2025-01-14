package gom

import (
	"fmt"
	"strings"
	"testing"
	"time"

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

// 数据库连接配置
var (
	mysqlDSN    = "root:123456@tcp(192.168.110.249:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	postgresDSN = "postgres://postgres:yzy123@192.168.110.249:5432/test?sslmode=disable"
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
		_, err = db.Chain().ExecuteRaw(sql)
		assert.NoError(t, err)
	}

	return db.Chain()
}

func cleanupTestDB(t *testing.T, db *Chain) {
	_, err := db.ExecuteRaw("DROP TABLE IF EXISTS test_users")
	assert.NoError(t, err)
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
	result, err := db.Insert(&user)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// 测试查询
	var queryUser TestUser
	dd, err := db.Table("test_users").First(&queryUser)
	assert.NotNil(t, dd)
	assert.NoError(t, err)
	assert.Equal(t, user.Name, queryUser.Name)
	assert.Equal(t, user.Age, queryUser.Age)
	assert.Equal(t, user.Email, queryUser.Email)

	// 测试更新
	queryUser.Age = 26
	result, err = db.Update(&queryUser)
	assert.NoError(t, err)
	affected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// 测试删除
	result, err = db.Delete(&queryUser)
	assert.NoError(t, err)
	affected, err = result.RowsAffected()
	assert.NoError(t, err)
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
		_, err := chain.Insert(&user)
		assert.NoError(t, err)
	}

	// 测试Eq条件
	var result []TestUser
	_, err := chain.Table("test_users").Eq("age", 25).Select(&result)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "User2", result[0].Name)

	// 测试Gt条件
	result = nil
	_, err = chain.Table("test_users").Gt("age", 25).Select(&result)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "User3", result[0].Name)

	// 测试Like条件
	result = nil
	_, err = chain.Table("test_users").Like("name", "User%").Select(&result)
	assert.NoError(t, err)
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
	_, err := db.DoTransaction(func(tx *Chain) (interface{}, error) {
		user1 := TestUser{Name: "TxUser1", Age: 20, Email: "tx1@example.com"}
		user2 := TestUser{Name: "TxUser2", Age: 25, Email: "tx2@example.com"}

		_, err := tx.Insert(&user1)
		if err != nil {
			return nil, err
		}

		_, err = tx.Insert(&user2)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	assert.NoError(t, err)

	// 验证事务结果
	var count int64
	count, err = db.Table("test_users").Count("id")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// 测试失败的事务
	_, err = db.DoTransaction(func(tx *Chain) (interface{}, error) {
		user := TestUser{Name: "TxUser3", Age: 30, Email: "tx3@example.com"}
		_, err := tx.Insert(&user)
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("rollback test")
	})
	assert.Error(t, err)

	// 验证回滚结果
	count, err = db.Table("test_users").Count("id")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
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

func testFields(t *testing.T, db *Chain) {
	// 插入测试数据
	user := TestUser{
		Name:  "Fields Test",
		Age:   30,
		Email: "fields@example.com",
	}
	_, err := db.Insert(&user)
	assert.NoError(t, err)

	// 测试Fields限制查询字段
	var result TestUser
	_, err = db.Table("test_users").Fields("name", "age").First(&result)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.Name)
	assert.NotZero(t, result.Age)
	assert.Empty(t, result.Email) // email字段未包含在Fields中

	// 测试Fields限制更新字段
	result.Name = "Updated Name"
	result.Age = 31
	result.Email = "updated@example.com"
	_, err = db.Fields("name").Update(&result)
	assert.NoError(t, err)

	// 验证只有name字段被更新
	var updated TestUser
	_, err = db.Table("test_users").First(&updated)
	assert.NoError(t, err)
	assert.Equal(t, "Updated Name", updated.Name)
	assert.Equal(t, 30, updated.Age)                     // age未被更新
	assert.Equal(t, "fields@example.com", updated.Email) // email未被更新
}
