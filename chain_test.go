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

func testFields(t *testing.T, db *Chain) {
	// 插入测试数据
	user := TestUser{
		Name:  "Fields Test",
		Age:   30,
		Email: "fields@example.com",
	}
	rs := db.Insert(&user)
	assert.NoError(t, rs.Error())

	// 测试Fields限制查询字段
	var result TestUser
	rs = db.Table("test_users").Fields("name", "age").First(&result)
	assert.NoError(t, rs.Error())
	assert.NotEmpty(t, result.Name)
	assert.NotZero(t, result.Age)
	assert.Empty(t, result.Email) // email字段未包含在Fields中

	// 测试Fields限制更新字段
	result.Name = "Updated Name"
	result.Age = 31
	result.Email = "updated@example.com"
	rs = db.Fields("name").Update(&result)
	assert.NoError(t, rs.Error())

	// 验证只有name字段被更新
	var updated TestUser
	rs = db.Table("test_users").First(&updated)
	assert.NoError(t, rs.Error())
	assert.Equal(t, "Updated Name", updated.Name)
	assert.Equal(t, 30, updated.Age)                     // age未被更新
	assert.Equal(t, "fields@example.com", updated.Email) // email未被更新
}
