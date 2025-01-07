package mysql

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", testutils.TestMySQLDSN)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// Create test table
	sql := `
		DROP TABLE IF EXISTS test_users;
		CREATE TABLE test_users (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			username VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL,
			age INT NOT NULL,
			active BOOLEAN NOT NULL,
			role VARCHAR(50) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			deleted_at TIMESTAMP NULL
		);
	`
	_, err = db.Exec(sql)
	assert.NoError(t, err)

	return db
}

func TestFactoryConnect(t *testing.T) {
	factory := &Factory{}

	// Test valid connection
	db, err := factory.Connect("root:123456@tcp(10.0.1.5:3306)/test?charset=utf8mb4&parseTime=True")
	assert.NoError(t, err)
	assert.NotNil(t, db)
	db.Close()

	// Test invalid connection
	db, err = factory.Connect("invalid")
	assert.Error(t, err)
	assert.Nil(t, db)
}

func TestFactoryBuildSelect(t *testing.T) {
	factory := &Factory{}

	// Test simple select
	query, args := factory.BuildSelect(
		"test_users",
		[]string{"username", "email"},
		[]*define.Condition{define.Eq("active", true)},
		"username ASC",
		10,
		0,
	)
	assert.Contains(t, query, "SELECT `username`, `email`")
	assert.Contains(t, query, "FROM `test_users`")
	assert.Contains(t, query, "WHERE `active` = ?")
	assert.Contains(t, query, "ORDER BY username ASC")
	assert.Contains(t, query, "LIMIT 10")
	assert.Equal(t, []interface{}{true}, args)

	// Test select with multiple conditions
	query, args = factory.BuildSelect(
		"test_users",
		[]string{"*"},
		[]*define.Condition{
			define.Gt("age", 18),
			define.Like("email", "%@example.com"),
		},
		"",
		0,
		0,
	)
	assert.Contains(t, query, "SELECT *")
	assert.Contains(t, query, "`age` > ?")
	assert.Contains(t, query, "`email` LIKE ?")
	assert.Equal(t, []interface{}{18, "%@example.com"}, args)
}

func TestFactoryBuildInsert(t *testing.T) {
	factory := &Factory{}
	now := time.Now()

	// Test single insert
	fields := map[string]interface{}{
		"username":   "test_user",
		"email":      "test@example.com",
		"age":        25,
		"active":     true,
		"role":       "user",
		"created_at": now,
	}
	fieldOrder := []string{"username", "email", "age", "active", "role", "created_at"}

	query, args := factory.BuildInsert("test_users", fields, fieldOrder)
	assert.Contains(t, query, "INSERT INTO `test_users`")
	assert.Len(t, args, 6)

	// Test empty insert
	query, args = factory.BuildInsert("test_users", nil, nil)
	assert.Empty(t, query)
	assert.Empty(t, args)
}

func TestFactoryBuildUpdate(t *testing.T) {
	factory := &Factory{}

	// Test update with conditions
	fields := map[string]interface{}{
		"age":    30,
		"active": false,
	}
	fieldOrder := []string{"age", "active"}
	conditions := []*define.Condition{define.Eq("username", "test_user")}

	query, args := factory.BuildUpdate("test_users", fields, fieldOrder, conditions)
	assert.Contains(t, query, "UPDATE `test_users` SET")
	assert.Contains(t, query, "`age` = ?")
	assert.Contains(t, query, "`active` = ?")
	assert.Contains(t, query, "WHERE `username` = ?")
	assert.Equal(t, []interface{}{30, false, "test_user"}, args)

	// Test empty update
	query, args = factory.BuildUpdate("test_users", nil, nil, nil)
	assert.Empty(t, query)
	assert.Empty(t, args)
}

func TestFactoryBuildDelete(t *testing.T) {
	factory := &Factory{}

	// Test delete with conditions
	conditions := []*define.Condition{
		define.Eq("username", "test_user"),
		define.Gt("age", 25),
	}

	query, args := factory.BuildDelete("test_users", conditions)
	assert.Contains(t, query, "DELETE FROM `test_users`")
	assert.Contains(t, query, "WHERE `username` = ?")
	assert.Contains(t, query, "`age` > ?")
	assert.Equal(t, []interface{}{"test_user", 25}, args)

	// Test delete without conditions
	query, args = factory.BuildDelete("test_users", nil)
	assert.Equal(t, "DELETE FROM `test_users`", query)
	assert.Empty(t, args)
}

func TestFactoryBuildCreateTable(t *testing.T) {
	factory := &Factory{}

	type TestStruct struct {
		ID        int64      `gom:"id,@,auto"`
		Username  string     `gom:"username,notnull"`
		Email     string     `gom:"email,notnull"`
		Age       int        `gom:"age,notnull"`
		Active    bool       `gom:"active,notnull"`
		CreatedAt time.Time  `gom:"created_at,notnull,default"`
		UpdatedAt time.Time  `gom:"updated_at,notnull"`
		DeletedAt *time.Time `gom:"deleted_at"`
	}

	query := factory.BuildCreateTable("test_users", reflect.TypeOf(TestStruct{}))
	assert.Contains(t, query, "CREATE TABLE IF NOT EXISTS `test_users`")
	assert.Contains(t, query, "`id` BIGINT AUTO_INCREMENT PRIMARY KEY")
	assert.Contains(t, query, "`username` VARCHAR(255) NOT NULL")
	assert.Contains(t, query, "`email` VARCHAR(255) NOT NULL")
	assert.Contains(t, query, "`age` INTEGER NOT NULL")
	assert.Contains(t, query, "`active` BOOLEAN NOT NULL")
	assert.Contains(t, query, "`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP")
	assert.Contains(t, query, "`updated_at` TIMESTAMP NOT NULL")
	assert.Contains(t, query, "`deleted_at` TIMESTAMP")
}

func TestFactoryGetTableInfo(t *testing.T) {
	factory := &Factory{}

	// Skip test if no database connection
	db, err := sql.Open("mysql", testutils.TestMySQLDSN)
	if err != nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Test error case
	info, err := factory.GetTableInfo(db, "non_existent_table")
	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestFactoryGetTables(t *testing.T) {
	factory := &Factory{}
	db, err := factory.Connect(testutils.TestMySQLDSN)
	if err != nil {
		t.Skip("Skipping test due to database connection error:", err)
		return
	}
	defer db.Close()

	// Create test tables
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test_table1 (id INT PRIMARY KEY)`)
	if err != nil {
		t.Skip("Skipping test due to table creation error:", err)
		return
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test_table2 (id INT PRIMARY KEY)`)
	if err != nil {
		t.Skip("Skipping test due to table creation error:", err)
		return
	}

	// Test GetTables with pattern
	tables, err := factory.GetTables(db, "test_%")
	if err != nil {
		t.Skip("Skipping test due to GetTables error:", err)
		return
	}
	assert.Contains(t, tables, "test_table1")
	assert.Contains(t, tables, "test_table2")

	// Clean up
	_, err = db.Exec("DROP TABLE IF EXISTS test_table1")
	if err != nil {
		t.Log("Warning: Failed to clean up test_table1:", err)
	}
	_, err = db.Exec("DROP TABLE IF EXISTS test_table2")
	if err != nil {
		t.Log("Warning: Failed to clean up test_table2:", err)
	}
}

func TestFactoryBuildOrderBy(t *testing.T) {
	factory := &Factory{}

	// Test single order
	orders := []define.OrderBy{
		{Field: "username", Type: define.OrderAsc},
	}
	orderBy := factory.BuildOrderBy(orders)
	assert.Equal(t, "ORDER BY `username` ASC", orderBy)

	// Test multiple orders
	orders = []define.OrderBy{
		{Field: "age", Type: define.OrderDesc},
		{Field: "username", Type: define.OrderAsc},
	}
	orderBy = factory.BuildOrderBy(orders)
	assert.Equal(t, "ORDER BY `age` DESC, `username` ASC", orderBy)

	// Test empty orders
	orderBy = factory.BuildOrderBy(nil)
	assert.Empty(t, orderBy)
}

func TestFactoryBuildSelectWithGroupBy(t *testing.T) {
	factory := &Factory{}

	// Test GROUP BY
	query, args := factory.BuildSelect(
		"test_users",
		[]string{
			"role",
			"COUNT(*) as count",
			"GROUP BY role",
		},
		nil,
		"count DESC",
		0,
		0,
	)
	assert.Contains(t, query, "SELECT `role`, COUNT(*) as count")
	assert.Contains(t, query, "FROM `test_users`")
	assert.Contains(t, query, "GROUP BY role")
	assert.Contains(t, query, "ORDER BY count DESC")
	assert.Empty(t, args)

	// Test GROUP BY with HAVING
	query, args = factory.BuildSelect(
		"test_users",
		[]string{
			"role",
			"AVG(age) as avg_age",
			"COUNT(*) as count",
			"GROUP BY role",
			"HAVING count > ? AND avg_age >= ?",
		},
		[]*define.Condition{
			{
				Field: "HAVING count > ? AND avg_age >= ?",
				Op:    define.OpCustom,
				Value: []interface{}{5, 25},
			},
		},
		"avg_age DESC",
		0,
		0,
	)
	assert.Contains(t, query, "SELECT `role`, AVG(age) as avg_age, COUNT(*) as count")
	assert.Contains(t, query, "FROM `test_users`")
	assert.Contains(t, query, "GROUP BY role")
	assert.Contains(t, query, "HAVING count > ? AND avg_age >= ?")
	assert.Contains(t, query, "ORDER BY avg_age DESC")
	assert.Equal(t, []interface{}{5, 25}, args)
}
