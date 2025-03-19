package gom

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

type NullTestModel struct {
	ID       int64   `gom:"id" sql:"id"`
	Name     string  `gom:"name" sql:"name"`
	Email    string  `gom:"email" sql:"email"`
	ParentID string  `gom:"parent_id" sql:"parent_id"`
	Age      int     `gom:"age" sql:"age"`
	Score    float64 `gom:"score" sql:"score"`
}

func (m *NullTestModel) TableName() string {
	return "null_tests"
}

func TestNullValuesHandling(t *testing.T) {
	db := getDB()
	if db == nil {
		t.Fatal("Failed to initialize test database")
	}
	defer db.Close()

	// 创建测试表
	_, err := db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS null_tests (
			id INTEGER PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL,
			parent_id VARCHAR(255) NULL,
			age INTEGER NULL,
			score REAL NULL
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// 插入具有NULL值的测试数据
	_, err = db.DB.Exec(`
		INSERT INTO null_tests (name, email, parent_id, age, score)
		VALUES (?, ?, NULL, NULL, NULL)
	`, "Test User", "test@example.com")
	if err != nil {
		t.Fatal(err)
	}

	// 测试使用Into映射到结构体
	var model NullTestModel
	err = db.Chain().Table("null_tests").First().Into(&model)
	if err != nil {
		t.Fatal(err)
	}

	// 验证NULL值已被正确处理
	assert.Equal(t, "Test User", model.Name)
	assert.Equal(t, "test@example.com", model.Email)
	assert.Equal(t, "", model.ParentID) // NULL应该变成空字符串
	assert.Equal(t, 0, model.Age)       // NULL整数应该变成0
	assert.Equal(t, 0.0, model.Score)   // NULL浮点数应该变成0.0

	// 测试使用List直接获取结果
	result := db.Chain().Table("null_tests").First()
	if result.Error != nil {
		t.Fatal(result.Error)
	}

	// 验证map中的NULL值处理
	row := result.Data[0]
	assert.Equal(t, "Test User", row["name"])
	assert.Equal(t, "test@example.com", row["email"])

	// 验证NULL值以sql.Null*类型表示
	if nullStr, ok := row["parent_id"].(sql.NullString); ok {
		assert.False(t, nullStr.Valid, "parent_id should be invalid (NULL)")
	}
	if nullInt, ok := row["age"].(sql.NullInt64); ok {
		assert.False(t, nullInt.Valid, "age should be invalid (NULL)")
	}
	if nullFloat, ok := row["score"].(sql.NullFloat64); ok {
		assert.False(t, nullFloat.Valid, "score should be invalid (NULL)")
	}

	// 测试使用指针类型处理NULL
	type NullTestWithPointers struct {
		ID       int64    `gom:"id"`
		ParentID *string  `gom:"parent_id"`
		Age      *int     `gom:"age"`
		Score    *float64 `gom:"score"`
	}

	var ptrModel NullTestWithPointers
	err = db.Chain().Table("null_tests").First().Into(&ptrModel)
	if err != nil {
		t.Fatal(err)
	}

	// 验证指针类型正确处理NULL
	if ptrModel.ParentID != nil {
		assert.Equal(t, "", *ptrModel.ParentID, "ParentID should be empty string or nil")
	}
	if ptrModel.Age != nil {
		assert.Equal(t, 0, *ptrModel.Age, "Age should be 0 or nil")
	}
	if ptrModel.Score != nil {
		assert.Equal(t, 0.0, *ptrModel.Score, "Score should be 0.0 or nil")
	}

	// 测试使用sql.Null*类型处理NULL
	type NullTestWithSqlNull struct {
		ID       int64           `gom:"id"`
		ParentID sql.NullString  `gom:"parent_id"`
		Age      sql.NullInt64   `gom:"age"`
		Score    sql.NullFloat64 `gom:"score"`
	}

	var sqlNullModel NullTestWithSqlNull
	err = db.Chain().Table("null_tests").First().Into(&sqlNullModel)
	if err != nil {
		t.Fatal(err)
	}

	// 验证sql.Null*类型正确处理NULL
	assert.False(t, sqlNullModel.ParentID.Valid)
	assert.False(t, sqlNullModel.Age.Valid)
	assert.False(t, sqlNullModel.Score.Valid)

	// 清理
	_, err = db.DB.Exec("DROP TABLE null_tests")
	if err != nil {
		t.Fatal(err)
	}
}
