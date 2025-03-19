package gom

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
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

// 测试扩展的NULL类型处理
type ExtendedNullTestModel struct {
	ID        int64     `gom:"id"`
	Name      string    `gom:"name"`
	ParentID  string    `gom:"parent_id"`  // 字符串类型 NULL
	Age       int       `gom:"age"`        // 整数类型 NULL
	Score     float64   `gom:"score"`      // 浮点数类型 NULL
	IsActive  bool      `gom:"is_active"`  // 布尔类型 NULL
	CreatedAt time.Time `gom:"created_at"` // 时间类型 NULL
	JsonData  string    `gom:"json_data"`  // JSON类型 NULL
}

func TestExtendedNullValueHandling(t *testing.T) {
	db := getDB()
	if db == nil {
		t.Fatal("Failed to initialize test database")
	}
	defer db.Close()

	// 创建测试表
	_, err := db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS extended_null_tests (
			id INTEGER PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			parent_id VARCHAR(255) NULL,
			age INTEGER NULL,
			score FLOAT NULL,
			is_active BOOLEAN NULL,
			created_at TIMESTAMP NULL,
			json_data JSON NULL
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// 插入具有NULL值的测试数据
	_, err = db.DB.Exec(`
		INSERT INTO extended_null_tests (name)
		VALUES (?)
	`, "Test Extended")
	if err != nil {
		t.Fatal(err)
	}

	// 测试使用Into映射到结构体
	var model ExtendedNullTestModel
	err = db.Chain().Table("extended_null_tests").First().Into(&model)
	if err != nil {
		t.Fatal(err)
	}

	// 验证所有NULL值已被正确处理
	assert.Equal(t, "Test Extended", model.Name)
	assert.Equal(t, "", model.ParentID)           // NULL字符串应该变成空字符串
	assert.Equal(t, 0, model.Age)                 // NULL整数应该变成0
	assert.Equal(t, 0.0, model.Score)             // NULL浮点数应该变成0.0
	assert.Equal(t, false, model.IsActive)        // NULL布尔值应该变成false
	assert.Equal(t, time.Time{}, model.CreatedAt) // NULL时间应该变成零时间
	assert.Equal(t, "", model.JsonData)           // NULL JSON应该变成空字符串

	// 获取原始数据，查看值的类型
	result := db.Chain().Table("extended_null_tests").First()
	if result.Error != nil {
		t.Fatal(result.Error)
	}

	// 调试输出
	row := result.Data[0]
	for k, v := range row {
		t.Logf("调试信息 - 字段: %s, 类型: %T, 值: %v", k, v, v)
	}

	// 测试使用指针类型处理不同类型的NULL
	type ExtendedNullWithPointers struct {
		ID        int64      `gom:"id"`
		ParentID  *string    `gom:"parent_id"`
		Age       *int       `gom:"age"`
		Score     *float64   `gom:"score"`
		IsActive  *bool      `gom:"is_active"`
		CreatedAt *time.Time `gom:"created_at"`
		JsonData  *string    `gom:"json_data"`
	}

	var ptrModel ExtendedNullWithPointers
	err = db.Chain().Table("extended_null_tests").First().Into(&ptrModel)
	if err != nil {
		t.Fatal(err)
	}

	// 验证指针类型正确处理NULL
	assert.Nil(t, ptrModel.ParentID)
	assert.Nil(t, ptrModel.Age)
	assert.Nil(t, ptrModel.Score)
	assert.Nil(t, ptrModel.IsActive)
	assert.Nil(t, ptrModel.CreatedAt)
	assert.Nil(t, ptrModel.JsonData)

	// 测试使用sql.Null*类型处理NULL
	type ExtendedNullWithSqlNull struct {
		ID        int64           `gom:"id"`
		ParentID  sql.NullString  `gom:"parent_id"`
		Age       sql.NullInt64   `gom:"age"`
		Score     sql.NullFloat64 `gom:"score"`
		IsActive  sql.NullBool    `gom:"is_active"`
		CreatedAt sql.NullTime    `gom:"created_at"`
		JsonData  sql.NullString  `gom:"json_data"`
	}

	var sqlNullModel ExtendedNullWithSqlNull
	err = db.Chain().Table("extended_null_tests").First().Into(&sqlNullModel)
	if err != nil {
		t.Fatal(err)
	}

	// 验证sql.Null*类型正确处理NULL
	assert.False(t, sqlNullModel.ParentID.Valid)
	assert.False(t, sqlNullModel.Age.Valid)
	assert.False(t, sqlNullModel.Score.Valid)
	assert.False(t, sqlNullModel.IsActive.Valid)
	assert.False(t, sqlNullModel.CreatedAt.Valid)
	assert.False(t, sqlNullModel.JsonData.Valid)

	// 清理
	_, err = db.DB.Exec("DROP TABLE extended_null_tests")
	if err != nil {
		t.Fatal(err)
	}
}

func TestListMethodNullHandling(t *testing.T) {
	db := getDB()
	if db == nil {
		t.Fatal("Failed to initialize test database")
	}
	defer db.Close()

	// 创建测试表
	_, err := db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS list_null_tests (
			id INTEGER PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			parent_id VARCHAR(255) NULL,
			age INTEGER NULL,
			score FLOAT NULL,
			is_active BOOLEAN NULL,
			created_at TIMESTAMP NULL,
			json_data JSON NULL
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// 插入具有NULL值的测试数据
	_, err = db.DB.Exec(`
		INSERT INTO list_null_tests (name)
		VALUES (?)
	`, "Test List Null")
	if err != nil {
		t.Fatal(err)
	}

	// 使用List方法获取结果
	result := db.Chain().Table("list_null_tests").List()
	if result.Error != nil {
		t.Fatal(result.Error)
	}

	if len(result.Data) == 0 {
		t.Fatal("No data returned")
	}

	row := result.Data[0]
	t.Logf("Row data: %+v", row)

	// 验证NULL值是否使用sql.Null*类型表示
	validateSqlNullType(t, row, "parent_id", sql.NullString{})
	validateSqlNullType(t, row, "age", sql.NullInt64{})
	validateSqlNullType(t, row, "score", sql.NullFloat64{})
	validateSqlNullBoolOrInt64(t, row, "is_active") // MySQL可能使用NullInt64表示布尔值
	validateSqlNullType(t, row, "created_at", sql.NullTime{})
	validateSqlNullType(t, row, "json_data", sql.NullString{})

	// 测试使用Into方法的情况
	var model struct {
		ID        int64     `gom:"id"`
		Name      string    `gom:"name"`
		ParentID  string    `gom:"parent_id"`
		Age       int       `gom:"age"`
		Score     float64   `gom:"score"`
		IsActive  bool      `gom:"is_active"`
		CreatedAt time.Time `gom:"created_at"`
		JsonData  string    `gom:"json_data"`
	}

	// 使用First方法来获取单条记录
	singleResult := db.Chain().Table("list_null_tests").First()
	if singleResult.Error != nil {
		t.Fatal(singleResult.Error)
	}

	err = singleResult.Into(&model)
	if err != nil {
		t.Fatal(err)
	}

	// 验证NULL值被转换为相应的零值
	assert.Equal(t, "Test List Null", model.Name)
	assert.Equal(t, "", model.ParentID)
	assert.Equal(t, 0, model.Age)
	assert.Equal(t, 0.0, model.Score)
	assert.Equal(t, false, model.IsActive)
	assert.Equal(t, time.Time{}, model.CreatedAt)
	assert.Equal(t, "", model.JsonData)

	// 测试指针类型
	var ptrModel struct {
		ID        int64      `gom:"id"`
		Name      string     `gom:"name"`
		ParentID  *string    `gom:"parent_id"`
		Age       *int       `gom:"age"`
		Score     *float64   `gom:"score"`
		IsActive  *bool      `gom:"is_active"`
		CreatedAt *time.Time `gom:"created_at"`
		JsonData  *string    `gom:"json_data"`
	}

	err = singleResult.Into(&ptrModel)
	if err != nil {
		t.Fatal(err)
	}

	// 验证NULL值被转换为nil
	assert.Equal(t, "Test List Null", ptrModel.Name)
	assert.Nil(t, ptrModel.ParentID)
	assert.Nil(t, ptrModel.Age)
	assert.Nil(t, ptrModel.Score)
	assert.Nil(t, ptrModel.IsActive)
	assert.Nil(t, ptrModel.CreatedAt)
	assert.Nil(t, ptrModel.JsonData)

	// 清理
	_, err = db.DB.Exec("DROP TABLE list_null_tests")
	if err != nil {
		t.Fatal(err)
	}
}

// 辅助函数：验证字段是否为指定的sql.Null*类型
func validateSqlNullType(t *testing.T, row map[string]interface{}, fieldName string, expectedType interface{}) {
	t.Helper()
	value, exists := row[fieldName]
	if !exists {
		t.Errorf("Field %s does not exist", fieldName)
		return
	}

	if value == nil {
		t.Errorf("Field %s is nil, expected sql.Null* type", fieldName)
		return
	}

	expectedTypeName := reflect.TypeOf(expectedType).String()
	actualTypeName := reflect.TypeOf(value).String()

	t.Logf("Field %s: expected type %s, got %s", fieldName, expectedTypeName, actualTypeName)

	if !reflect.TypeOf(value).AssignableTo(reflect.TypeOf(expectedType)) {
		t.Errorf("Field %s type mismatch: expected %T, got %T", fieldName, expectedType, value)
		return
	}

	// 验证Valid字段为false
	if v := reflect.ValueOf(value); v.Kind() == reflect.Struct {
		validField := v.FieldByName("Valid")
		if !validField.IsValid() {
			t.Errorf("Field %s does not have a Valid field", fieldName)
			return
		}

		if validField.Bool() {
			t.Errorf("Field %s Valid field is true, expected false for NULL value", fieldName)
		}
	} else {
		t.Errorf("Field %s is not a struct, so cannot access Valid field", fieldName)
	}
}

// 辅助函数：验证字段是否为布尔类型的NULL值
func validateSqlNullBoolOrInt64(t *testing.T, row map[string]interface{}, fieldName string) {
	t.Helper()
	value, exists := row[fieldName]
	if !exists {
		t.Errorf("Field %s does not exist", fieldName)
		return
	}

	if value == nil {
		t.Errorf("Field %s is nil, expected sql.Null* type", fieldName)
		return
	}

	// MySQL可能使用NullInt64表示布尔值
	_, isNullBool := value.(sql.NullBool)
	_, isNullInt64 := value.(sql.NullInt64)

	t.Logf("Field %s type: %T", fieldName, value)

	if !isNullBool && !isNullInt64 {
		t.Errorf("Field %s type mismatch: expected sql.NullBool or sql.NullInt64, got %T", fieldName, value)
		return
	}

	// 验证Valid字段为false
	if v := reflect.ValueOf(value); v.Kind() == reflect.Struct {
		validField := v.FieldByName("Valid")
		if !validField.IsValid() {
			t.Errorf("Field %s does not have a Valid field", fieldName)
			return
		}

		if validField.Bool() {
			t.Errorf("Field %s Valid field is true, expected false for NULL value", fieldName)
		}
	} else {
		t.Errorf("Field %s is not a struct, so cannot access Valid field", fieldName)
	}
}

// 添加一个更全面的测试，验证NULL值处理在各种情况下都能正常工作
func TestComprehensiveNullHandling(t *testing.T) {
	db := getDB()
	if db == nil {
		t.Fatal("Failed to initialize test database")
	}
	defer db.Close()

	// 创建测试表
	_, err := db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS comprehensive_null_tests (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			null_string VARCHAR(255) NULL,
			null_int INT NULL,
			null_float FLOAT NULL,
			null_bool BOOLEAN NULL,
			null_date DATETIME NULL,
			null_json JSON NULL
		)
	`)
	assert.NoError(t, err)
	defer db.DB.Exec("DROP TABLE comprehensive_null_tests")

	// 插入包含NULL值的测试数据
	_, err = db.DB.Exec(`
		INSERT INTO comprehensive_null_tests 
		(null_string, null_int, null_float, null_bool, null_date, null_json)
		VALUES (NULL, NULL, NULL, NULL, NULL, NULL)
	`)
	assert.NoError(t, err)

	// 插入包含非NULL值的测试数据
	_, err = db.DB.Exec(`
		INSERT INTO comprehensive_null_tests 
		(null_string, null_int, null_float, null_bool, null_date, null_json)
		VALUES ('test string', 42, 3.14, 1, '2025-01-01 12:00:00', '{"key": "value"}')
	`)
	assert.NoError(t, err)

	// 使用List()方法获取结果
	result := db.Chain().Table("comprehensive_null_tests").List()
	assert.NoError(t, result.Error)
	assert.Equal(t, 2, len(result.Data))

	// 验证第一行（全NULL）
	nullRow := result.Data[0]

	// 检查NULL值是否被正确识别为sql.Null*类型
	validateNullField(t, nullRow, "null_string", "sql.NullString", false)
	validateNullField(t, nullRow, "null_int", "sql.NullInt64", false)
	validateNullField(t, nullRow, "null_float", "sql.NullFloat64", false)
	validateNullField(t, nullRow, "null_bool", "sql.NullBool", false)
	validateNullField(t, nullRow, "null_date", "sql.NullTime", false)

	// 验证第二行（非NULL值）
	nonNullRow := result.Data[1]

	// 检查非NULL值是否被正确识别为sql.Null*类型，且Valid为true
	validateNonNullField(t, nonNullRow, "null_string", "sql.NullString", "test string")
	validateNonNullField(t, nonNullRow, "null_int", "sql.NullInt64", int64(42))
	validateNonNullField(t, nonNullRow, "null_float", "sql.NullFloat64", float64(3.14))
	validateNonNullField(t, nonNullRow, "null_bool", "sql.NullBool", true)

	// 测试Into方法将NULL值转换为结构体字段
	type TestStruct struct {
		ID         int64           `gom:"id"`
		NullString sql.NullString  `gom:"null_string"`
		NullInt    sql.NullInt64   `gom:"null_int"`
		NullFloat  sql.NullFloat64 `gom:"null_float"`
		NullBool   sql.NullBool    `gom:"null_bool"`
		NullDate   sql.NullTime    `gom:"null_date"`
		NullJSON   sql.NullString  `gom:"null_json"`
	}

	// 使用First()方法获取第一行（全NULL）
	nullResult := db.Chain().Table("comprehensive_null_tests").Where("id", define.OpEq, 1).First()

	var nullModel TestStruct
	err = nullResult.Into(&nullModel)
	assert.NoError(t, err)

	// 验证所有字段的Valid值为false
	assert.False(t, nullModel.NullString.Valid)
	assert.False(t, nullModel.NullInt.Valid)
	assert.False(t, nullModel.NullFloat.Valid)
	assert.False(t, nullModel.NullBool.Valid)
	assert.False(t, nullModel.NullDate.Valid)
	assert.False(t, nullModel.NullJSON.Valid)

	// 使用First()方法获取第二行（非NULL值）
	nonNullResult := db.Chain().Table("comprehensive_null_tests").Where("id", define.OpEq, 2).First()

	var nonNullModel TestStruct
	err = nonNullResult.Into(&nonNullModel)
	assert.NoError(t, err)

	// 验证所有字段的Valid值为true，且值正确
	assert.True(t, nonNullModel.NullString.Valid)
	assert.Equal(t, "test string", nonNullModel.NullString.String)

	assert.True(t, nonNullModel.NullInt.Valid)
	assert.Equal(t, int64(42), nonNullModel.NullInt.Int64)

	assert.True(t, nonNullModel.NullFloat.Valid)
	assert.InDelta(t, 3.14, nonNullModel.NullFloat.Float64, 0.01)

	assert.True(t, nonNullModel.NullBool.Valid)
	assert.True(t, nonNullModel.NullBool.Bool)

	assert.True(t, nonNullModel.NullDate.Valid)

	assert.True(t, nonNullModel.NullJSON.Valid)
	assert.Contains(t, nonNullModel.NullJSON.String, "key")

	// 测试指针类型的NULL处理
	type PtrTestStruct struct {
		ID         int64      `gom:"id"`
		NullString *string    `gom:"null_string"`
		NullInt    *int64     `gom:"null_int"`
		NullFloat  *float64   `gom:"null_float"`
		NullBool   *bool      `gom:"null_bool"`
		NullDate   *time.Time `gom:"null_date"`
		NullJSON   *string    `gom:"null_json"`
	}

	// 验证NULL值处理：对于指针类型，NULL值应该转换为nil
	var nullPtrModel PtrTestStruct
	err = nullResult.Into(&nullPtrModel)
	assert.NoError(t, err)

	assert.Nil(t, nullPtrModel.NullString)
	assert.Nil(t, nullPtrModel.NullInt)
	assert.Nil(t, nullPtrModel.NullFloat)
	assert.Nil(t, nullPtrModel.NullBool)
	assert.Nil(t, nullPtrModel.NullDate)
	assert.Nil(t, nullPtrModel.NullJSON)

	// 验证非NULL值处理：对于指针类型，非NULL值应该转换为指向实际值的指针
	var nonNullPtrModel PtrTestStruct
	err = nonNullResult.Into(&nonNullPtrModel)
	assert.NoError(t, err)

	assert.NotNil(t, nonNullPtrModel.NullString)
	assert.Equal(t, "test string", *nonNullPtrModel.NullString)

	assert.NotNil(t, nonNullPtrModel.NullInt)
	assert.Equal(t, int64(42), *nonNullPtrModel.NullInt)

	assert.NotNil(t, nonNullPtrModel.NullFloat)
	assert.InDelta(t, 3.14, *nonNullPtrModel.NullFloat, 0.01)

	assert.NotNil(t, nonNullPtrModel.NullBool)
	assert.True(t, *nonNullPtrModel.NullBool)

	assert.NotNil(t, nonNullPtrModel.NullDate)

	assert.NotNil(t, nonNullPtrModel.NullJSON)
	assert.Contains(t, *nonNullPtrModel.NullJSON, "key")
}

// 辅助函数：验证字段是否为NULL
func validateNullField(t *testing.T, row map[string]interface{}, fieldName, expectedType string, expectValid bool) {
	value, exists := row[fieldName]
	assert.True(t, exists, "Field %s should exist", fieldName)

	if !exists {
		return
	}

	// 获取类型名称
	typeName := reflect.TypeOf(value).String()
	t.Logf("字段 %s 的类型是 %s", fieldName, typeName)

	// 验证是否为sql.Null*类型
	switch {
	case strings.Contains(fieldName, "int") || strings.Contains(fieldName, "year") || strings.Contains(fieldName, "bit"):
		assert.Equal(t, "sql.NullInt64", typeName, "字段 %s 应该是 sql.NullInt64 类型", fieldName)
	case strings.Contains(fieldName, "decimal") || strings.Contains(fieldName, "float") || strings.Contains(fieldName, "double"):
		assert.Equal(t, "sql.NullFloat64", typeName, "字段 %s 应该是 sql.NullFloat64 类型", fieldName)
	case strings.Contains(fieldName, "char") || strings.Contains(fieldName, "text") || strings.Contains(fieldName, "enum") ||
		strings.Contains(fieldName, "set") || strings.Contains(fieldName, "json"):
		assert.Equal(t, "sql.NullString", typeName, "字段 %s 应该是 sql.NullString 类型", fieldName)
	case strings.Contains(fieldName, "date") || strings.Contains(fieldName, "time"):
		assert.Equal(t, "sql.NullTime", typeName, "字段 %s 应该是 sql.NullTime 类型", fieldName)
	case strings.Contains(fieldName, "bool"):
		// MySQL可能使用NullInt64表示布尔值
		assert.True(t, typeName == "sql.NullBool" || typeName == "sql.NullInt64",
			"字段 %s 应该是 sql.NullBool 或 sql.NullInt64 类型", fieldName)
	case strings.Contains(fieldName, "binary") || strings.Contains(fieldName, "blob"):
		assert.Equal(t, "sql.NullString", typeName, "字段 %s 应该是 sql.NullString 类型", fieldName)
	}

	// 只有结构体类型才能检查Valid字段
	if reflect.TypeOf(value).Kind() == reflect.Struct {
		validField := reflect.ValueOf(value).FieldByName("Valid")
		if validField.IsValid() {
			assert.Equal(t, expectValid, validField.Bool(), "字段 %s 的Valid值应该为%v", fieldName, expectValid)
		}
	}
}

// 辅助函数：验证非NULL字段
func validateNonNullField(t *testing.T, row map[string]interface{}, fieldName, expectedType string, expectedValue interface{}) {
	validateNullField(t, row, fieldName, expectedType, true)

	value, exists := row[fieldName]
	if !exists {
		t.Fatalf("字段 %s 不存在", fieldName)
		return
	}

	valueValue := reflect.ValueOf(value)

	// 检查值是否为结构体类型
	if valueValue.Kind() != reflect.Struct {
		t.Fatalf("字段 %s 的值不是结构体类型, 实际类型: %s", fieldName, valueValue.Type().String())
		return
	}

	// 检查实际类型
	actualType := valueValue.Type().String()
	var actualValue interface{}
	var fieldIsValid bool
	var field reflect.Value

	// 特殊处理布尔类型，MySQL可能使用NullInt64表示布尔值
	if expectedType == "sql.NullBool" && actualType == "sql.NullInt64" {
		// 如果期望是布尔型但实际是整型，尝试获取Int64字段
		field = valueValue.FieldByName("Int64")
		if field.IsValid() && field.CanInterface() {
			intValue := field.Interface().(int64)
			// 转换为布尔值(0=false, 非0=true)
			actualValue = intValue != 0
			fieldIsValid = true
		} else {
			t.Fatalf("字段 %s 没有有效的Int64字段", fieldName)
			return
		}
	} else {
		// 标准处理
		switch expectedType {
		case "sql.NullString":
			field = valueValue.FieldByName("String")
			if field.IsValid() && field.CanInterface() {
				actualValue = field.Interface()
				fieldIsValid = true
			} else {
				t.Fatalf("字段 %s 没有有效的String字段", fieldName)
				return
			}
		case "sql.NullInt64":
			field = valueValue.FieldByName("Int64")
			if field.IsValid() && field.CanInterface() {
				actualValue = field.Interface()
				fieldIsValid = true
			} else {
				t.Fatalf("字段 %s 没有有效的Int64字段", fieldName)
				return
			}
		case "sql.NullFloat64":
			field = valueValue.FieldByName("Float64")
			if field.IsValid() && field.CanInterface() {
				actualValue = field.Interface()
				fieldIsValid = true
			} else {
				t.Fatalf("字段 %s 没有有效的Float64字段", fieldName)
				return
			}
		case "sql.NullBool":
			field = valueValue.FieldByName("Bool")
			if field.IsValid() && field.CanInterface() {
				actualValue = field.Interface()
				fieldIsValid = true
			} else {
				t.Fatalf("字段 %s 没有有效的Bool字段", fieldName)
				return
			}
		case "sql.NullTime":
			field = valueValue.FieldByName("Time")
			if field.IsValid() && field.CanInterface() {
				actualValue = field.Interface()
				fieldIsValid = true
			} else {
				t.Fatalf("字段 %s 没有有效的Time字段", fieldName)
				return
			}
		}
	}

	if !fieldIsValid {
		t.Fatalf("字段 %s 不能获取有效值", fieldName)
		return
	}

	switch expectedValue.(type) {
	case float64:
		assert.InDelta(t, expectedValue.(float64), actualValue.(float64), 0.01)
	default:
		assert.Equal(t, expectedValue, actualValue)
	}
}

// 编写一个新的测试，专门测试所有数据类型列为NULL的情况
func TestComprehensiveNullTypesInList(t *testing.T) {
	db := getDB()
	if db == nil {
		t.Fatal("Failed to initialize test database")
	}
	defer db.Close()

	// 创建一个包含各种数据类型的测试表
	_, err := db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS all_null_types_test (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			null_tinyint TINYINT NULL,
			null_smallint SMALLINT NULL,
			null_mediumint MEDIUMINT NULL,
			null_int INT NULL,
			null_bigint BIGINT NULL,
			null_decimal DECIMAL(10, 2) NULL,
			null_float FLOAT NULL,
			null_double DOUBLE NULL,
			null_bit BIT(1) NULL,
			null_char CHAR(10) NULL,
			null_varchar VARCHAR(255) NULL,
			null_text TEXT NULL,
			null_date DATE NULL,
			null_time TIME NULL,
			null_datetime DATETIME NULL,
			null_timestamp TIMESTAMP NULL,
			null_year YEAR NULL,
			null_enum ENUM('value1', 'value2') NULL,
			null_set SET('value1', 'value2') NULL,
			null_binary BINARY(10) NULL,
			null_varbinary VARBINARY(255) NULL,
			null_blob BLOB NULL,
			null_json JSON NULL,
			null_bool BOOLEAN NULL
		)
	`)
	assert.NoError(t, err)
	defer db.DB.Exec("DROP TABLE all_null_types_test")

	// 插入全NULL的测试数据
	_, err = db.DB.Exec(`
		INSERT INTO all_null_types_test (
			null_tinyint, null_smallint, null_mediumint, null_int, null_bigint,
			null_decimal, null_float, null_double, null_bit, null_char,
			null_varchar, null_text, null_date, null_time, null_datetime,
			null_timestamp, null_year, null_enum, null_set, null_binary,
			null_varbinary, null_blob, null_json, null_bool
		) VALUES (
			NULL, NULL, NULL, NULL, NULL,
			NULL, NULL, NULL, NULL, NULL,
			NULL, NULL, NULL, NULL, NULL,
			NULL, NULL, NULL, NULL, NULL,
			NULL, NULL, NULL, NULL
		)
	`)
	assert.NoError(t, err)

	// 使用List方法获取结果
	result := db.Chain().Table("all_null_types_test").List()
	assert.NoError(t, result.Error)
	assert.Equal(t, 1, len(result.Data))

	// 验证所有字段都被正确识别为NULL
	row := result.Data[0]

	t.Log("检查所有字段都被正确识别为sql.Null*类型且Valid为false")
	for field, value := range row {
		if field == "id" {
			continue // 跳过主键字段
		}

		if value == nil {
			t.Errorf("字段 %s 是nil，应该是sql.Null*类型", field)
			continue
		}

		// 获取类型名称
		valueType := reflect.TypeOf(value)
		typeName := valueType.String()
		t.Logf("字段 %s 的类型是 %s", field, typeName)

		// 验证是否为结构体类型且有Valid字段
		isStructWithValidField := false
		if valueType.Kind() == reflect.Struct {
			validField := reflect.ValueOf(value).FieldByName("Valid")
			if validField.IsValid() && validField.Kind() == reflect.Bool {
				isStructWithValidField = true
				// 验证Valid字段为false
				assert.False(t, validField.Bool(), "字段 %s 的Valid值应该为false", field)
			}
		}

		if !isStructWithValidField {
			t.Logf("警告: 字段 %s 不是sql.Null*类型，其类型为 %s", field, typeName)
		}
	}

	// 测试使用Into方法从全NULL的记录创建不同类型的结构体

	// 1. 使用基本类型的结构体
	type BasicStruct struct {
		ID           int64     `gom:"id"`
		NullTinyInt  int       `gom:"null_tinyint"`
		NullInt      int       `gom:"null_int"`
		NullFloat    float64   `gom:"null_float"`
		NullVarchar  string    `gom:"null_varchar"`
		NullDatetime time.Time `gom:"null_datetime"`
		NullBool     bool      `gom:"null_bool"`
	}

	var basicModel BasicStruct
	err = result.First().Into(&basicModel)
	assert.NoError(t, err)

	// 验证NULL值被转换为零值
	assert.Equal(t, 0, basicModel.NullTinyInt)
	assert.Equal(t, 0, basicModel.NullInt)
	assert.Equal(t, 0.0, basicModel.NullFloat)
	assert.Equal(t, "", basicModel.NullVarchar)
	assert.Equal(t, time.Time{}, basicModel.NullDatetime)
	assert.Equal(t, false, basicModel.NullBool)

	// 2. 使用指针类型的结构体
	type PtrStruct struct {
		ID           int64      `gom:"id"`
		NullTinyInt  *int       `gom:"null_tinyint"`
		NullInt      *int       `gom:"null_int"`
		NullFloat    *float64   `gom:"null_float"`
		NullVarchar  *string    `gom:"null_varchar"`
		NullDatetime *time.Time `gom:"null_datetime"`
		NullBool     *bool      `gom:"null_bool"`
	}

	var ptrModel PtrStruct
	err = result.First().Into(&ptrModel)
	assert.NoError(t, err)

	// 验证NULL值被转换为nil指针
	assert.Nil(t, ptrModel.NullTinyInt)
	assert.Nil(t, ptrModel.NullInt)
	assert.Nil(t, ptrModel.NullFloat)
	assert.Nil(t, ptrModel.NullVarchar)
	assert.Nil(t, ptrModel.NullDatetime)
	assert.Nil(t, ptrModel.NullBool)

	// 3. 使用sql.Null*类型的结构体
	type SqlNullStruct struct {
		ID           int64           `gom:"id"`
		NullTinyInt  sql.NullInt64   `gom:"null_tinyint"`
		NullInt      sql.NullInt64   `gom:"null_int"`
		NullFloat    sql.NullFloat64 `gom:"null_float"`
		NullVarchar  sql.NullString  `gom:"null_varchar"`
		NullDatetime sql.NullTime    `gom:"null_datetime"`
		NullBool     sql.NullBool    `gom:"null_bool"`
	}

	var sqlNullModel SqlNullStruct
	err = result.First().Into(&sqlNullModel)
	assert.NoError(t, err)

	// 验证所有字段的Valid值为false
	assert.False(t, sqlNullModel.NullTinyInt.Valid)
	assert.False(t, sqlNullModel.NullInt.Valid)
	assert.False(t, sqlNullModel.NullFloat.Valid)
	assert.False(t, sqlNullModel.NullVarchar.Valid)
	assert.False(t, sqlNullModel.NullDatetime.Valid)
	assert.False(t, sqlNullModel.NullBool.Valid)

	// 4. 验证通过List处理多条记录时NULL值的处理
	// 插入一些混合NULL和非NULL值的记录
	_, err = db.DB.Exec(`
		INSERT INTO all_null_types_test (
			null_tinyint, null_smallint, null_int, null_float, null_varchar, null_datetime, null_bool
		) VALUES (
			1, 2, 3, 4.5, 'test string', '2025-01-01 12:00:00', 1
		)
	`)
	assert.NoError(t, err)

	_, err = db.DB.Exec(`
		INSERT INTO all_null_types_test (
			null_tinyint, null_smallint, null_int, null_float, null_varchar
		) VALUES (
			10, 20, NULL, 40.5, NULL
		)
	`)
	assert.NoError(t, err)

	// 获取所有记录
	mixedResult := db.Chain().Table("all_null_types_test").List()
	assert.NoError(t, mixedResult.Error)
	assert.Equal(t, 3, len(mixedResult.Data))

	// 使用sql.Null*类型结构体的切片接收结果
	var sqlNullModels []SqlNullStruct
	err = mixedResult.Into(&sqlNullModels)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(sqlNullModels))

	// 验证第一条记录（全部是NULL）
	assert.False(t, sqlNullModels[0].NullInt.Valid)
	assert.False(t, sqlNullModels[0].NullVarchar.Valid)

	// 验证第二条记录（全部非NULL）
	assert.True(t, sqlNullModels[1].NullInt.Valid)
	assert.Equal(t, int64(3), sqlNullModels[1].NullInt.Int64)
	assert.True(t, sqlNullModels[1].NullVarchar.Valid)
	assert.Equal(t, "test string", sqlNullModels[1].NullVarchar.String)

	// 验证第三条记录（混合NULL和非NULL）
	assert.False(t, sqlNullModels[2].NullInt.Valid)     // NULL
	assert.False(t, sqlNullModels[2].NullVarchar.Valid) // NULL
	assert.True(t, sqlNullModels[2].NullFloat.Valid)    // 非NULL
	assert.Equal(t, 40.5, sqlNullModels[2].NullFloat.Float64)

	// 使用指针类型结构体的切片接收结果
	var ptrModels []PtrStruct
	err = mixedResult.Into(&ptrModels)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(ptrModels))

	// 验证指针类型也可以正确处理NULL和非NULL值
	assert.Nil(t, ptrModels[0].NullInt)    // 第一条记录，NULL
	assert.NotNil(t, ptrModels[1].NullInt) // 第二条记录，非NULL
	assert.Equal(t, 3, *ptrModels[1].NullInt)
	assert.Nil(t, ptrModels[2].NullInt) // 第三条记录，NULL
}
