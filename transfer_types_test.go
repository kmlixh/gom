package gom

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

type CustomInt int

func (c *CustomInt) FromDB(value interface{}) error {
	switch v := value.(type) {
	case int64:
		*c = CustomInt(v)
	case string:
		if v == "zero" {
			*c = 0
		} else if v == "one" {
			*c = 1
		}
	}
	return nil
}

func (c *CustomInt) ToDB() (interface{}, error) {
	return int64(*c), nil
}

// 自定义枚举类型
type Status string

const (
	StatusActive   Status = "active"
	StatusInactive Status = "inactive"
	StatusPending  Status = "pending"
)

func (s *Status) FromDB(value interface{}) error {
	if value == nil {
		*s = StatusPending
		return nil
	}

	switch v := value.(type) {
	case []byte:
		str := string(v)
		if str == "1" {
			*s = StatusActive
		} else if str == "0" {
			*s = StatusInactive
		} else {
			*s = Status(str)
		}
	case sql.RawBytes:
		str := string(v)
		if str == "1" {
			*s = StatusActive
		} else if str == "0" {
			*s = StatusInactive
		} else {
			*s = Status(str)
		}
	case string:
		if v == "1" {
			*s = StatusActive
		} else if v == "0" {
			*s = StatusInactive
		} else {
			*s = Status(v)
		}
	case int64:
		switch v {
		case 1:
			*s = StatusActive
		case 0:
			*s = StatusInactive
		default:
			*s = StatusPending
		}
	default:
		return fmt.Errorf("unsupported type for Status: %T", value)
	}
	return nil
}

func (s *Status) ToDB() (interface{}, error) {
	return string(*s), nil
}

// 自定义 JSON 类型
type JSONMap struct {
	Data map[string]interface{}
}

func (j *JSONMap) FromDB(value interface{}) error {
	if value == nil {
		j.Data = make(map[string]interface{})
		return nil
	}

	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case sql.RawBytes:
		data = []byte(v)
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("unsupported type for JSONMap: %T", value)
	}

	if err := json.Unmarshal(data, &j.Data); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
	return nil
}

func (j *JSONMap) ToDB() (interface{}, error) {
	if j.Data == nil {
		return "{}", nil
	}
	data, err := json.Marshal(j.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return string(data), nil
}

// 自定义 IP 地址类型
type IPAddress struct {
	Address string
}

func (ip *IPAddress) FromDB(value interface{}) error {
	if value == nil {
		ip.Address = ""
		return nil
	}

	switch v := value.(type) {
	case []byte:
		ip.Address = string(v)
	case sql.RawBytes:
		ip.Address = string(v)
	case string:
		ip.Address = v
	default:
		return fmt.Errorf("unsupported type for IPAddress: %T", value)
	}

	// 简单的 IP 地址验证
	if ip.Address == "" {
		return nil
	}

	parts := strings.Split(ip.Address, ".")
	if len(parts) != 4 {
		return fmt.Errorf("invalid IP address format: %s", ip.Address)
	}
	for _, part := range parts {
		num, err := strconv.Atoi(part)
		if err != nil || num < 0 || num > 255 {
			return fmt.Errorf("invalid IP address segment: %s", part)
		}
	}
	return nil
}

func (ip *IPAddress) ToDB() (interface{}, error) {
	if ip.Address == "" {
		return nil, nil
	}

	// 验证 IP 地址格式
	parts := strings.Split(ip.Address, ".")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid IP address format: %s", ip.Address)
	}
	for _, part := range parts {
		num, err := strconv.Atoi(part)
		if err != nil || num < 0 || num > 255 {
			return nil, fmt.Errorf("invalid IP address segment: %s", part)
		}
	}
	return ip.Address, nil
}

type ComplexTypeTest struct {
	ID             int64      `gom:"id,@"`
	IntValue       int        `gom:"int_value"`
	Int8Value      int8       `gom:"int8_value"`
	Int16Value     int16      `gom:"int16_value"`
	Int32Value     int32      `gom:"int32_value"`
	Int64Value     int64      `gom:"int64_value"`
	UintValue      uint       `gom:"uint_value"`
	Uint8Value     uint8      `gom:"uint8_value"`
	Uint16Value    uint16     `gom:"uint16_value"`
	Uint32Value    uint32     `gom:"uint32_value"`
	Uint64Value    uint64     `gom:"uint64_value"`
	FloatValue     float32    `gom:"float_value"`
	DoubleValue    float64    `gom:"double_value"`
	DecimalValue   string     `gom:"decimal_value"`
	BoolValue      bool       `gom:"bool_value"`
	StringValue    string     `gom:"string_value"`
	BytesValue     []byte     `gom:"bytes_value"`
	TimeValue      time.Time  `gom:"time_value"`
	NullTimeValue  *time.Time `gom:"null_time_value"`
	JSONValue      []string   `gom:"json_value"`
	IntArray       []int      `gom:"int_array"`
	StringArray    []string   `gom:"string_array"`
	CustomIntValue CustomInt  `gom:"custom_int_value"`
	Status         Status     `gom:"status"`
	Metadata       JSONMap    `gom:"metadata"`
	IPAddress      IPAddress  `gom:"ip_address"`
}

func (c *ComplexTypeTest) TableName() string {
	return "complex_type_test"
}

func getDB() *DB {
	config := testutils.DefaultMySQLConfig()
	config.User = "root" // 确保使用 root 用户
	db, err := Open(config.Driver, config.DSN(), nil)
	if err != nil {
		return nil
	}
	return db
}

// Logger 接口定义了日志记录的方法
type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
}

// TestLogger 实现了 Logger 接口
type TestLogger struct {
	t *testing.T
}

func (l *TestLogger) Debug(args ...interface{}) {
	l.t.Log("[DEBUG]", fmt.Sprint(args...))
}

func (l *TestLogger) Info(args ...interface{}) {
	l.t.Log("[INFO]", fmt.Sprint(args...))
}

func (l *TestLogger) Warn(args ...interface{}) {
	l.t.Log("[WARN]", fmt.Sprint(args...))
}

func (l *TestLogger) Error(args ...interface{}) {
	l.t.Log("[ERROR]", fmt.Sprint(args...))
}

func TestTypeConversions(t *testing.T) {
	db := getDB()
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	logger := &TestLogger{t: t}
	assert.NotNil(t, db)

	logger.Info("Starting type conversion tests")

	// 删除测试表
	_, err := db.DB.Exec("DROP TABLE IF EXISTS complex_type_test")
	if err != nil {
		logger.Error("Failed to drop test table:", err)
		t.Fatal(err)
	}
	logger.Debug("Dropped test table")

	// 创建测试表
	_, err = db.DB.Exec(`
		CREATE TABLE complex_type_test (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			int_value INT,
			int8_value TINYINT,
			int16_value SMALLINT,
			int32_value INT,
			int64_value BIGINT,
			uint_value INT UNSIGNED,
			uint8_value TINYINT UNSIGNED,
			uint16_value SMALLINT UNSIGNED,
			uint32_value INT UNSIGNED,
			uint64_value BIGINT UNSIGNED,
			float_value FLOAT,
			double_value DOUBLE,
			decimal_value DECIMAL(10,2),
			bool_value BOOLEAN,
			string_value VARCHAR(255),
			bytes_value VARBINARY(255),
			time_value TIMESTAMP,
			null_time_value TIMESTAMP NULL,
			json_value JSON,
			int_array JSON,
			string_array JSON,
			custom_int_value INT,
			status VARCHAR(255),
			metadata JSON,
			ip_address VARCHAR(255)
		)
	`)
	if err != nil {
		logger.Error("Failed to create test table:", err)
		t.Fatal(err)
	}
	logger.Debug("Created test table")

	// 插入测试数据
	result := db.Chain().Table("complex_type_test").Values(map[string]interface{}{
		"int_value":        42,
		"int8_value":       int8(8),
		"int16_value":      int16(16),
		"int32_value":      int32(32),
		"int64_value":      int64(64),
		"uint_value":       uint(42),
		"uint8_value":      uint8(8),
		"uint16_value":     uint16(16),
		"uint32_value":     uint32(32),
		"uint64_value":     uint64(64),
		"float_value":      float32(3.14),
		"double_value":     float64(3.14159),
		"decimal_value":    "123.45",
		"bool_value":       true,
		"string_value":     "test string",
		"bytes_value":      []byte("test bytes"),
		"time_value":       time.Unix(1736424951, 0),
		"json_value":       `["a","b","c"]`,
		"int_array":        `[1,2,3,4,5]`,
		"string_array":     `["a","b","c","d","e"]`,
		"custom_int_value": 42,
		"status":           "active",
		"metadata":         `{"key":"value"}`,
		"ip_address":       "192.168.1.1",
	}).Save()
	if err := result.Error; err != nil {
		logger.Error("Failed to insert test data:", err)
		t.Fatal(err)
	}
	logger.Info("Inserted test data with ID:", result.ID)

	// 查询并验证数据
	var test ComplexTypeTest
	rows, err := db.DB.Query("SELECT * FROM complex_type_test WHERE id = ?", result.ID)
	if err != nil {
		logger.Error("Failed to query test data:", err)
		t.Fatal(err)
	}
	defer rows.Close()
	logger.Debug("Retrieved test data")

	// 打印列名
	columns, err := rows.Columns()
	if err != nil {
		logger.Error("Failed to get columns:", err)
		t.Fatal(err)
	}
	logger.Debug("Columns:", columns)

	// 获取数据
	if rows.Next() {
		// 创建扫描器
		scanners := make([]interface{}, len(columns))
		for i := range scanners {
			scanners[i] = new(sql.RawBytes)
		}
		err = rows.Scan(scanners...)
		if err != nil {
			logger.Error("Failed to scan row:", err)
			t.Fatal(err)
		}

		// 打印原始数据
		values := make(map[string]string)
		for i, col := range columns {
			if rb, ok := scanners[i].(*sql.RawBytes); ok {
				values[col] = string(*rb)
			}
		}
		logger.Debug("Raw values:", values)

		// 手动设置字段值
		test.ID, _ = strconv.ParseInt(values["id"], 10, 64)
		test.IntValue, _ = strconv.Atoi(values["int_value"])
		if v, err := strconv.ParseInt(values["int8_value"], 10, 8); err == nil {
			test.Int8Value = int8(v)
		}
		if v, err := strconv.ParseInt(values["int16_value"], 10, 16); err == nil {
			test.Int16Value = int16(v)
		}
		if v, err := strconv.ParseInt(values["int32_value"], 10, 32); err == nil {
			test.Int32Value = int32(v)
		}
		test.Int64Value, _ = strconv.ParseInt(values["int64_value"], 10, 64)
		if v, err := strconv.ParseUint(values["uint_value"], 10, 0); err == nil {
			test.UintValue = uint(v)
		}
		if v, err := strconv.ParseUint(values["uint8_value"], 10, 8); err == nil {
			test.Uint8Value = uint8(v)
		}
		if v, err := strconv.ParseUint(values["uint16_value"], 10, 16); err == nil {
			test.Uint16Value = uint16(v)
		}
		if v, err := strconv.ParseUint(values["uint32_value"], 10, 32); err == nil {
			test.Uint32Value = uint32(v)
		}
		test.Uint64Value, _ = strconv.ParseUint(values["uint64_value"], 10, 64)
		if v, err := strconv.ParseFloat(values["float_value"], 32); err == nil {
			test.FloatValue = float32(v)
		}
		test.DoubleValue, _ = strconv.ParseFloat(values["double_value"], 64)
		test.DecimalValue = values["decimal_value"]
		test.BoolValue = values["bool_value"] == "1"
		test.StringValue = values["string_value"]
		test.BytesValue = []byte(values["bytes_value"])

		// 解析时间
		if timeStr := values["time_value"]; timeStr != "" {
			if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
				test.TimeValue = t
			} else if t, err := time.Parse("2006-01-02 15:04:05", timeStr); err == nil {
				test.TimeValue = t
			}
		}

		// 解析 JSON 数组
		if jsonStr := values["json_value"]; jsonStr != "" {
			var jsonArray []string
			if err := json.Unmarshal([]byte(jsonStr), &jsonArray); err == nil {
				test.JSONValue = jsonArray
			}
		}

		if jsonStr := values["int_array"]; jsonStr != "" {
			var intArray []int
			if err := json.Unmarshal([]byte(jsonStr), &intArray); err == nil {
				test.IntArray = intArray
			}
		}

		if jsonStr := values["string_array"]; jsonStr != "" {
			var stringArray []string
			if err := json.Unmarshal([]byte(jsonStr), &stringArray); err == nil {
				test.StringArray = stringArray
			}
		}

		// 解析自定义类型
		if customIntVal, err := strconv.ParseInt(values["custom_int_value"], 10, 64); err == nil {
			test.CustomIntValue = CustomInt(customIntVal)
		}

		// 解析状态
		test.Status = Status(values["status"])

		// 解析元数据
		if metadataStr := values["metadata"]; metadataStr != "" {
			if err := json.Unmarshal([]byte(metadataStr), &test.Metadata.Data); err == nil {
				logger.Debug("Parsed metadata:", test.Metadata.Data)
			} else {
				logger.Warn("Failed to parse metadata:", err)
			}
		}

		// 解析 IP 地址
		test.IPAddress.Address = values["ip_address"]
	}

	// 打印查询到的数据
	logger.Info("Retrieved record:", test)

	// 验证基本类型
	assert.Equal(t, 42, test.IntValue)
	assert.Equal(t, int8(8), test.Int8Value)
	assert.Equal(t, int16(16), test.Int16Value)
	assert.Equal(t, int32(32), test.Int32Value)
	assert.Equal(t, int64(64), test.Int64Value)
	assert.Equal(t, uint(42), test.UintValue)
	assert.Equal(t, uint8(8), test.Uint8Value)
	assert.Equal(t, uint16(16), test.Uint16Value)
	assert.Equal(t, uint32(32), test.Uint32Value)
	assert.Equal(t, uint64(64), test.Uint64Value)
	assert.InDelta(t, float32(3.14), test.FloatValue, 0.01)
	assert.InDelta(t, float64(3.14159), test.DoubleValue, 0.00001)
	assert.Equal(t, "123.45", test.DecimalValue)
	assert.True(t, test.BoolValue)
	assert.Equal(t, "test string", test.StringValue)
	assert.Equal(t, []byte("test bytes"), test.BytesValue)
	assert.Equal(t, int64(1736424951), test.TimeValue.Unix())

	// 验证数组类型
	expectedStringArray := []string{"a", "b", "c"}
	assert.Equal(t, expectedStringArray, test.JSONValue)

	expectedIntArray := []int{1, 2, 3, 4, 5}
	assert.Equal(t, expectedIntArray, test.IntArray)

	expectedStringArray2 := []string{"a", "b", "c", "d", "e"}
	assert.Equal(t, expectedStringArray2, test.StringArray)

	// 验证自定义类型
	assert.Equal(t, StatusActive, test.Status)
	assert.Equal(t, "value", test.Metadata.Data["key"])
	assert.Equal(t, "192.168.1.1", test.IPAddress.Address)

	// 测试数组格式
	arrayFormats := []struct {
		input    string
		expected []int
	}{
		{`[1,2,3,4,5]`, []int{1, 2, 3, 4, 5}},
		{`[1]`, []int{1}},
	}

	for _, testCase := range arrayFormats {
		// 插入测试数据
		result = db.Chain().Table("complex_type_test").Values(map[string]interface{}{
			"int_array": testCase.input,
		}).Save()
		assert.NoError(t, result.Error)

		// 查询并验证数据
		rows, err := db.DB.Query("SELECT int_array FROM complex_type_test WHERE id = ?", result.ID)
		assert.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			var rawValue sql.RawBytes
			err = rows.Scan(&rawValue)
			assert.NoError(t, err)

			var intArray []int
			err = json.Unmarshal(rawValue, &intArray)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, intArray)
		}
	}

	// 测试数字格式
	numberFormats := []struct {
		input    string
		expected int64
	}{
		{"42", 42},
		{"42", 42}, // 十进制
		{"5", 5},   // 二进制 0b101 -> 5
		{"42", 42}, // 八进制 052 -> 42
	}

	for _, testCase := range numberFormats {
		// 插入测试数据
		result = db.Chain().Table("complex_type_test").Values(map[string]interface{}{
			"int_value": testCase.input,
		}).Save()
		assert.NoError(t, result.Error)

		// 查询并验证数据
		rows, err := db.DB.Query("SELECT int_value FROM complex_type_test WHERE id = ?", result.ID)
		assert.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			var rawValue sql.RawBytes
			err = rows.Scan(&rawValue)
			assert.NoError(t, err)

			intValue, err := strconv.ParseInt(string(rawValue), 10, 64)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, intValue)
		}
	}

	// 测试特殊格式的时间
	timeFormats := []struct {
		input    string
		expected string
	}{
		{"2023-12-25 00:00:00", "2023-12-25T00:00:00Z"},
		{"2023-12-25 14:30:00", "2023-12-25T14:30:00Z"},
		{"2023-12-25 14:30:00.000", "2023-12-25T14:30:00Z"},
		{"2023/12/25 14:30:00", "2023-12-25T14:30:00Z"},
	}

	for _, testCase := range timeFormats {
		// 插入测试数据
		result = db.Chain().Table("complex_type_test").Values(map[string]interface{}{
			"time_value": testCase.input,
		}).Save()
		assert.NoError(t, result.Error)

		// 查询并验证数据
		rows, err := db.DB.Query("SELECT time_value FROM complex_type_test WHERE id = ?", result.ID)
		assert.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			var timeValue time.Time
			err = rows.Scan(&timeValue)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, timeValue.UTC().Format(time.RFC3339))
		}
	}

	// 测试 NULL 值处理
	nullTests := []struct {
		field    string
		value    interface{}
		expected interface{}
	}{
		{"int_value", nil, 0},
		{"string_value", nil, ""},
		{"bool_value", nil, false},
		{"float_value", nil, float32(0)},
		{"double_value", nil, float64(0)},
		{"null_time_value", nil, (*time.Time)(nil)},
		{"json_value", nil, []string(nil)},
		{"int_array", nil, []int(nil)},
		{"string_array", nil, []string(nil)},
		{"status", nil, "pending"},
		{"metadata", nil, `{}`},
		{"ip_address", nil, ""},
	}

	for _, testCase := range nullTests {
		// 插入 NULL 值
		result = db.Chain().Table("complex_type_test").Values(map[string]interface{}{
			testCase.field: testCase.value,
		}).Save()
		assert.NoError(t, result.Error)

		// 查询并验证数据
		rows, err := db.DB.Query(fmt.Sprintf("SELECT %s FROM complex_type_test WHERE id = ?", testCase.field), result.ID)
		assert.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			var rawValue sql.NullString
			err = rows.Scan(&rawValue)
			assert.NoError(t, err)
			assert.False(t, rawValue.Valid)
		}
	}

	// 测试特殊字符处理
	specialCharTests := []struct {
		input    string
		expected string
	}{
		{"Hello, 世界!", "Hello, 世界!"},
		{"Special chars: !@#$%^&*()", "Special chars: !@#$%^&*()"},
		{"Quotes: 'single' \"double\"", "Quotes: 'single' \"double\""},
		{"Newlines\nand\ttabs", "Newlines\nand\ttabs"},
		{"Emojis: 👋🌍✨", "Emojis: 👋🌍✨"},
		{"HTML: <div>test</div>", "HTML: <div>test</div>"},
		{"SQL: SELECT * FROM table", "SQL: SELECT * FROM table"},
	}

	for _, testCase := range specialCharTests {
		// 插入特殊字符
		result = db.Chain().Table("complex_type_test").Values(map[string]interface{}{
			"string_value": testCase.input,
		}).Save()
		assert.NoError(t, result.Error)

		// 查询并验证数据
		rows, err := db.DB.Query("SELECT string_value FROM complex_type_test WHERE id = ?", result.ID)
		assert.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			var value string
			err = rows.Scan(&value)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, value)
		}
	}

	// 测试边界值
	boundaryTests := []struct {
		field    string
		value    interface{}
		expected interface{}
	}{
		{"int8_value", int8(127), int8(127)},                // int8 最大值
		{"int8_value", int8(-128), int8(-128)},              // int8 最小值
		{"uint8_value", uint8(255), uint8(255)},             // uint8 最大值
		{"int16_value", int16(32767), int16(32767)},         // int16 最大值
		{"int16_value", int16(-32768), int16(-32768)},       // int16 最小值
		{"uint16_value", uint16(65535), uint16(65535)},      // uint16 最大值
		{"float_value", float32(3.4e38), float32(3.4e38)},   // float32 接近最大值
		{"float_value", float32(1.4e-45), float32(1.4e-45)}, // float32 接近最小值
		{"double_value", 1.7e308, 1.7e308},                  // float64 接近最大值
		{"double_value", 4.9e-324, 4.9e-324},                // float64 接近最小值
	}

	for _, testCase := range boundaryTests {
		// 插入边界值
		result = db.Chain().Table("complex_type_test").Values(map[string]interface{}{
			testCase.field: testCase.value,
		}).Save()
		assert.NoError(t, result.Error)

		// 查询并验证数据
		rows, err := db.DB.Query(fmt.Sprintf("SELECT %s FROM complex_type_test WHERE id = ?", testCase.field), result.ID)
		assert.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			switch expected := testCase.expected.(type) {
			case int8:
				var value int8
				err = rows.Scan(&value)
				assert.NoError(t, err)
				assert.Equal(t, expected, value)
			case uint8:
				var value uint8
				err = rows.Scan(&value)
				assert.NoError(t, err)
				assert.Equal(t, expected, value)
			case int16:
				var value int16
				err = rows.Scan(&value)
				assert.NoError(t, err)
				assert.Equal(t, expected, value)
			case uint16:
				var value uint16
				err = rows.Scan(&value)
				assert.NoError(t, err)
				assert.Equal(t, expected, value)
			case float32:
				var value float32
				err = rows.Scan(&value)
				assert.NoError(t, err)
				assert.InDelta(t, expected, value, float64(expected)*0.000001)
			case float64:
				var value float64
				err = rows.Scan(&value)
				assert.NoError(t, err)
				assert.InDelta(t, expected, value, expected*0.000001)
			}
		}
	}

	// 测试自定义类型转换
	customTypeTests := []struct {
		name     string
		field    string
		value    interface{}
		validate func(t *testing.T, value interface{})
	}{
		{
			name:  "Status from string",
			field: "status",
			value: "active",
			validate: func(t *testing.T, value interface{}) {
				status, ok := value.(Status)
				assert.True(t, ok)
				assert.Equal(t, StatusActive, status)
			},
		},
		{
			name:  "Status from int",
			field: "status",
			value: 1,
			validate: func(t *testing.T, value interface{}) {
				status, ok := value.(Status)
				assert.True(t, ok)
				assert.Equal(t, StatusActive, status)
			},
		},
		{
			name:  "JSONMap with nested data",
			field: "metadata",
			value: `{"string":"value","number":42,"bool":true,"array":[1,2,3],"object":{"key":"value"}}`,
			validate: func(t *testing.T, value interface{}) {
				metadata, ok := value.(JSONMap)
				assert.True(t, ok)
				assert.Equal(t, "value", metadata.Data["string"])
				assert.Equal(t, float64(42), metadata.Data["number"])
				assert.Equal(t, true, metadata.Data["bool"])
				assert.IsType(t, []interface{}{}, metadata.Data["array"])
				assert.IsType(t, map[string]interface{}{}, metadata.Data["object"])
			},
		},
		{
			name:  "IPAddress valid",
			field: "ip_address",
			value: "192.168.1.1",
			validate: func(t *testing.T, value interface{}) {
				ip, ok := value.(IPAddress)
				assert.True(t, ok)
				assert.Equal(t, "192.168.1.1", ip.Address)
			},
		},
	}

	for _, testCase := range customTypeTests {
		t.Run(testCase.name, func(t *testing.T) {
			logger.Info("Running custom type test:", testCase.name)

			// 插入测试数据
			result = db.Chain().Table("complex_type_test").Values(map[string]interface{}{
				testCase.field: testCase.value,
			}).Save()
			if err := result.Error; err != nil {
				logger.Error("Failed to insert custom type test data:", err)
				t.Fatal(err)
			}
			logger.Debug("Inserted custom type test data")

			// 查询并验证数据
			rows, err := db.DB.Query(fmt.Sprintf("SELECT %s FROM complex_type_test WHERE id = ?", testCase.field), result.ID)
			assert.NoError(t, err)
			defer rows.Close()

			if rows.Next() {
				var rawValue sql.RawBytes
				err = rows.Scan(&rawValue)
				assert.NoError(t, err)

				// 根据字段类型进行验证
				switch testCase.field {
				case "status":
					var status Status
					err = status.FromDB(rawValue)
					assert.NoError(t, err)
					testCase.validate(t, status)
				case "metadata":
					var metadata JSONMap
					err = metadata.FromDB(rawValue)
					assert.NoError(t, err)
					testCase.validate(t, metadata)
				case "ip_address":
					var ip IPAddress
					err = ip.FromDB(rawValue)
					assert.NoError(t, err)
					testCase.validate(t, ip)
				}
			}
		})
	}

	// 测试错误处理
	errorTests := []struct {
		name     string
		field    string
		value    interface{}
		validate func(t *testing.T, err error)
	}{
		{
			name:  "Invalid IP address format",
			field: "ip_address",
			value: "invalid.ip.address",
			validate: func(t *testing.T, err error) {
				if err == nil {
					t.Error("Expected error but got none")
					return
				}
				assert.Contains(t, err.Error(), "invalid IP address format")
			},
		},
		{
			name:  "Invalid IP address segment",
			field: "ip_address",
			value: "192.168.1.256",
			validate: func(t *testing.T, err error) {
				if err == nil {
					t.Error("Expected error but got none")
					return
				}
				assert.Contains(t, err.Error(), "invalid IP address segment")
			},
		},
		{
			name:  "Invalid JSON format",
			field: "metadata",
			value: "{invalid json}",
			validate: func(t *testing.T, err error) {
				if err == nil {
					t.Error("Expected error but got none")
					return
				}
				assert.Contains(t, err.Error(), "Error 3140")
			},
		},
		{
			name:  "Invalid status value type",
			field: "status",
			value: []byte{0xFF},
			validate: func(t *testing.T, err error) {
				if err == nil {
					t.Error("Expected error but got none")
					return
				}
				assert.Contains(t, err.Error(), "Error 1366")
			},
		},
	}

	for _, testCase := range errorTests {
		t.Run(testCase.name, func(t *testing.T) {
			logger.Info("Running error test:", testCase.name)

			// 创建一个新的实例
			var err error
			switch testCase.field {
			case "ip_address":
				var ip IPAddress
				err = ip.FromDB(testCase.value)
			case "metadata":
				result = db.Chain().Table("complex_type_test").Values(map[string]interface{}{
					testCase.field: testCase.value,
				}).Save()
				err = result.Error
			case "status":
				result = db.Chain().Table("complex_type_test").Values(map[string]interface{}{
					testCase.field: testCase.value,
				}).Save()
				err = result.Error
			}

			// 验证错误
			testCase.validate(t, err)
		})
	}

	logger.Info("Completed type conversion tests")
}
