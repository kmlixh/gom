package define

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Result implements sql.Result interface and includes query result functionality
type Result struct {
	ID       int64            `json:"id"`
	Affected int64            `json:"affected"`
	Error    error            `json:"error"`
	Data     []map[string]any `json:"data"`
	Columns  []string         `json:"columns"`
}

// LastInsertId returns the last inserted ID
func (r *Result) LastInsertId() (int64, error) {
	if r.Error != nil {
		return 0, r.Error
	}
	return r.ID, nil
}

// RowsAffected returns the number of rows affected
func (r *Result) RowsAffected() (int64, error) {
	if r.Error != nil {
		return 0, r.Error
	}
	return r.Affected, nil
}

// Empty returns true if the result is empty
func (r *Result) Empty() bool {
	return len(r.Data) == 0
}

// Size returns the number of rows in the result
func (r *Result) Size() int {
	return len(r.Data)
}

// isNullSqlType 判断值是否为sql.Null*类型，并返回Valid值
func isNullSqlType(value interface{}) (bool, bool) {
	if value == nil {
		return false, false
	}

	v := reflect.ValueOf(value)

	// 首先检查值是否为结构体类型
	if v.Kind() != reflect.Struct {
		return false, false
	}

	if validField := v.FieldByName("Valid"); validField.IsValid() && validField.Kind() == reflect.Bool {
		return validField.Bool(), true
	}

	return false, false
}

// convertFieldValue converts a database value to the appropriate Go type
func convertFieldValue(value interface{}, fieldValue reflect.Value) error {
	// 检查是否为NULL的sql.Null*类型，并且目标是指针类型
	if fieldValue.Kind() == reflect.Ptr {
		// 检查value是否为sql.Null*类型
		if nullValue, ok := value.(interface{ Valid() bool }); ok {
			// 这是自定义的Valid接口
			if !nullValue.Valid() {
				// 如果不是有效值，直接设置为nil
				fieldValue.Set(reflect.Zero(fieldValue.Type()))
				return nil
			}
		} else if nullValue, ok := isNullSqlType(value); ok && !nullValue {
			// 这是sql.Null*类型，并且Valid=false
			// 设置为nil
			fieldValue.Set(reflect.Zero(fieldValue.Type()))
			return nil
		}
	}

	if value == nil {
		// 对于nil值，根据字段类型设置适当的零值
		switch fieldValue.Kind() {
		case reflect.String:
			fieldValue.SetString("")
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fieldValue.SetInt(0)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fieldValue.SetUint(0)
		case reflect.Float32, reflect.Float64:
			fieldValue.SetFloat(0)
		case reflect.Bool:
			fieldValue.SetBool(false)
		case reflect.Struct:
			// 对于time.Time类型，设置零值
			if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
				fieldValue.Set(reflect.ValueOf(time.Time{}))
			}
		case reflect.Ptr:
			// 如果是指针类型，设置为nil
			fieldValue.Set(reflect.Zero(fieldValue.Type()))
		}
		return nil
	}

	// Handle pointer types
	if fieldValue.Kind() == reflect.Ptr {
		if fieldValue.IsNil() {
			fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
		}
		return convertFieldValue(value, fieldValue.Elem())
	}

	switch v := value.(type) {
	case *sql.NullInt64:
		if !v.Valid {
			return nil
		}
		switch fieldValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if !fieldValue.OverflowInt(v.Int64) {
				fieldValue.SetInt(v.Int64)
				return nil
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if v.Int64 >= 0 && !fieldValue.OverflowUint(uint64(v.Int64)) {
				fieldValue.SetUint(uint64(v.Int64))
				return nil
			}
		case reflect.Float32, reflect.Float64:
			fieldValue.SetFloat(float64(v.Int64))
			return nil
		case reflect.Bool:
			fieldValue.SetBool(v.Int64 != 0)
			return nil
		case reflect.String:
			fieldValue.SetString(strconv.FormatInt(v.Int64, 10))
			return nil
		}
		return fmt.Errorf("cannot convert int64 to %v", fieldValue.Type())

	case *sql.NullFloat64:
		if !v.Valid {
			return nil
		}
		switch fieldValue.Kind() {
		case reflect.Float32, reflect.Float64:
			if !fieldValue.OverflowFloat(v.Float64) {
				fieldValue.SetFloat(v.Float64)
				return nil
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if v.Float64 == float64(int64(v.Float64)) {
				if !fieldValue.OverflowInt(int64(v.Float64)) {
					fieldValue.SetInt(int64(v.Float64))
					return nil
				}
			}
		case reflect.String:
			fieldValue.SetString(strconv.FormatFloat(v.Float64, 'f', -1, 64))
			return nil
		}
		return fmt.Errorf("cannot convert float64 to %v", fieldValue.Type())

	case *sql.NullString:
		if !v.Valid {
			return nil
		}
		switch fieldValue.Kind() {
		case reflect.String:
			fieldValue.SetString(v.String)
			return nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if i, err := strconv.ParseInt(v.String, 10, 64); err == nil && !fieldValue.OverflowInt(i) {
				fieldValue.SetInt(i)
				return nil
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if i, err := strconv.ParseUint(v.String, 10, 64); err == nil && !fieldValue.OverflowUint(i) {
				fieldValue.SetUint(i)
				return nil
			}
		case reflect.Float32, reflect.Float64:
			if f, err := strconv.ParseFloat(v.String, 64); err == nil && !fieldValue.OverflowFloat(f) {
				fieldValue.SetFloat(f)
				return nil
			}
		case reflect.Bool:
			if b, err := strconv.ParseBool(v.String); err == nil {
				fieldValue.SetBool(b)
				return nil
			}
		}
		return fmt.Errorf("cannot convert string to %v", fieldValue.Type())

	case *sql.NullTime:
		if !v.Valid {
			return nil
		}
		if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
			fieldValue.Set(reflect.ValueOf(v.Time))
			return nil
		}
		return fmt.Errorf("cannot convert time to %v", fieldValue.Type())

	case *sql.RawBytes:
		if v == nil {
			return nil
		}
		str := string(*v)
		switch fieldValue.Kind() {
		case reflect.String:
			fieldValue.SetString(str)
			return nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if i, err := strconv.ParseInt(str, 10, 64); err == nil && !fieldValue.OverflowInt(i) {
				fieldValue.SetInt(i)
				return nil
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if i, err := strconv.ParseUint(str, 10, 64); err == nil && !fieldValue.OverflowUint(i) {
				fieldValue.SetUint(i)
				return nil
			}
		case reflect.Float32, reflect.Float64:
			if f, err := strconv.ParseFloat(str, 64); err == nil && !fieldValue.OverflowFloat(f) {
				fieldValue.SetFloat(f)
				return nil
			}
		case reflect.Bool:
			if b, err := strconv.ParseBool(str); err == nil {
				fieldValue.SetBool(b)
				return nil
			}
		case reflect.Slice:
			if fieldValue.Type().Elem().Kind() == reflect.Uint8 {
				fieldValue.SetBytes(*v)
				return nil
			}
		}
		return fmt.Errorf("cannot convert []byte to %v", fieldValue.Type())

	case int8:
		switch fieldValue.Kind() {
		case reflect.Bool:
			fieldValue.SetBool(v != 0)
			return nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if !fieldValue.OverflowInt(int64(v)) {
				fieldValue.SetInt(int64(v))
				return nil
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if v >= 0 && !fieldValue.OverflowUint(uint64(v)) {
				fieldValue.SetUint(uint64(v))
				return nil
			}
		case reflect.Float32, reflect.Float64:
			fieldValue.SetFloat(float64(v))
			return nil
		case reflect.String:
			fieldValue.SetString(strconv.FormatInt(int64(v), 10))
			return nil
		}
		return fmt.Errorf("cannot convert int8 to %v", fieldValue.Type())
	}

	// Try using the global type converter for other types
	if converted, err := ConvertValue(value, fieldValue.Type()); err == nil {
		fieldValue.Set(reflect.ValueOf(converted))
		return nil
	}

	return fmt.Errorf("unsupported type conversion from %T to %v", value, fieldValue.Type())
}

// Scan implements the sql.Scanner interface
func (r *Result) Scan(rows *sql.Rows) error {
	if rows == nil {
		return nil
	}

	// 获取列类型信息
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	// 根据列类型创建对应扫描值
	values := make([]interface{}, len(colTypes))
	for i, ct := range colTypes {
		scanType := ct.ScanType()
		if Debug {
			fmt.Println("scanType:", scanType.Kind())
		}
		switch scanType {
		case reflect.TypeOf(time.Time{}):
			values[i] = new(*time.Time)
		case reflect.TypeOf([]byte{}):
			values[i] = new([]byte)
		case reflect.TypeOf(false):
			values[i] = new(bool)
		case reflect.TypeOf(float64(0)):
			values[i] = new(float64)
		case reflect.TypeOf(int64(0)):
			values[i] = new(int64)
		case reflect.TypeOf(int32(0)):
			values[i] = new(int32)
		case reflect.TypeOf(int16(0)):
			values[i] = new(int16)
		case reflect.TypeOf(int8(0)):
			values[i] = new(int8)
		case reflect.TypeOf(uint(0)):
			values[i] = new(uint)
		case reflect.TypeOf(uint64(0)):
			values[i] = new(uint64)
		case reflect.TypeOf(uint32(0)):
			values[i] = new(uint32)
		case reflect.TypeOf(uint16(0)):
			values[i] = new(uint16)
		case reflect.TypeOf(uint8(0)):
			values[i] = new(uint8)
		case reflect.TypeOf(byte(0)):
			values[i] = new(byte)
		case reflect.TypeOf([]byte{}):
			values[i] = new([]byte)
		case reflect.TypeOf(""):
			values[i] = new(string)
		default:
			// 使用驱动推荐的默认类型
			values[i] = reflect.New(scanType).Interface()
		}
	}

	// 初始化数据容器
	var resultSet []map[string]interface{}

	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return err
		}

		rowData := make(map[string]interface{})
		for i, ct := range colTypes {
			val := reflect.ValueOf(values[i]).Elem().Interface()

			// 处理指针和NULL值
			if val == nil {
				rowData[ct.Name()] = nil
				continue
			}

			// 类型安全转换
			switch v := val.(type) {
			case *time.Time:
				if v != nil {
					rowData[ct.Name()] = *v
				} else {
					rowData[ct.Name()] = nil
				}
			case []byte:
				// 尝试解析特殊类型
				if dbType := ct.DatabaseTypeName(); dbType != "" {
					switch dbType {
					case "JSON", "JSONB":
						var jsonData interface{}
						if err := json.Unmarshal(v, &jsonData); err == nil {
							rowData[ct.Name()] = jsonData
							continue
						}
					}
				}
				rowData[ct.Name()] = string(v)
			default:
				rowData[ct.Name()] = v
			}
		}
		resultSet = append(resultSet, rowData)
	}

	r.Data = resultSet
	return rows.Err()
}

// Into converts the result data into the specified struct slice or struct pointer
func (r *Result) Into(dest interface{}) error {
	if r == nil {
		return fmt.Errorf("result is nil")
	}

	if r.Error != nil {
		return r.Error
	}

	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}

	// Get the element that the pointer points to
	destElem := destValue.Elem()

	// Handle struct pointer case
	if destElem.Kind() == reflect.Struct {
		if len(r.Data) == 0 {
			return sql.ErrNoRows
		}
		if len(r.Data) > 1 {
			return fmt.Errorf("destination must be a pointer to slice for multiple results")
		}
		// Use the first row of data for struct
		return ConvertRowToStruct(r.Data[0], destElem)
	}

	// Handle slice pointer case
	if destElem.Kind() == reflect.Slice {
		// Set to empty slice if no data
		if len(r.Data) == 0 {
			destElem.Set(reflect.MakeSlice(destElem.Type(), 0, 0))
			return nil
		}

		elemType := destElem.Type().Elem()
		isPtr := elemType.Kind() == reflect.Ptr
		if isPtr {
			elemType = elemType.Elem()
		}
		if elemType.Kind() != reflect.Struct {
			return fmt.Errorf("slice element must be a struct or pointer to struct")
		}

		// Create field map for faster lookup
		fieldMap := make(map[string]*reflect.StructField)
		for i := 0; i < elemType.NumField(); i++ {
			field := elemType.Field(i)
			tag := field.Tag.Get("gom")
			if tag == "" {
				continue
			}
			columnName := strings.Split(tag, ",")[0]
			if columnName == "" {
				continue
			}
			fieldMap[strings.ToLower(columnName)] = &field
		}

		// Convert each data row to struct
		for _, data := range r.Data {
			newElem := reflect.New(elemType)
			if err := ConvertRowToStruct(data, newElem.Elem()); err != nil {
				return err
			}

			if isPtr {
				destElem.Set(reflect.Append(destElem, newElem))
			} else {
				destElem.Set(reflect.Append(destElem, newElem.Elem()))
			}
		}
		return nil
	}

	return fmt.Errorf("destination must be a pointer to struct or slice")
}

// ConvertRowToStruct converts a single data row to a struct
func ConvertRowToStruct(data map[string]interface{}, structValue reflect.Value) error {
	structType := structValue.Type()

	// Create field map for the struct
	fieldMap := make(map[string]*reflect.StructField)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" {
			continue
		}
		columnName := strings.Split(tag, ",")[0]
		if columnName == "" {
			continue
		}
		fieldMap[strings.ToLower(columnName)] = &field
	}

	// Convert each field
	for columnName, value := range data {
		columnName = strings.ToLower(columnName)
		if field, ok := fieldMap[columnName]; ok {
			fieldValue := structValue.FieldByName(field.Name)
			if !fieldValue.CanSet() {
				continue
			}

			// 处理指针类型的NULL值特殊情况
			if fieldValue.Kind() == reflect.Ptr && isNullValue(value) {
				// 对于NULL值和指针类型，直接设置为nil
				fieldValue.Set(reflect.Zero(fieldValue.Type()))
				continue
			}

			// 检查目标字段是否为sql.Null*类型
			isSqlNullField := false
			if fieldValue.Kind() == reflect.Struct {
				fieldType := fieldValue.Type()
				validField, exists := fieldType.FieldByName("Valid")
				isSqlNullField = exists && validField.Type.Kind() == reflect.Bool
			}

			if isSqlNullField {
				// 如果目标字段是sql.Null*类型，需要特殊处理
				// 根据value的类型和是否为NULL设置Valid字段
				validField := fieldValue.FieldByName("Valid")

				// 如果源值为nil或者是sql.Null*类型且Valid为false，则设置Valid为false
				if isNullValue(value) || (isSqlNullType(value) && !getSqlNullValidValue(value)) {
					validField.SetBool(false)
				} else {
					validField.SetBool(true)

					// 获取实际值，如果是sql.Null*类型需要提取其中的值
					var actualValue interface{}
					if isSqlNullType(value) {
						actualValue = getSqlNullActualValue(value)
					} else {
						actualValue = value
					}

					// 检查字节数组是否为空
					if bytes, ok := actualValue.([]byte); ok && len(bytes) == 0 {
						validField.SetBool(false)
						continue
					}

					// 根据字段类型设置对应的值
					switch fieldValue.Type().String() {
					case "sql.NullString":
						strValue, err := convertToString(actualValue)
						if err == nil && strValue != "" {
							fieldValue.FieldByName("String").SetString(strValue)
						} else {
							validField.SetBool(false)
						}
					case "sql.NullInt64":
						intValue, err := convertToInt64(actualValue)
						if err == nil {
							fieldValue.FieldByName("Int64").SetInt(intValue)
						} else {
							validField.SetBool(false)
						}
					case "sql.NullFloat64":
						floatValue, err := convertToFloat64(actualValue)
						if err == nil {
							fieldValue.FieldByName("Float64").SetFloat(floatValue)
						} else {
							validField.SetBool(false)
						}
					case "sql.NullBool":
						boolValue, err := convertToBool(actualValue)
						if err == nil {
							fieldValue.FieldByName("Bool").SetBool(boolValue)
						} else {
							validField.SetBool(false)
						}
					case "sql.NullTime":
						timeValue, err := convertToTime(actualValue)
						if err == nil {
							fieldValue.FieldByName("Time").Set(reflect.ValueOf(timeValue))
						} else {
							validField.SetBool(false)
						}
					}
				}
				continue
			} else if isSqlNullType(value) {
				// 处理sql.Null*类型的转换
				if !getSqlNullValidValue(value) {
					// 如果是基本类型且值为NULL，则根据类型设置零值
					switch fieldValue.Kind() {
					case reflect.String:
						fieldValue.SetString("")
						continue
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						fieldValue.SetInt(0)
						continue
					case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						fieldValue.SetUint(0)
						continue
					case reflect.Float32, reflect.Float64:
						fieldValue.SetFloat(0)
						continue
					case reflect.Bool:
						fieldValue.SetBool(false)
						continue
					case reflect.Struct:
						// 处理time.Time等特殊结构体
						if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
							fieldValue.Set(reflect.ValueOf(time.Time{}))
							continue
						}
					}
				} else {
					// 值有效，提取实际值
					value = getSqlNullActualValue(value)
				}
			}

			if err := convertFieldValue(value, fieldValue); err != nil {
				return fmt.Errorf("error converting field %s: %v", field.Name, err)
			}
		}
	}

	return nil
}

// 辅助函数：检查值是否为NULL或空值
func isNullValue(value interface{}) bool {
	if value == nil {
		return true
	}

	// 检查空字节数组和空字符串
	switch v := value.(type) {
	case []byte:
		return len(v) == 0
	case string:
		return v == ""
	}

	// 检查sql.Null*类型
	return isSqlNullType(value) && !getSqlNullValidValue(value)
}

// 辅助函数：检查是否为sql.Null*类型
func isSqlNullType(value interface{}) bool {
	if value == nil {
		return false
	}

	v := reflect.ValueOf(value)

	// 只有结构体类型才可能是sql.Null*类型
	if v.Kind() != reflect.Struct {
		return false
	}

	validField := v.FieldByName("Valid")
	return validField.IsValid() && validField.Kind() == reflect.Bool
}

// 辅助函数：获取sql.Null*类型的Valid值
func getSqlNullValidValue(value interface{}) bool {
	if value == nil {
		return false
	}

	v := reflect.ValueOf(value)

	// 只有结构体类型才可能是sql.Null*类型
	if v.Kind() != reflect.Struct {
		return true
	}

	validField := v.FieldByName("Valid")
	if validField.IsValid() && validField.Kind() == reflect.Bool {
		return validField.Bool()
	}
	return true
}

// 辅助函数：获取sql.Null*类型的实际值
func getSqlNullActualValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	v := reflect.ValueOf(value)

	// 只有结构体类型才可能是sql.Null*类型
	if v.Kind() != reflect.Struct {
		return value
	}

	validField := v.FieldByName("Valid")
	if validField.IsValid() && validField.Kind() == reflect.Bool && validField.Bool() {
		// 根据结构体类型获取对应的值字段
		var valueField reflect.Value
		switch value.(type) {
		case sql.NullString:
			valueField = v.FieldByName("String")
		case sql.NullInt64:
			valueField = v.FieldByName("Int64")
		case sql.NullFloat64:
			valueField = v.FieldByName("Float64")
		case sql.NullBool:
			valueField = v.FieldByName("Bool")
		case sql.NullTime:
			valueField = v.FieldByName("Time")
		default:
			// 尝试通用方法获取值字段
			for _, fieldName := range []string{"String", "Int64", "Float64", "Bool", "Time", "Value"} {
				valueField = v.FieldByName(fieldName)
				if valueField.IsValid() {
					break
				}
			}
		}

		if valueField.IsValid() {
			return valueField.Interface()
		}
	}

	return nil
}

// IntoMap scans a single result row into a map
func (r *Result) IntoMap() (map[string]interface{}, error) {
	if r.Error != nil {
		return nil, r.Error
	}

	if len(r.Data) == 0 {
		return nil, sql.ErrNoRows
	}

	return r.Data[0], nil
}

// IntoMaps returns all result rows as a slice of maps
func (r *Result) IntoMaps() ([]map[string]interface{}, error) {
	if r.Error != nil {
		return nil, r.Error
	}

	return r.Data, nil
}

// First returns the first result or error if no results
func (r *Result) First() *Result {
	if r.Error != nil {
		return r
	}
	if len(r.Data) > 0 {
		return &Result{
			Data:    r.Data[:1],
			Columns: r.Columns,
		}
	}
	return &Result{Error: sql.ErrNoRows}
}

// ToJSON converts the result to JSON string
func (r *Result) ToJSON() (string, error) {
	if r.Error != nil {
		return "", r.Error
	}

	jsonData, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("failed to marshal result to JSON: %v", err)
	}

	return string(jsonData), nil
}

// FromJSON parses JSON string into the result
func (r *Result) FromJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), r)
}

// Ensure Result implements sql.Result interface
var _ sql.Result = (*Result)(nil)

// GetRow returns a DataRow for the specified index
func (r *Result) GetRow(index int) (*DataRow, error) {
	if r.Error != nil {
		return nil, r.Error
	}

	if index < 0 || index >= len(r.Data) {
		return nil, fmt.Errorf("index out of range: %d", index)
	}

	return &DataRow{data: r.Data[index]}, nil
}

// GetFirstRow returns the first DataRow from the result
func (r *Result) GetFirstRow() (*DataRow, error) {
	return r.GetRow(0)
}

// 辅助函数：转换为字符串
func convertToString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}

	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprintf("%v", v), nil
	case time.Time:
		return v.Format(time.RFC3339), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// 辅助函数：转换为int64
func convertToInt64(value interface{}) (int64, error) {
	if value == nil {
		return 0, nil
	}

	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v <= math.MaxInt64 {
			return int64(v), nil
		}
		return 0, fmt.Errorf("uint64 value %d out of int64 range", v)
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

// 辅助函数：转换为float64
func convertToFloat64(value interface{}) (float64, error) {
	if value == nil {
		return 0, nil
	}

	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

// 辅助函数：转换为time.Time
func convertToTime(value interface{}) (time.Time, error) {
	if value == nil {
		return time.Time{}, nil
	}

	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		// 尝试多种格式解析时间
		formats := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse time string: %s", v)
	case []byte:
		return convertToTime(string(v))
	case int64:
		return time.Unix(v, 0), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", v)
	}
}
