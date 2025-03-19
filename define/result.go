package define

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
			if fieldValue.Kind() == reflect.Ptr && value == nil {
				// 对于NULL值和指针类型，直接设置为nil而不是创建零值的指针
				fieldValue.Set(reflect.Zero(fieldValue.Type()))
				continue
			}

			// 处理sql.Null*类型的NULL值特殊情况
			if value != nil {
				valueType := reflect.TypeOf(value)
				if valueType.Implements(reflect.TypeOf((*sql.Scanner)(nil)).Elem()) {
					valueValue := reflect.ValueOf(value)
					if validField := valueValue.FieldByName("Valid"); validField.IsValid() && !validField.Bool() {
						// 这是SQL NULL值，如果目标是指针类型，设置为nil
						if fieldValue.Kind() == reflect.Ptr {
							fieldValue.Set(reflect.Zero(fieldValue.Type()))
							continue
						}
					}
				}
			}

			if err := convertFieldValue(value, fieldValue); err != nil {
				return fmt.Errorf("error converting field %s: %v", field.Name, err)
			}
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
