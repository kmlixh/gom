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

// convertValue converts a database value to the appropriate Go type
func convertValue(value interface{}, fieldValue reflect.Value) error {
	if value == nil {
		// For sql.Null* types, set Valid to false
		if fieldValue.Type().String() == "sql.NullString" {
			fieldValue.Set(reflect.ValueOf(sql.NullString{Valid: false}))
			return nil
		} else if fieldValue.Type().String() == "sql.NullInt64" {
			fieldValue.Set(reflect.ValueOf(sql.NullInt64{Valid: false}))
			return nil
		} else if fieldValue.Type().String() == "sql.NullFloat64" {
			fieldValue.Set(reflect.ValueOf(sql.NullFloat64{Valid: false}))
			return nil
		} else if fieldValue.Type().String() == "sql.NullBool" {
			fieldValue.Set(reflect.ValueOf(sql.NullBool{Valid: false}))
			return nil
		} else if fieldValue.Type().String() == "sql.NullTime" {
			fieldValue.Set(reflect.ValueOf(sql.NullTime{Valid: false}))
			return nil
		}
		return nil
	}

	// Check if fieldValue is valid and can be set
	if !fieldValue.IsValid() || !fieldValue.CanSet() {
		return fmt.Errorf("invalid or cannot set field value")
	}

	// Handle sql.Null* types first
	switch v := value.(type) {
	case sql.NullString:
		if fieldValue.Type().String() == "sql.NullString" {
			fieldValue.Set(reflect.ValueOf(v))
			return nil
		}
		if v.Valid {
			return convertValue(v.String, fieldValue)
		}
		return nil
	case sql.NullInt64:
		if fieldValue.Type().String() == "sql.NullInt64" {
			fieldValue.Set(reflect.ValueOf(v))
			return nil
		}
		if v.Valid {
			return convertValue(v.Int64, fieldValue)
		}
		return nil
	case sql.NullFloat64:
		if fieldValue.Type().String() == "sql.NullFloat64" {
			fieldValue.Set(reflect.ValueOf(v))
			return nil
		}
		if v.Valid {
			return convertValue(v.Float64, fieldValue)
		}
		return nil
	case sql.NullBool:
		if fieldValue.Type().String() == "sql.NullBool" {
			fieldValue.Set(reflect.ValueOf(v))
			return nil
		}
		if v.Valid {
			return convertValue(v.Bool, fieldValue)
		}
		return nil
	case sql.NullTime:
		if fieldValue.Type().String() == "sql.NullTime" {
			fieldValue.Set(reflect.ValueOf(v))
			return nil
		}
		if v.Valid {
			return convertValue(v.Time, fieldValue)
		}
		return nil
	case []uint8:
		// Handle conversion to sql.Null* types
		str := string(v)
		if str == "" {
			// Handle empty string as NULL for sql.Null* types
			if fieldValue.Type().String() == "sql.NullString" {
				fieldValue.Set(reflect.ValueOf(sql.NullString{Valid: false}))
				return nil
			} else if fieldValue.Type().String() == "sql.NullFloat64" {
				fieldValue.Set(reflect.ValueOf(sql.NullFloat64{Valid: false}))
				return nil
			} else if fieldValue.Type().String() == "sql.NullInt64" {
				fieldValue.Set(reflect.ValueOf(sql.NullInt64{Valid: false}))
				return nil
			} else if fieldValue.Type().String() == "sql.NullBool" {
				fieldValue.Set(reflect.ValueOf(sql.NullBool{Valid: false}))
				return nil
			}
		}

		// Try to convert to sql.Null* types if not empty
		if fieldValue.Type().String() == "sql.NullFloat64" {
			if f, err := strconv.ParseFloat(str, 64); err == nil {
				fieldValue.Set(reflect.ValueOf(sql.NullFloat64{Valid: true, Float64: f}))
				return nil
			}
			fieldValue.Set(reflect.ValueOf(sql.NullFloat64{Valid: false}))
			return nil
		} else if fieldValue.Type().String() == "sql.NullInt64" {
			if i, err := strconv.ParseInt(str, 10, 64); err == nil {
				fieldValue.Set(reflect.ValueOf(sql.NullInt64{Valid: true, Int64: i}))
				return nil
			}
			fieldValue.Set(reflect.ValueOf(sql.NullInt64{Valid: false}))
			return nil
		} else if fieldValue.Type().String() == "sql.NullString" {
			fieldValue.Set(reflect.ValueOf(sql.NullString{Valid: true, String: str}))
			return nil
		} else if fieldValue.Type().String() == "sql.NullBool" {
			if len(v) == 0 {
				fieldValue.SetBool(false)
				return nil
			}
			if len(v) == 1 {
				fieldValue.SetBool(v[0] != 0)
				return nil
			}
			s := strings.ToLower(string(v))
			if s == "true" || s == "1" || s == "yes" || s == "on" {
				fieldValue.SetBool(true)
				return nil
			}
			if s == "false" || s == "0" || s == "no" || s == "off" || s == "" {
				fieldValue.SetBool(false)
				return nil
			}
		}

		// Handle other []uint8 conversions
		if str != "" {
			// Try to convert to float64 first
			if f, err := strconv.ParseFloat(str, 64); err == nil {
				return convertValue(f, fieldValue)
			}
			// Try to convert to int64
			if i, err := strconv.ParseInt(str, 10, 64); err == nil {
				return convertValue(i, fieldValue)
			}
			// Use string value for other types
			return convertValue(str, fieldValue)
		}
		return nil
	}

	switch fieldValue.Kind() {
	case reflect.Bool:
		if b, ok := value.(bool); ok {
			fieldValue.SetBool(b)
			return nil
		}
		if i, ok := value.(int64); ok {
			fieldValue.SetBool(i != 0)
			return nil
		}
		if s, ok := value.(string); ok {
			s = strings.ToLower(s)
			if s == "true" || s == "1" || s == "yes" || s == "on" {
				fieldValue.SetBool(true)
				return nil
			}
			if s == "false" || s == "0" || s == "no" || s == "off" {
				fieldValue.SetBool(false)
				return nil
			}
		}
		if b, ok := value.([]uint8); ok {
			if fieldValue.Type() == reflect.TypeOf(sql.NullBool{}) {
				if len(b) == 0 {
					fieldValue.Set(reflect.ValueOf(sql.NullBool{Valid: false}))
					return nil
				}
				if len(b) == 1 {
					fieldValue.Set(reflect.ValueOf(sql.NullBool{
						Valid: true,
						Bool:  b[0] != 0,
					}))
					return nil
				}
				str := strings.ToLower(string(b))
				fieldValue.Set(reflect.ValueOf(sql.NullBool{
					Valid: true,
					Bool:  str == "true" || str == "1" || str == "yes" || str == "on",
				}))
				return nil
			}

			if len(b) == 1 {
				fieldValue.SetBool(b[0] != 0)
				return nil
			}
			str := strings.ToLower(string(b))
			fieldValue.SetBool(str == "true" || str == "1" || str == "yes" || str == "on")
			return nil
		}
		return fmt.Errorf("cannot convert %T to bool", value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := value.(type) {
		case int64:
			fieldValue.SetInt(v)
			return nil
		case int:
			fieldValue.SetInt(int64(v))
			return nil
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				fieldValue.SetInt(i)
				return nil
			}
		case []uint8:
			if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				fieldValue.SetInt(i)
				return nil
			}
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := value.(type) {
		case uint64:
			fieldValue.SetUint(v)
			return nil
		case uint:
			fieldValue.SetUint(uint64(v))
			return nil
		case int64:
			if v >= 0 {
				fieldValue.SetUint(uint64(v))
				return nil
			}
		case string:
			if i, err := strconv.ParseUint(v, 10, 64); err == nil {
				fieldValue.SetUint(i)
				return nil
			}
		}
	case reflect.Float32, reflect.Float64:
		switch v := value.(type) {
		case float64:
			fieldValue.SetFloat(v)
			return nil
		case float32:
			fieldValue.SetFloat(float64(v))
			return nil
		case int64:
			fieldValue.SetFloat(float64(v))
			return nil
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				fieldValue.SetFloat(f)
				return nil
			}
		}
	case reflect.String:
		switch v := value.(type) {
		case string:
			fieldValue.SetString(v)
			return nil
		case []uint8:
			fieldValue.SetString(string(v))
			return nil
		}
	case reflect.Struct:
		if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
			switch v := value.(type) {
			case time.Time:
				fieldValue.Set(reflect.ValueOf(v))
				return nil
			case string:
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					fieldValue.Set(reflect.ValueOf(t))
					return nil
				}
			case []uint8:
				if t, err := time.Parse(time.RFC3339, string(v)); err == nil {
					fieldValue.Set(reflect.ValueOf(t))
					return nil
				}
			}
		}
	case reflect.Slice:
		switch v := value.(type) {
		case string:
			return json.Unmarshal([]byte(v), fieldValue.Addr().Interface())
		case []uint8:
			return json.Unmarshal(v, fieldValue.Addr().Interface())
		}
	case reflect.Map:
		switch v := value.(type) {
		case string:
			return json.Unmarshal([]byte(v), fieldValue.Addr().Interface())
		case []uint8:
			return json.Unmarshal(v, fieldValue.Addr().Interface())
		case map[string]interface{}:
			fieldValue.Set(reflect.ValueOf(v))
			return nil
		}
	}

	// Try direct type conversion if possible
	srcValue := reflect.ValueOf(value)
	if srcValue.Type().ConvertibleTo(fieldValue.Type()) {
		fieldValue.Set(srcValue.Convert(fieldValue.Type()))
		return nil
	}

	return fmt.Errorf("cannot convert %T to %s", value, fieldValue.Type())
}

// Scan implements the sql.Scanner interface
func (r *Result) Scan(rows *sql.Rows) error {
	if rows == nil {
		return nil
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return err
		}

		data := make(map[string]interface{})
		for i, column := range columns {
			if values[i] == nil {
				continue
			}
			value := *(values[i].(*interface{}))
			if value != nil {
				data[column] = value
			}
		}
		r.Data = append(r.Data, data)
	}

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

			if err := convertValue(value, fieldValue); err != nil {
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
