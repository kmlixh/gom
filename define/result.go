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
	ID       int64
	Affected int64
	Error    error
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
		return nil
	}

	switch fieldValue.Kind() {
	case reflect.Bool:
		switch v := value.(type) {
		case int64, int, int32, int16, int8:
			fieldValue.SetBool(reflect.ValueOf(v).Int() != 0)
			return nil
		case uint64, uint, uint32, uint16, uint8:
			fieldValue.SetBool(reflect.ValueOf(v).Uint() != 0)
			return nil
		case []uint8:
			if len(v) == 1 {
				fieldValue.SetBool(v[0] != 0)
				return nil
			}
			str := strings.ToLower(strings.TrimSpace(string(v)))
			fieldValue.SetBool(str == "true" || str == "1" || str == "yes" || str == "on")
			return nil
		case bool:
			fieldValue.SetBool(v)
			return nil
		case string:
			str := strings.ToLower(strings.TrimSpace(v))
			fieldValue.SetBool(str == "true" || str == "1" || str == "yes" || str == "on")
			return nil
		}
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

// Into converts the result data into the specified struct slice
func (r *Result) Into(dest interface{}) error {
	if r == nil {
		// For nil Result, set destination to empty slice
		destValue := reflect.ValueOf(dest)
		if destValue.Kind() != reflect.Ptr || destValue.IsNil() {
			return fmt.Errorf("destination must be a non-nil pointer")
		}
		sliceValue := destValue.Elem()
		if sliceValue.Kind() != reflect.Slice {
			return fmt.Errorf("destination must be a slice")
		}
		// Set to empty slice
		sliceValue.Set(reflect.MakeSlice(sliceValue.Type(), 0, 0))
		return nil
	}

	if r.Error != nil {
		return r.Error
	}

	if len(r.Data) == 0 {
		return nil
	}

	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}

	sliceValue := destValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return fmt.Errorf("destination must be a slice")
	}

	elemType := sliceValue.Type().Elem()
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
		for columnName, value := range data {
			columnName = strings.ToLower(columnName)
			if field, ok := fieldMap[columnName]; ok {
				fieldValue := newElem.Elem().FieldByName(field.Name)
				if !fieldValue.CanSet() {
					continue
				}

				if err := convertValue(value, fieldValue); err != nil {
					return fmt.Errorf("error converting field %s: %v", field.Name, err)
				}
			}
		}

		if isPtr {
			sliceValue.Set(reflect.Append(sliceValue, newElem))
		} else {
			sliceValue.Set(reflect.Append(sliceValue, newElem.Elem()))
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
