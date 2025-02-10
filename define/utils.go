package define

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// GetFieldMap returns a map of field names to their values for a struct
func GetFieldMap(obj interface{}) map[string]interface{} {
	fieldMap := make(map[string]interface{})
	if obj == nil {
		return fieldMap
	}

	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return fieldMap
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return fieldMap
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}

		value := v.Field(i).Interface()
		fieldMap[field.Name] = value

		tag := field.Tag.Get("json")
		if tag != "" {
			name := strings.Split(tag, ",")[0]
			if name != "-" {
				fieldMap[name] = value
			}
		}
	}

	return fieldMap
}

// Global type converter instance
var converter ITypeConverter = NewTypeConverter()

// ConvertValue converts a value to the target type using the global type converter
func ConvertValue(value interface{}, targetType reflect.Type) (interface{}, error) {
	return converter.Convert(value, targetType)
}

// StructToMap converts a struct to a map[string]interface{} using gom tags
// Only non-zero values are included in the result
func StructToMap(obj interface{}) (map[string]interface{}, error) {
	if obj == nil {
		return nil, errors.New("input object is nil")
	}

	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, errors.New("input object is not a struct")
	}

	result := make(map[string]interface{})
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Skip unexported fields
		if !fieldType.IsExported() {
			continue
		}

		// Skip zero values
		if field.IsZero() {
			continue
		}

		// Get field name from tag or use struct field name
		fieldName := fieldType.Name
		if tag := fieldType.Tag.Get("gom"); tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] != "" {
				fieldName = parts[0]
			}
		}

		// Add non-zero value to result
		result[fieldName] = field.Interface()
	}

	return result, nil
}
func GetFieldToColMap(i any, tableInfo *TableInfo) (map[string]string, map[string]string, error) {
	fieldMap := make(map[string]string)
	colMap := make(map[string]string)
	if i == nil {
		return fieldMap, colMap, fmt.Errorf("input object is nil")
	}

	v := reflect.ValueOf(i)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return fieldMap, colMap, fmt.Errorf("input object is nil")
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return fieldMap, colMap, fmt.Errorf("input object is not a struct")
	}

	for _, col := range tableInfo.Columns {
		colMap[col.Name] = col.Name
	}
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		fieldName := ""
		colName := ""
		tag := field.Tag.Get("json")
		if tag != "" {
			name := strings.Split(tag, ",")[0]
			if name != "-" {
				fieldName = name
			}
		}
		if fieldName != "" {

			tag := field.Tag.Get("gom")
			if tag != "" {
				parts := strings.Split(tag, ",")
				if parts[0] != "" && parts[0] != "-" {
					colName = parts[0]
				}
			}
			if colName == "" {
				colName = fieldName
			}
		}
		if _, ok := colMap[colName]; ok {
			fieldMap[fieldName] = colName
			colMap[colName] = fieldName
			break
		}

	}
	return fieldMap, colMap, nil
}

// isZeroValue checks if a reflect.Value is the zero value for its type
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return v.String() == ""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Slice, reflect.Map:
		return v.IsNil() || v.Len() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	default:
		return false
	}
}
