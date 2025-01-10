package define

import (
	"reflect"
	"strings"
	"time"
)

// StructToMap converts a struct to a map[string]interface{} using gom tags
func StructToMap(model interface{}) map[string]interface{} {
	if model == nil {
		return nil
	}

	// Get model type and value
	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		return nil
	}

	fieldMap := make(map[string]interface{})

	// Extract field values from the model
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		fieldValue := modelValue.Field(i)

		// Skip invalid types
		switch fieldValue.Kind() {
		case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Complex64, reflect.Complex128:
			continue
		}

		// Get field name and check tag options
		fieldName := field.Name
		isAuto := false
		isDefault := false

		if tag := field.Tag.Get("gom"); tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] != "" {
				fieldName = parts[0]
			}

			// Check tag options
			for _, part := range parts[1:] {
				switch part {
				case "auto":
					isAuto = true
				case "default":
					isDefault = true
				}
			}

			// Skip auto-increment fields
			if isAuto {
				continue
			}

			// Skip zero value fields with default tag
			if isDefault && fieldValue.IsZero() {
				continue
			}
		}

		if !fieldValue.IsValid() {
			continue
		}

		// Handle different types
		var value interface{}
		switch fieldValue.Kind() {
		case reflect.Struct:
			if _, isTime := fieldValue.Interface().(time.Time); isTime {
				value = fieldValue.Interface()
			} else {
				nestedMap := StructToMap(fieldValue.Interface())
				if nestedMap != nil {
					value = nestedMap
				} else {
					value = fieldValue.Interface()
				}
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			value = int(fieldValue.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			value = int(fieldValue.Uint())
		case reflect.Float32, reflect.Float64:
			value = fieldValue.Float()
		case reflect.String:
			value = fieldValue.String()
		case reflect.Bool:
			value = fieldValue.Bool()
		case reflect.Ptr, reflect.Interface:
			if !fieldValue.IsNil() {
				if fieldValue.Elem().Kind() == reflect.Struct {
					nestedMap := StructToMap(fieldValue.Elem().Interface())
					if nestedMap != nil {
						value = nestedMap
					} else {
						value = fieldValue.Elem().Interface()
					}
				} else {
					value = fieldValue.Elem().Interface()
				}
			} else {
				value = nil
			}
		case reflect.Map:
			if !fieldValue.IsNil() {
				value = fieldValue.Interface()
			} else {
				value = nil
			}
		case reflect.Slice, reflect.Array:
			if !fieldValue.IsNil() {
				value = fieldValue.Interface()
			} else {
				value = nil
			}
		default:
			// Try to convert custom types
			if fieldValue.Type().Name() != "" {
				switch fieldValue.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					value = int(fieldValue.Int())
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					value = int(fieldValue.Uint())
				case reflect.Float32, reflect.Float64:
					value = fieldValue.Float()
				case reflect.String:
					value = fieldValue.String()
				case reflect.Bool:
					value = fieldValue.Bool()
				default:
					value = fieldValue.Interface()
				}
			} else {
				value = fieldValue.Interface()
			}
		}

		fieldMap[fieldName] = value
	}

	return fieldMap
}
