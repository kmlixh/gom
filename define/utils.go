package define

import (
	"reflect"
	"strings"
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
		tag := field.Tag.Get("gom")
		if tag == "" {
			continue
		}

		// Parse tag
		parts := strings.Split(tag, ",")
		columnName := parts[0]
		if columnName == "" {
			continue
		}

		// Skip auto-increment fields
		isAuto := false
		for _, part := range parts[1:] {
			if part == "auto" {
				isAuto = true
				break
			}
		}
		if isAuto {
			continue
		}

		// Get field value
		fieldValue := modelValue.Field(i)
		if !fieldValue.IsValid() {
			continue
		}

		// Handle zero values based on tag options
		isZero := reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(fieldValue.Type()).Interface())
		if isZero {
			hasDefault := false
			for _, part := range parts[1:] {
				if part == "default" {
					hasDefault = true
					break
				}
			}
			if hasDefault {
				continue
			}
		}

		fieldMap[columnName] = fieldValue.Interface()
	}

	return fieldMap
}
