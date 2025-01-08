package define

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
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

// Into scans the result into a slice of structs
func (r *Result) Into(dest interface{}) error {
	if r.Error != nil {
		return r.Error
	}

	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer")
	}

	sliceValue := destValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	// Get the type of slice elements
	elemType := sliceValue.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	if isPtr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("slice elements must be structs")
	}

	// Create a map of field names to struct fields
	fieldMap := make(map[string]reflect.StructField)
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)

		// Try gom tag first
		tag := field.Tag.Get("gom")
		if tag != "" && tag != "-" {
			parts := strings.Split(tag, ",")
			columnName := strings.ToLower(parts[0])
			fieldMap[columnName] = field
		}

		// Try json tag if gom tag is not present
		jsonTag := field.Tag.Get("json")
		if jsonTag != "" && jsonTag != "-" {
			parts := strings.Split(jsonTag, ",")
			columnName := strings.ToLower(parts[0])
			fieldMap[columnName] = field
		}

		// Also map the field name itself in lowercase
		fieldMap[strings.ToLower(field.Name)] = field
	}

	// Create a new slice with the correct capacity
	newSlice := reflect.MakeSlice(sliceValue.Type(), 0, len(r.Data))

	// Iterate over each row in the result
	for _, row := range r.Data {
		// Create a new struct for this row
		newElem := reflect.New(elemType).Elem()

		// Set each field in the struct
		for columnName, value := range row {
			columnName = strings.ToLower(columnName)
			if field, ok := fieldMap[columnName]; ok {
				fieldValue := newElem.FieldByName(field.Name)
				if !fieldValue.CanSet() {
					continue
				}

				// Convert the value to the field's type
				if value == nil {
					continue
				}

				// Handle JSON data from database
				switch v := value.(type) {
				case string:
					// Try to unmarshal if the field is a struct, map, slice, or array
					switch fieldValue.Kind() {
					case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
						if err := json.Unmarshal([]byte(v), fieldValue.Addr().Interface()); err == nil {
							continue
						}
					}
				case []byte:
					// Handle JSON stored as bytes
					switch fieldValue.Kind() {
					case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
						if err := json.Unmarshal(v, fieldValue.Addr().Interface()); err == nil {
							continue
						}
					case reflect.String:
						// If the field is string, try to unmarshal first, if fails then set as string
						var js interface{}
						if err := json.Unmarshal(v, &js); err == nil {
							// It's valid JSON, keep it as JSON string
							fieldValue.SetString(string(v))
						} else {
							// Not valid JSON, treat as normal string
							fieldValue.SetString(string(v))
						}
						continue
					}
				case map[string]interface{}, []interface{}:
					// Handle JSON objects/arrays already decoded by database driver
					jsonData, err := json.Marshal(v)
					if err == nil {
						switch fieldValue.Kind() {
						case reflect.Struct, reflect.Map:
							if err := json.Unmarshal(jsonData, fieldValue.Addr().Interface()); err == nil {
								continue
							}
						case reflect.String:
							fieldValue.SetString(string(jsonData))
							continue
						}
					}
				}

				// Normal type conversion
				srcValue := reflect.ValueOf(value)
				if srcValue.Type().ConvertibleTo(fieldValue.Type()) {
					fieldValue.Set(srcValue.Convert(fieldValue.Type()))
				}
			}
		}

		// Add the new element to the slice
		if isPtr {
			newSlice = reflect.Append(newSlice, newElem.Addr())
		} else {
			newSlice = reflect.Append(newSlice, newElem)
		}
	}

	// Set the new slice as the value of dest
	destValue.Elem().Set(newSlice)
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
