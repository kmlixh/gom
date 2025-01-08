package define

import (
	"database/sql"
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
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}
		// Parse tag to get column name
		parts := strings.Split(tag, ",")
		columnName := strings.ToLower(parts[0])
		fieldMap[columnName] = field

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

// Ensure Result implements sql.Result interface
var _ sql.Result = (*Result)(nil)
