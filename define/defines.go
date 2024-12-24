package define

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// SQLFactory defines the interface for different SQL dialects
type SQLFactory interface {
	Connect(dsn string) (*DB, error)
	GenerateSelectSQL(table string, fields []string, where string, orderBy string, limit, offset int) string
	GenerateInsertSQL(table string, fields []string) string
	GenerateUpdateSQL(table string, fields []string, where string) string
	GenerateDeleteSQL(table string, where string) string
	GenerateBatchInsertSQL(table string, fields []string, valueCount int) string
	GenerateBatchUpdateSQL(table string, fields []string, where string, valueCount int) string
	GenerateBatchDeleteSQL(table string, where string, valueCount int) string
}

// QueryResult represents a query result
type QueryResult struct {
	Data    []map[string]interface{} `json:"data"`
	Columns []string                 `json:"columns"`
}

// Into scans the result into a slice of structs
func (qr *QueryResult) Into(dest interface{}) error {
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

	// Create a new slice with the correct capacity
	newSlice := reflect.MakeSlice(sliceValue.Type(), 0, len(qr.Data))

	// Iterate through each map and create struct instances
	for _, item := range qr.Data {
		// Create a new struct instance
		structPtr := reflect.New(elemType)
		structVal := structPtr.Elem()

		// Fill the struct fields
		for i := 0; i < elemType.NumField(); i++ {
			field := elemType.Field(i)
			fieldVal := structVal.Field(i)

			// Get the column name from gom tag
			tag := field.Tag.Get("gom")
			if tag == "" || tag == "-" {
				continue
			}

			// Get the value from map
			if value, ok := item[tag]; ok {
				if err := setFieldValue(fieldVal, value); err != nil {
					return fmt.Errorf("failed to set field %s: %v", field.Name, err)
				}
			}
		}

		// Append the struct to slice
		if isPtr {
			newSlice = reflect.Append(newSlice, structPtr)
		} else {
			newSlice = reflect.Append(newSlice, structVal)
		}
	}

	// Set the result back to the destination slice
	sliceValue.Set(newSlice)
	return nil
}

// setFieldValue sets the appropriate value to the struct field
func setFieldValue(field reflect.Value, value interface{}) error {
	if value == nil {
		return nil
	}

	// Handle the case where value is a pointer
	valueVal := reflect.ValueOf(value)
	if valueVal.Kind() == reflect.Ptr {
		if valueVal.IsNil() {
			return nil
		}
		valueVal = valueVal.Elem()
	}

	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := valueVal.Interface().(type) {
		case int64:
			field.SetInt(v)
		case int:
			field.SetInt(int64(v))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := valueVal.Interface().(type) {
		case uint64:
			field.SetUint(v)
		case uint:
			field.SetUint(uint64(v))
		}
	case reflect.Float32, reflect.Float64:
		switch v := valueVal.Interface().(type) {
		case float64:
			field.SetFloat(v)
		case float32:
			field.SetFloat(float64(v))
		}
	case reflect.String:
		if s, ok := valueVal.Interface().(string); ok {
			field.SetString(s)
		}
	case reflect.Bool:
		if b, ok := valueVal.Interface().(bool); ok {
			field.SetBool(b)
		}
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			if t, ok := valueVal.Interface().(time.Time); ok {
				field.Set(reflect.ValueOf(t))
			}
		}
	}
	return nil
}

// Empty returns true if the result is empty
func (qr *QueryResult) Empty() bool {
	return len(qr.Data) == 0
}

// Size returns the number of rows in the result
func (qr *QueryResult) Size() int {
	return len(qr.Data)
}

// DB represents the database connection
type DB struct {
	DB        *sql.DB
	Tx        *sql.Tx
	Factory   SQLFactory
	RoutineID int64
}

// QueryChain represents a chain of query operations
type QueryChain struct {
	DB          *DB
	TableName   string
	FieldList   []string
	WhereClause string
	WhereArgs   []interface{}
	OrderByExpr string
	LimitCount  int
	OffsetCount int
	Factory     SQLFactory
}

// Where adds a WHERE clause to the query
func (qc *QueryChain) Where(where string, args ...interface{}) *QueryChain {
	qc.WhereClause = where
	qc.WhereArgs = args
	return qc
}

// Select specifies the fields to select
func (qc *QueryChain) Select(fields ...string) *QueryChain {
	qc.FieldList = fields
	return qc
}

// OrderBy adds an ORDER BY clause
func (qc *QueryChain) OrderBy(field string) *QueryChain {
	qc.OrderByExpr = field
	return qc
}

// OrderByDesc adds a descending ORDER BY clause
func (qc *QueryChain) OrderByDesc(field string) *QueryChain {
	qc.OrderByExpr = field + " DESC"
	return qc
}

// Limit sets the LIMIT clause
func (qc *QueryChain) Limit(limit int) *QueryChain {
	qc.LimitCount = limit
	return qc
}

// Offset sets the OFFSET clause
func (qc *QueryChain) Offset(offset int) *QueryChain {
	qc.OffsetCount = offset
	return qc
}

// List executes the query and returns all results
func (qc *QueryChain) List() (*QueryResult, error) {
	if len(qc.FieldList) == 0 {
		qc.FieldList = []string{"*"}
	}

	query := qc.Factory.GenerateSelectSQL(qc.TableName, qc.FieldList, qc.WhereClause, qc.OrderByExpr, qc.LimitCount, qc.OffsetCount)
	rows, err := qc.DB.ExecuteQuery(query, qc.WhereArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	return &QueryResult{
		Data:    result,
		Columns: columns,
	}, nil
}

// First returns the first result
func (qc *QueryChain) First() (*QueryResult, error) {
	qc.LimitCount = 1
	return qc.List()
}

// Last returns the last result
func (qc *QueryChain) Last() (*QueryResult, error) {
	if qc.OrderByExpr == "" {
		if len(qc.FieldList) > 0 && qc.FieldList[0] != "*" {
			qc.OrderByExpr = qc.FieldList[0] + " DESC"
		}
	} else {
		if !strings.Contains(strings.ToUpper(qc.OrderByExpr), "DESC") {
			qc.OrderByExpr += " DESC"
		}
	}
	qc.LimitCount = 1
	return qc.List()
}

// Count returns the count of results
func (qc *QueryChain) Count() (int64, error) {
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", qc.TableName)
	if qc.WhereClause != "" {
		countQuery += " WHERE " + qc.WhereClause
	}

	rows, err := qc.DB.ExecuteQuery(countQuery, qc.WhereArgs...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

// Exists returns true if any results exist
func (qc *QueryChain) Exists() (bool, error) {
	count, err := qc.Count()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// FactoryMap stores registered SQL factories
var FactoryMap = make(map[string]SQLFactory)

// RegisterFactory registers a SQL factory with the given name
func RegisterFactory(name string, factory SQLFactory) {
	FactoryMap[name] = factory
}

// GetFactory returns a registered SQL factory by name
func GetFactory(name string) (SQLFactory, bool) {
	factory, ok := FactoryMap[name]
	return factory, ok
}
