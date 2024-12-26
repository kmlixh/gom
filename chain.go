package gom

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/kmlixh/gom/v4/define"
)

// OrderType represents the type of ordering
type OrderType int

const (
	OrderAsc  OrderType = iota // Ascending order
	OrderDesc                  // Descending order
)

// OrderBy represents an order by clause
type OrderBy struct {
	Field string
	Type  OrderType
}

// Chain represents the base chain structure
type Chain struct {
	db      *DB
	factory define.SQLFactory
	tx      *sql.Tx

	// Store original chain for transaction rollback
	originalChain *Chain

	// Transaction isolation level
	isolationLevel sql.IsolationLevel

	// Model for potential ID callback
	model interface{}

	// Common fields
	tableName string
	conds     []*define.Condition

	// Query specific fields
	fieldList    []string
	orderByExprs []OrderBy // 修改为 OrderBy 切片
	limitCount   int
	offsetCount  int

	// Update specific fields
	updateFields map[string]interface{}

	// Insert specific fields
	insertFields map[string]interface{}
	batchValues  []map[string]interface{}
}

// Table sets the table name for the chain
func (c *Chain) Table(table string) *Chain {
	c.tableName = table
	return c
}

// Fields sets the fields to select
func (c *Chain) Fields(fields ...string) *Chain {
	c.fieldList = fields
	return c
}

// OrderBy adds an ascending order by clause
func (c *Chain) OrderBy(field string) *Chain {
	c.orderByExprs = append(c.orderByExprs, OrderBy{Field: field, Type: OrderAsc})
	return c
}

// OrderByDesc adds a descending order by clause
func (c *Chain) OrderByDesc(field string) *Chain {
	c.orderByExprs = append(c.orderByExprs, OrderBy{Field: field, Type: OrderDesc})
	return c
}

// buildOrderByExpr builds the ORDER BY expression
func (c *Chain) buildOrderByExpr() string {
	if len(c.orderByExprs) == 0 {
		return ""
	}

	var orders []string
	for _, order := range c.orderByExprs {
		if order.Type == OrderDesc {
			orders = append(orders, order.Field+" DESC")
		} else {
			orders = append(orders, order.Field)
		}
	}
	return strings.Join(orders, ", ")
}

// Limit sets the limit count
func (c *Chain) Limit(count int) *Chain {
	c.limitCount = count
	return c
}

// Offset sets the offset count
func (c *Chain) Offset(count int) *Chain {
	c.offsetCount = count
	return c
}

// Where adds a where condition with custom operator
func (c *Chain) Where(field string, op define.OpType, value interface{}) *Chain {
	c.conds = append(c.conds, define.NewCondition(field, op, value))
	return c
}

// Eq adds an equals condition
func (c *Chain) Eq(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.Eq(field, value))
	return c
}

// Ne adds a not equals condition
func (c *Chain) Ne(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.Ne(field, value))
	return c
}

// Gt adds a greater than condition
func (c *Chain) Gt(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.Gt(field, value))
	return c
}

// Ge adds a greater than or equal condition
func (c *Chain) Ge(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.Ge(field, value))
	return c
}

// Lt adds a less than condition
func (c *Chain) Lt(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.Lt(field, value))
	return c
}

// Le adds a less than or equal condition
func (c *Chain) Le(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.Le(field, value))
	return c
}

// Like adds a LIKE condition
func (c *Chain) Like(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.Like(field, value))
	return c
}

// NotLike adds a NOT LIKE condition
func (c *Chain) NotLike(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.NotLike(field, value))
	return c
}

// In adds an IN condition
func (c *Chain) In(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.In(field, value))
	return c
}

// NotIn adds a NOT IN condition
func (c *Chain) NotIn(field string, value interface{}) *Chain {
	c.conds = append(c.conds, define.NotIn(field, value))
	return c
}

// IsNull adds an IS NULL condition
func (c *Chain) IsNull(field string) *Chain {
	c.conds = append(c.conds, define.IsNull(field))
	return c
}

// IsNotNull adds an IS NOT NULL condition
func (c *Chain) IsNotNull(field string) *Chain {
	c.conds = append(c.conds, define.IsNotNull(field))
	return c
}

// Between adds a BETWEEN condition
func (c *Chain) Between(field string, start, end interface{}) *Chain {
	c.conds = append(c.conds, define.Between(field, start, end))
	return c
}

// NotBetween adds a NOT BETWEEN condition
func (c *Chain) NotBetween(field string, start, end interface{}) *Chain {
	c.conds = append(c.conds, define.NotBetween(field, start, end))
	return c
}

// OrEq adds an OR equals condition
func (c *Chain) OrEq(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.Eq(field, value))
	} else {
		c.conds = append(c.conds, define.Eq(field, value))
	}
	return c
}

// OrNe adds an OR not equals condition
func (c *Chain) OrNe(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.Ne(field, value))
	} else {
		c.conds = append(c.conds, define.Ne(field, value))
	}
	return c
}

// OrGt adds an OR greater than condition
func (c *Chain) OrGt(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.Gt(field, value))
	} else {
		c.conds = append(c.conds, define.Gt(field, value))
	}
	return c
}

// OrGe adds an OR greater than or equal condition
func (c *Chain) OrGe(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.Ge(field, value))
	} else {
		c.conds = append(c.conds, define.Ge(field, value))
	}
	return c
}

// OrLt adds an OR less than condition
func (c *Chain) OrLt(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.Lt(field, value))
	} else {
		c.conds = append(c.conds, define.Lt(field, value))
	}
	return c
}

// OrLe adds an OR less than or equal condition
func (c *Chain) OrLe(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.Le(field, value))
	} else {
		c.conds = append(c.conds, define.Le(field, value))
	}
	return c
}

// OrLike adds an OR LIKE condition
func (c *Chain) OrLike(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.Like(field, value))
	} else {
		c.conds = append(c.conds, define.Like(field, value))
	}
	return c
}

// OrNotLike adds an OR NOT LIKE condition
func (c *Chain) OrNotLike(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.NotLike(field, value))
	} else {
		c.conds = append(c.conds, define.NotLike(field, value))
	}
	return c
}

// OrIn adds an OR IN condition
func (c *Chain) OrIn(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.In(field, value))
	} else {
		c.conds = append(c.conds, define.In(field, value))
	}
	return c
}

// OrNotIn adds an OR NOT IN condition
func (c *Chain) OrNotIn(field string, value interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.NotIn(field, value))
	} else {
		c.conds = append(c.conds, define.NotIn(field, value))
	}
	return c
}

// OrIsNull adds an OR IS NULL condition
func (c *Chain) OrIsNull(field string) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.IsNull(field))
	} else {
		c.conds = append(c.conds, define.IsNull(field))
	}
	return c
}

// OrIsNotNull adds an OR IS NOT NULL condition
func (c *Chain) OrIsNotNull(field string) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.IsNotNull(field))
	} else {
		c.conds = append(c.conds, define.IsNotNull(field))
	}
	return c
}

// OrBetween adds an OR BETWEEN condition
func (c *Chain) OrBetween(field string, start, end interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.Between(field, start, end))
	} else {
		c.conds = append(c.conds, define.Between(field, start, end))
	}
	return c
}

// OrNotBetween adds an OR NOT BETWEEN condition
func (c *Chain) OrNotBetween(field string, start, end interface{}) *Chain {
	if len(c.conds) > 0 {
		lastCond := c.conds[len(c.conds)-1]
		lastCond.Or(define.NotBetween(field, start, end))
	} else {
		c.conds = append(c.conds, define.NotBetween(field, start, end))
	}
	return c
}

// Set sets update fields
func (c *Chain) Set(field string, value interface{}) *Chain {
	if c.updateFields == nil {
		c.updateFields = make(map[string]interface{})
	}
	c.updateFields[field] = value
	return c
}

// Values sets insert fields
func (c *Chain) Values(fields map[string]interface{}) *Chain {
	c.insertFields = fields
	return c
}

// BatchValues sets batch insert values
func (c *Chain) BatchValues(values []map[string]interface{}) *Chain {
	c.batchValues = values
	return c
}

// From sets the table name and conditions from a struct or string
func (c *Chain) From(model interface{}) *Chain {
	// Store the model for potential ID callback
	c.model = model

	// Handle string type parameter
	if tableName, ok := model.(string); ok {
		c.tableName = tableName
		return c
	}

	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	// Set table name if not already set
	if c.tableName == "" {
		c.tableName = getTableNameFromStruct(modelType, model)
	}

	// Get non-empty fields
	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
	}

	// Get fields and values
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		value := modelValue.Field(i)

		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}

		// Parse tag to get column name
		parts := strings.Split(tag, ",")
		columnName := parts[0]

		// Handle special cases for query model
		if strings.HasSuffix(modelType.Name(), "Query") {
			if !value.IsZero() {
				switch {
				case strings.HasPrefix(field.Name, "Min"):
					c.Where(strings.TrimPrefix(columnName, "min_"), define.OpGe, value.Interface())
				case strings.HasPrefix(field.Name, "Max"):
					c.Where(strings.TrimPrefix(columnName, "max_"), define.OpLe, value.Interface())
				case field.Name == "IsActive":
					c.Where("active", define.OpEq, value.Interface())
				default:
					c.Where(columnName, define.OpEq, value.Interface())
				}
			}
			continue
		}

		// Handle normal model fields
		if !value.IsZero() {
			if field.Name == "ID" {
				c.Where(columnName, define.OpEq, value.Interface())
			} else {
				if c.insertFields == nil {
					c.insertFields = make(map[string]interface{})
				}
				c.insertFields[columnName] = value.Interface()
			}
		}
	}

	return c
}

// List executes a SELECT query and returns all results
func (c *Chain) List() *QueryResult {
	orderByExpr := c.buildOrderByExpr()
	sqlStr, args := c.factory.BuildSelect(c.tableName, c.fieldList, c.conds, orderByExpr, c.limitCount, c.offsetCount)
	if define.Debug {
		// Convert pointer values to actual values for logging
		logArgs := make([]interface{}, len(args))
		for i, arg := range args {
			if reflect.TypeOf(arg).Kind() == reflect.Ptr {
				logArgs[i] = reflect.ValueOf(arg).Elem().Interface()
			} else {
				logArgs[i] = arg
			}
		}
		log.Printf("[SQL] %s %v", sqlStr, logArgs)
	}

	var rows *sql.Rows
	var err error
	if c.tx != nil {
		rows, err = c.tx.Query(sqlStr, args...)
	} else {
		rows, err = c.db.DB.Query(sqlStr, args...)
	}
	if err != nil {
		return &QueryResult{err: err}
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &QueryResult{err: err}
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		err = rows.Scan(values...)
		if err != nil {
			return &QueryResult{err: err}
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			// Convert column name to snake_case if it's in CamelCase
			if strings.ToLower(col) != col {
				var result []rune
				for i, r := range col {
					if i > 0 && r >= 'A' && r <= 'Z' {
						result = append(result, '_')
					}
					result = append(result, unicode.ToLower(r))
				}
				col = string(result)
			}
			row[col] = val
		}
		result = append(result, row)
	}

	return &QueryResult{
		Data:    result,
		Columns: columns,
	}
}

// First returns the first result
func (c *Chain) First() *QueryResult {
	c.Limit(1)
	return c.List()
}

// Last returns the last result
func (c *Chain) Last() *QueryResult {
	if len(c.orderByExprs) == 0 {
		c.OrderByDesc("id")
	} else {
		// 反转所有排序的方向
		newOrders := make([]OrderBy, len(c.orderByExprs))
		for i, order := range c.orderByExprs {
			if order.Type == OrderAsc {
				newOrders[i] = OrderBy{Field: order.Field, Type: OrderDesc}
			} else {
				newOrders[i] = OrderBy{Field: order.Field, Type: OrderAsc}
			}
		}
		c.orderByExprs = newOrders
	}
	c.Limit(1)
	return c.List()
}

// One returns exactly one result
func (c *Chain) One() *QueryResult {
	result := c.List()
	if result.Size() != 1 {
		return &QueryResult{err: fmt.Errorf("expected 1 result, got %d", result.Size())}
	}
	return result
}

// Save executes an INSERT or UPDATE query
func (c *Chain) Save() (define.Result, error) {
	if len(c.conds) > 0 {
		// If there are conditions, do an update
		if c.updateFields == nil {
			c.updateFields = make(map[string]interface{})
		}
		sqlStr, args := c.factory.BuildUpdate(c.tableName, c.updateFields, c.conds)
		if sqlStr == "" {
			return define.Result{}, fmt.Errorf("no fields to update")
		}
		if define.Debug {
			log.Printf("[SQL] %s %v", sqlStr, args)
		}
		if c.tx != nil {
			result, err := c.tx.Exec(sqlStr, args...)
			if err != nil {
				return define.Result{}, err
			}
			affected, _ := result.RowsAffected()
			return define.Result{Affected: affected}, nil
		}
		result, err := c.db.DB.Exec(sqlStr, args...)
		if err != nil {
			return define.Result{}, err
		}
		affected, _ := result.RowsAffected()
		return define.Result{Affected: affected}, nil
	}

	// Otherwise, do an insert
	if len(c.batchValues) > 0 {
		sqlStr, args := c.factory.BuildBatchInsert(c.tableName, c.batchValues)
		if define.Debug {
			log.Printf("[SQL] %s %v", sqlStr, args)
		}
		if c.tx != nil {
			result, err := c.tx.Exec(sqlStr, args...)
			if err != nil {
				return define.Result{}, err
			}
			affected, _ := result.RowsAffected()
			lastID, _ := result.LastInsertId()
			return define.Result{ID: lastID, Affected: affected}, nil
		}
		result, err := c.db.DB.Exec(sqlStr, args...)
		if err != nil {
			return define.Result{}, err
		}
		affected, _ := result.RowsAffected()
		lastID, _ := result.LastInsertId()
		return define.Result{ID: lastID, Affected: affected}, nil
	}

	sqlStr, args := c.factory.BuildInsert(c.tableName, c.insertFields)
	if define.Debug {
		log.Printf("[SQL] %s %v", sqlStr, args)
	}

	// For PostgreSQL, we need to scan the returned ID
	if strings.Contains(sqlStr, "RETURNING") {
		var id int64
		var err error
		if c.tx != nil {
			err = c.tx.QueryRow(sqlStr, args...).Scan(&id)
		} else {
			err = c.db.DB.QueryRow(sqlStr, args...).Scan(&id)
		}
		if err != nil {
			return define.Result{}, err
		}

		// Try to set ID back to the entity if it exists
		if c.model != nil {
			if modelValue := reflect.ValueOf(c.model); modelValue.Kind() == reflect.Ptr {
				if idField := modelValue.Elem().FieldByName("ID"); idField.IsValid() && idField.CanSet() {
					idField.SetInt(id)
				}
			}
		}

		return define.Result{ID: id, Affected: 1}, nil
	}

	var result sql.Result
	var err error

	if c.tx != nil {
		result, err = c.tx.Exec(sqlStr, args...)
	} else {
		result, err = c.db.DB.Exec(sqlStr, args...)
	}

	if err != nil {
		return define.Result{}, err
	}

	affected, _ := result.RowsAffected()
	lastID, _ := result.LastInsertId()

	// Try to set ID back to the entity if it exists
	if c.model != nil {
		if modelValue := reflect.ValueOf(c.model); modelValue.Kind() == reflect.Ptr {
			if idField := modelValue.Elem().FieldByName("ID"); idField.IsValid() && idField.CanSet() {
				idField.SetInt(lastID)
			}
		}
	}

	return define.Result{ID: lastID, Affected: affected}, nil
}

// Update executes an UPDATE query
func (c *Chain) Update() (sql.Result, error) {
	if len(c.updateFields) == 0 && len(c.insertFields) > 0 {
		c.updateFields = c.insertFields
	}
	sql, args := c.factory.BuildUpdate(c.tableName, c.updateFields, c.conds)
	if define.Debug {
		log.Printf("[SQL] %s %v\n", sql, args)
	}
	if c.tx != nil {
		return c.tx.Exec(sql, args...)
	}
	return c.db.DB.Exec(sql, args...)
}

// Delete executes a DELETE query
func (c *Chain) Delete() (sql.Result, error) {
	sql, args := c.factory.BuildDelete(c.tableName, c.conds)
	if define.Debug {
		log.Printf("[SQL] %s %v", sql, args)
	}
	if c.tx != nil {
		return c.tx.Exec(sql, args...)
	}
	return c.db.DB.Exec(sql, args...)
}

// Page sets the page number and page size for pagination
func (c *Chain) Page(pageNum, pageSize int) *Chain {
	if pageNum < 1 {
		pageNum = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	c.limitCount = pageSize
	c.offsetCount = (pageNum - 1) * pageSize
	return c
}

// Into scans the result into a struct or slice of structs
func (c *Chain) Into(dest interface{}) error {
	result := c.List()
	if result.err != nil {
		return result.err
	}
	return result.Into(dest)
}

// RawQuery executes a raw SQL query
func (c *Chain) RawQuery(sqlStr string, args ...interface{}) *QueryResult {
	if define.Debug {
		log.Printf("[SQL] %s %v", sqlStr, args)
	}

	var rows *sql.Rows
	var err error
	if c.tx != nil {
		rows, err = c.tx.Query(sqlStr, args...)
	} else {
		rows, err = c.db.DB.Query(sqlStr, args...)
	}
	if err != nil {
		return &QueryResult{err: err}
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &QueryResult{err: err}
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		err = rows.Scan(values...)
		if err != nil {
			return &QueryResult{err: err}
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			row[col] = val
		}
		result = append(result, row)
	}

	return &QueryResult{
		Data:    result,
		Columns: columns,
	}
}

// RawExecute executes a raw SQL statement with args
func (c *Chain) RawExecute(sql string, args ...interface{}) (sql.Result, error) {
	if define.Debug {
		log.Printf("[SQL] %s %v\n", sql, args)
	}
	if c.tx != nil {
		return c.tx.Exec(sql, args...)
	}
	return c.db.DB.Exec(sql, args...)
}

// QueryResult represents a query result
type QueryResult struct {
	Data    []map[string]interface{} `json:"data"`
	Columns []string                 `json:"columns"`
	err     error
}

// Error returns the error if any
func (qr *QueryResult) Error() error {
	return qr.err
}

// Empty returns true if the result is empty
func (qr *QueryResult) Empty() bool {
	return len(qr.Data) == 0
}

// Size returns the number of rows in the result
func (qr *QueryResult) Size() int {
	return len(qr.Data)
}

// Into scans the result into a slice of structs
func (qr *QueryResult) Into(dest interface{}) error {
	if qr.err != nil {
		return qr.err
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

	// Create a new slice with the correct capacity
	newSlice := reflect.MakeSlice(sliceValue.Type(), 0, len(qr.Data))

	// Create a map of column names to struct fields
	fieldMap := make(map[string]reflect.StructField)
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}
		// Parse tag to get column name
		parts := strings.Split(tag, ",")
		columnName := parts[0]
		fieldMap[columnName] = field
	}

	// Iterate through each map and create struct instances
	for _, item := range qr.Data {
		// Create a new struct instance
		structPtr := reflect.New(elemType)
		structVal := structPtr.Elem()

		// Fill the struct fields
		for key, value := range item {
			// Find the corresponding field
			field, ok := fieldMap[key]
			if !ok {
				continue
			}

			// Set the field value
			fieldVal := structVal.FieldByName(field.Name)
			if err := setFieldValue(fieldVal, value); err != nil {
				return fmt.Errorf("failed to set field %s: %v", field.Name, err)
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
		value = valueVal.Elem().Interface()
	}

	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := value.(type) {
		case int64:
			field.SetInt(v)
		case int32:
			field.SetInt(int64(v))
		case int:
			field.SetInt(int64(v))
		case []uint8:
			i, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return err
			}
			field.SetInt(i)
		case string:
			i, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return err
			}
			field.SetInt(i)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := value.(type) {
		case uint64:
			field.SetUint(v)
		case uint32:
			field.SetUint(uint64(v))
		case uint:
			field.SetUint(uint64(v))
		case []uint8:
			i, err := strconv.ParseUint(string(v), 10, 64)
			if err != nil {
				return err
			}
			field.SetUint(i)
		case string:
			i, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return err
			}
			field.SetUint(i)
		}
	case reflect.Float32, reflect.Float64:
		switch v := value.(type) {
		case float64:
			field.SetFloat(v)
		case float32:
			field.SetFloat(float64(v))
		case []uint8:
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return err
			}
			field.SetFloat(f)
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return err
			}
			field.SetFloat(f)
		}
	case reflect.String:
		switch v := value.(type) {
		case string:
			field.SetString(v)
		case []uint8:
			field.SetString(string(v))
		}
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			field.SetBool(v)
		case []uint8:
			b, err := strconv.ParseBool(string(v))
			if err != nil {
				return err
			}
			field.SetBool(b)
		case string:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			field.SetBool(b)
		}
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			switch v := value.(type) {
			case time.Time:
				field.Set(reflect.ValueOf(v))
			case []uint8:
				t, err := time.Parse("2006-01-02 15:04:05", string(v))
				if err != nil {
					return err
				}
				field.Set(reflect.ValueOf(t))
			case string:
				t, err := time.Parse("2006-01-02 15:04:05", v)
				if err != nil {
					return err
				}
				field.Set(reflect.ValueOf(t))
			}
		}
	}
	return nil
}

// getTableNameFromStruct derives table name from struct name or ITableModel interface
func getTableNameFromStruct(t reflect.Type, model interface{}) string {
	// Check if model implements ITableModel interface
	if tableModel, ok := model.(define.ITableModel); ok {
		if tableName := tableModel.TableName(); tableName != "" {
			return tableName
		}
	}

	tableName := t.Name()

	// Handle query struct
	tableName = strings.TrimSuffix(tableName, "Query")

	// Convert CamelCase to snake_case
	var result []rune
	for i, r := range tableName {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result = append(result, '_')
		}
		result = append(result, r)
	}
	tableName = strings.ToLower(string(result))

	// Remove common suffixes
	suffixes := []string{"_model", "_entity", "_struct"}
	for _, suffix := range suffixes {
		tableName = strings.TrimSuffix(tableName, suffix)
	}

	// Add 's' for plural form
	if !strings.HasSuffix(tableName, "s") {
		tableName += "s"
	}

	return tableName
}

// CreateTable creates a table based on the model struct
func (c *Chain) CreateTable(model interface{}) error {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		return fmt.Errorf("model must be a struct or pointer to struct")
	}

	// Get table name if not set
	if c.tableName == "" {
		c.tableName = getTableNameFromStruct(modelType, model)
	}

	// Check if model implements ITableModel interface
	if tableModel, ok := model.(define.ITableModel); ok {
		if createSql := tableModel.CreateSql(); createSql != "" {
			if define.Debug {
				log.Printf("[SQL] %s\n", createSql)
			}
			_, err := c.db.DB.Exec(createSql)
			if err != nil {
				log.Printf("[ERROR] Failed to create table: %v\n", err)
				log.Printf("[SQL] %s\n", createSql)
			}
			return err
		}
	}

	// Build and execute CREATE TABLE statement using default logic
	sql := c.factory.BuildCreateTable(c.tableName, modelType)
	if define.Debug {
		log.Printf("[SQL] %s\n", sql)
	}
	_, err := c.db.DB.Exec(sql)
	if err != nil {
		log.Printf("[ERROR] Failed to create table: %v\n", err)
		log.Printf("[SQL] %s\n", sql)
	}
	return err
}

// Begin starts a new transaction with optional isolation level
func (c *Chain) Begin() error {
	if c.tx != nil {
		return fmt.Errorf("transaction already in progress")
	}

	var tx *sql.Tx
	var err error

	if c.isolationLevel != 0 {
		tx, err = c.db.DB.BeginTx(context.Background(), &sql.TxOptions{
			Isolation: c.isolationLevel,
		})
	} else {
		tx, err = c.db.DB.Begin()
	}

	if err != nil {
		return err
	}

	// Create a backup of current chain
	originalChain := &Chain{
		db:             c.db,
		factory:        c.factory,
		tableName:      c.tableName,
		conds:          c.conds,
		fieldList:      c.fieldList,
		orderByExprs:   c.orderByExprs,
		limitCount:     c.limitCount,
		offsetCount:    c.offsetCount,
		updateFields:   c.updateFields,
		insertFields:   c.insertFields,
		batchValues:    c.batchValues,
		isolationLevel: c.isolationLevel,
	}

	// Update current chain with transaction
	c.originalChain = originalChain
	c.tx = tx

	return nil
}

// Commit commits the transaction
func (c *Chain) Commit() error {
	if c.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}

	err := c.tx.Commit()
	if err != nil {
		return err
	}

	c.cleanup()
	return nil
}

// Rollback rolls back the transaction
func (c *Chain) Rollback() error {
	if c.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}

	err := c.tx.Rollback()
	if err != nil {
		return err
	}

	c.cleanup()
	return nil
}

// cleanup restores the chain to its original state
func (c *Chain) cleanup() {
	if c.originalChain == nil {
		return
	}

	// Restore original values
	c.db = c.originalChain.db
	c.factory = c.originalChain.factory
	c.tableName = c.originalChain.tableName
	c.conds = c.originalChain.conds
	c.fieldList = c.originalChain.fieldList
	c.orderByExprs = c.originalChain.orderByExprs
	c.limitCount = c.originalChain.limitCount
	c.offsetCount = c.originalChain.offsetCount
	c.updateFields = c.originalChain.updateFields
	c.insertFields = c.originalChain.insertFields
	c.batchValues = c.originalChain.batchValues

	// Clear transaction and original chain
	c.tx = nil
	c.originalChain = nil
}

// Transaction executes a function within a transaction
func (c *Chain) Transaction(fn func(*Chain) error) error {
	err := c.Begin()
	if err != nil {
		return err
	}

	err = fn(c)
	if err != nil {
		// Rollback on error
		if rbErr := c.Rollback(); rbErr != nil {
			return fmt.Errorf("error rolling back: %v (original error: %v)", rbErr, err)
		}
		return err
	}

	// Commit the transaction
	if err = c.Commit(); err != nil {
		return fmt.Errorf("error committing: %v", err)
	}

	return nil
}

// IsInTransaction returns whether the chain is currently in a transaction
func (c *Chain) IsInTransaction() bool {
	return c.tx != nil
}

// SetIsolationLevel sets the isolation level for the next transaction
func (c *Chain) SetIsolationLevel(level sql.IsolationLevel) *Chain {
	c.isolationLevel = level
	return c
}

// Savepoint creates a savepoint with the given name
func (c *Chain) Savepoint(name string) error {
	if !c.IsInTransaction() {
		return fmt.Errorf("savepoint can only be created within a transaction")
	}
	_, err := c.tx.Exec(fmt.Sprintf("SAVEPOINT %s", name))
	return err
}

// RollbackTo rolls back to the specified savepoint
func (c *Chain) RollbackTo(name string) error {
	if !c.IsInTransaction() {
		return fmt.Errorf("rollback to savepoint can only be done within a transaction")
	}
	_, err := c.tx.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name))
	return err
}

// ReleaseSavepoint releases the specified savepoint
func (c *Chain) ReleaseSavepoint(name string) error {
	if !c.IsInTransaction() {
		return fmt.Errorf("release savepoint can only be done within a transaction")
	}
	_, err := c.tx.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", name))
	return err
}

// First returns the first result or error if no results
func (qr *QueryResult) First() *QueryResult {
	if qr.err != nil {
		return qr
	}
	if len(qr.Data) > 0 {
		return &QueryResult{
			Data:    qr.Data[:1],
			Columns: qr.Columns,
		}
	}
	return &QueryResult{err: sql.ErrNoRows}
}

// Where2 adds a condition directly
func (c *Chain) Where2(cond *define.Condition) *Chain {
	c.conds = append(c.conds, cond)
	return c
}

// NewCondition creates a new condition with AND join type
func (c *Chain) NewCondition() *define.Condition {
	return &define.Condition{Join: define.JoinAnd}
}

// WhereGroup starts a new condition group with AND join type
func (c *Chain) WhereGroup() *define.Condition {
	cond := &define.Condition{Join: define.JoinAnd, IsSubGroup: true}
	c.conds = append(c.conds, cond)
	return cond
}

// OrWhereGroup starts a new condition group with OR join type
func (c *Chain) OrWhereGroup() *define.Condition {
	cond := &define.Condition{Join: define.JoinOr, IsSubGroup: true}
	c.conds = append(c.conds, cond)
	return cond
}
