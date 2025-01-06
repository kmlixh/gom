package gom

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/kmlixh/gom/v4/define"
)

// Chain represents the base chain structure
type Chain struct {
	db      *DB
	factory define.SQLFactory
	tx      *sql.Tx

	// Store original chain for transaction rollback
	originalChain *Chain

	// Transaction isolation level
	isolationLevel sql.IsolationLevel

	// Transaction state tracking
	inTransaction bool
	txError       error

	// Model for potential ID callback
	model interface{}

	// Common fields
	tableName string
	conds     []*define.Condition

	// Query specific fields
	fieldList    []string
	orderByExprs []define.OrderBy
	limitCount   int
	offsetCount  int

	// Fields for update and insert operations
	fieldMap    map[string]interface{}
	fieldOrder  []string
	batchValues []map[string]interface{}
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
	c.orderByExprs = append(c.orderByExprs, define.OrderBy{Field: field, Type: define.OrderAsc})
	return c
}

// OrderByDesc adds a descending order by clause
func (c *Chain) OrderByDesc(field string) *Chain {
	c.orderByExprs = append(c.orderByExprs, define.OrderBy{Field: field, Type: define.OrderDesc})
	return c
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
	values := make([]interface{}, 0)
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			values = append(values, v.Index(i).Interface())
		}
	} else {
		values = append(values, value)
	}
	c.conds = append(c.conds, define.In(field, values...))
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
	if c.fieldMap == nil {
		c.fieldMap = make(map[string]interface{})
	}
	c.fieldMap[field] = value
	c.fieldOrder = append(c.fieldOrder, field)
	return c
}

// Values sets insert fields
func (c *Chain) Values(fields map[string]interface{}) *Chain {
	c.fieldMap = fields
	// Reset field order
	c.fieldOrder = make([]string, 0, len(fields))
	for field := range fields {
		c.fieldOrder = append(c.fieldOrder, field)
	}
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
				if c.fieldMap == nil {
					c.fieldMap = make(map[string]interface{})
				}
				c.fieldMap[columnName] = value.Interface()
				if !contains(c.fieldOrder, columnName) {
					c.fieldOrder = append(c.fieldOrder, columnName)
				}
			}
		} else if !strings.HasSuffix(modelType.Name(), "Query") {
			// For non-query models, include zero-value fields in INSERT operations
			if field.Name != "ID" {
				if c.fieldMap == nil {
					c.fieldMap = make(map[string]interface{})
				}
				c.fieldMap[columnName] = value.Interface()
				if !contains(c.fieldOrder, columnName) {
					c.fieldOrder = append(c.fieldOrder, columnName)
				}
			}
		}
	}

	return c
}

// List executes a SELECT query and returns all results
func (c *Chain) List(dest ...interface{}) *QueryResult {
	result := c.list()
	if len(dest) > 0 && result.err == nil {
		result.err = result.Into(dest[0])
	}
	return result
}

// list is the internal implementation of List
func (c *Chain) list() *QueryResult {
	orderByExpr := c.buildOrderBy()
	sqlStr, args := c.factory.BuildSelect(c.tableName, c.fieldList, c.conds, orderByExpr, c.limitCount, c.offsetCount)
	if define.Debug {
		log.Printf("[SQL] %s %v\n", sqlStr, args)
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

	// 获取列类型信息
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return &QueryResult{err: err}
	}

	// 打印列类型信息
	if define.Debug {
		log.Printf("Column Types:")
		for _, ct := range columnTypes {
			log.Printf("  %s: DatabaseTypeName=%s, ScanType=%v",
				ct.Name(), ct.DatabaseTypeName(), ct.ScanType())
		}
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

			// 打印实际值的类型
			if define.Debug {
				log.Printf("Column %s: value type=%T, value=%v", col, val, val)
			}

			// 根据列类型进行转换
			if val != nil {
				colType := columnTypes[i]
				dbTypeName := strings.ToUpper(colType.DatabaseTypeName())
				scanType := colType.ScanType()

				switch {
				case strings.Contains(dbTypeName, "CHAR") ||
					strings.Contains(dbTypeName, "TEXT") ||
					strings.Contains(dbTypeName, "VARCHAR") ||
					strings.Contains(dbTypeName, "JSON"):
					if byteVal, ok := val.([]uint8); ok {
						val = string(byteVal)
					}
				case strings.Contains(dbTypeName, "TINYINT") ||
					strings.Contains(dbTypeName, "BOOL"):
					// 转换为 bool
					switch v := val.(type) {
					case []uint8:
						if string(v) == "1" || strings.ToLower(string(v)) == "true" {
							val = true
						} else {
							val = false
						}
					case int64:
						val = v != 0
					case int32:
						val = v != 0
					case int:
						val = v != 0
					case int8:
						val = v != 0
					case bool:
						val = v
					}
					// 如果是 sql.NullInt64 或 sql.NullBool，需要特殊处理
					if scanType != nil {
						switch scanType.String() {
						case "sql.NullInt64":
							if v, ok := val.(int64); ok {
								val = v != 0
							}
						case "sql.NullBool":
							if v, ok := val.(bool); ok {
								val = v
							}
						}
					}
				case strings.Contains(dbTypeName, "INT") ||
					strings.Contains(dbTypeName, "BIGINT"):
					// 转换为 int64
					switch v := val.(type) {
					case []uint8:
						val, _ = strconv.ParseInt(string(v), 10, 64)
					case int32:
						val = int64(v)
					case int:
						val = int64(v)
					}
				case strings.Contains(dbTypeName, "FLOAT") ||
					strings.Contains(dbTypeName, "DOUBLE") ||
					strings.Contains(dbTypeName, "DECIMAL"):
					// 转换为 float64
					switch v := val.(type) {
					case []uint8:
						val, _ = strconv.ParseFloat(string(v), 64)
					case float32:
						val = float64(v)
					}
				case strings.Contains(dbTypeName, "DATE"):
					// 转换为 time.Time，只保留日期部分
					switch v := val.(type) {
					case []uint8:
						dateStr := string(v)
						if t, err := time.ParseInLocation("2006-01-02", dateStr, time.Local); err == nil {
							val = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
						}
					case string:
						if t, err := time.ParseInLocation("2006-01-02", v, time.Local); err == nil {
							val = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
						}
					case time.Time:
						// 对于 MySQL 的 DATE 类型，需要特殊处理时区
						t := v.In(time.Local)
						// 由于 MySQL 的 DATE 类型不包含时区信息，我们需要确保使用本地时区
						year, month, day := t.Date()
						val = time.Date(year, month, day, 0, 0, 0, 0, time.Local)
					}
				case strings.Contains(dbTypeName, "TIMESTAMP") ||
					strings.Contains(dbTypeName, "DATETIME"):
					// 转换为 time.Time
					switch v := val.(type) {
					case []uint8:
						timeStr := string(v)
						formats := []string{
							"2006-01-02 15:04:05",
							"2006-01-02T15:04:05Z",
							time.RFC3339,
						}
						for _, format := range formats {
							if t, err := time.ParseInLocation(format, timeStr, time.Local); err == nil {
								val = t
								break
							}
						}
					case string:
						formats := []string{
							"2006-01-02 15:04:05",
							"2006-01-02T15:04:05Z",
							time.RFC3339,
						}
						for _, format := range formats {
							if t, err := time.ParseInLocation(format, v, time.Local); err == nil {
								val = t
								break
							}
						}
					case time.Time:
						val = v.In(time.Local)
					}
				}
			}

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
func (c *Chain) First(dest ...interface{}) *QueryResult {
	c.Limit(1)
	result := c.list()
	if result.err != nil {
		return result
	}
	if len(result.Data) == 0 {
		return &QueryResult{err: sql.ErrNoRows}
	}
	if len(dest) > 0 {
		result.err = result.Into(dest[0])
	}
	return result
}

// One returns exactly one result
func (c *Chain) One(dest ...interface{}) *QueryResult {
	result := c.list()
	if result.Size() != 1 {
		result.err = fmt.Errorf("expected 1 result, got %d", result.Size())
		return result
	}
	if len(dest) > 0 && result.err == nil {
		// Create a slice of the same type as dest
		destValue := reflect.ValueOf(dest[0])
		if destValue.Kind() != reflect.Ptr {
			result.err = fmt.Errorf("dest must be a pointer")
			return result
		}
		elemType := destValue.Elem().Type()
		sliceType := reflect.SliceOf(elemType)
		slicePtr := reflect.New(sliceType)

		// Convert the result into the slice
		result.err = result.Into(slicePtr.Interface())
		if result.err != nil {
			return result
		}

		// Set the first element back to dest
		destValue.Elem().Set(slicePtr.Elem().Index(0))
	}
	return result
}

// Save executes an INSERT or UPDATE query
func (c *Chain) Save(models ...interface{}) define.Result {
	// 如果没有提供模型，使用已设置的更新字段
	if len(models) == 0 {
		if len(c.fieldMap) == 0 {
			return define.Result{Error: fmt.Errorf("no fields to update")}
		}
		if len(c.conds) > 0 {
			// Update
			return c.executeUpdate()
		} else {
			// Insert
			return c.executeInsert()
		}
	}

	// 处理提供的模型
	for _, model := range models {
		if model == nil {
			continue
		}

		fields, err := c.extractModelFields(model)
		if err != nil {
			return define.Result{Error: err}
		}

		c.fieldMap = fields
		c.fieldOrder = make([]string, 0, len(fields))
		for field := range fields {
			c.fieldOrder = append(c.fieldOrder, field)
		}

		if len(c.conds) > 0 {
			// Update
			result := c.executeUpdate()
			if result.Error != nil {
				return result
			}
		} else {
			// Insert
			result := c.executeInsert()
			if result.Error != nil {
				return result
			}
		}
	}

	return define.Result{}
}

// executeInsert executes an INSERT query
func (c *Chain) executeInsert() define.Result {
	sqlStr, args := c.factory.BuildInsert(c.tableName, c.fieldMap, c.fieldOrder)
	if define.Debug {
		log.Printf("[SQL] %s %v", sqlStr, args)
	}

	var result sql.Result
	var err error
	if c.tx != nil {
		result, err = c.tx.Exec(sqlStr, args...)
	} else {
		result, err = c.db.DB.Exec(sqlStr, args...)
	}

	if err != nil {
		return define.Result{Error: err}
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return define.Result{Error: err}
	}

	affected, _ := result.RowsAffected()
	return define.Result{
		ID:       lastID,
		Affected: affected,
	}
}

// executeUpdate executes an UPDATE query
func (c *Chain) executeUpdate() define.Result {
	sqlStr, args := c.factory.BuildUpdate(c.tableName, c.fieldMap, c.fieldOrder, c.conds)
	if define.Debug {
		log.Printf("[SQL] %s %v", sqlStr, args)
	}

	var result sql.Result
	var err error
	if c.tx != nil {
		result, err = c.tx.Exec(sqlStr, args...)
	} else {
		result, err = c.db.DB.Exec(sqlStr, args...)
	}

	if err != nil {
		return define.Result{Error: err}
	}

	lastID, _ := result.LastInsertId()
	affected, _ := result.RowsAffected()

	return define.Result{
		ID:       lastID,
		Affected: affected,
	}
}

// Update updates records with the given fields
func (c *Chain) Update(fields map[string]interface{}) define.Result {
	if len(c.conds) > 0 {
		// Update
		c.fieldMap = fields
		c.fieldOrder = make([]string, 0, len(fields))
		for field := range fields {
			c.fieldOrder = append(c.fieldOrder, field)
		}
		return c.executeUpdate()
	}

	// Insert
	c.fieldMap = fields
	c.fieldOrder = make([]string, 0, len(fields))
	for field := range fields {
		c.fieldOrder = append(c.fieldOrder, field)
	}
	return c.executeInsert()
}

// Delete executes a DELETE query
func (c *Chain) Delete(models ...interface{}) define.Result {
	// 如果没有提供模型，使用已设置的条件
	if len(models) == 0 {
		sql, args := c.factory.BuildDelete(c.tableName, c.conds)
		if define.Debug {
			log.Printf("[SQL] %s %v", sql, args)
		}
		var sqlResult interface {
			LastInsertId() (int64, error)
			RowsAffected() (int64, error)
		}
		var err error
		if c.tx != nil {
			sqlResult, err = c.tx.Exec(sql, args...)
		} else {
			sqlResult, err = c.db.DB.Exec(sql, args...)
		}
		if err != nil {
			return define.Result{Error: err}
		}
		affected, err := sqlResult.RowsAffected()
		if err != nil {
			return define.Result{Error: err}
		}
		return define.Result{Affected: affected}
	}

	// 如果只有一个模型，不需要事务
	if len(models) == 1 {
		return c.deleteSingleModel(models[0])
	}

	// 多个模型需要使用事务
	return c.deleteMultipleModels(models)
}

// deleteSingleModel deletes a single model without transaction
func (c *Chain) deleteSingleModel(model interface{}) define.Result {
	// 获取模型的主键值
	pkValue, err := c.extractPrimaryKeyValue(model)
	if err != nil {
		return define.Result{Error: err}
	}

	// 使用主键作为删除条件
	c.conds = append(c.conds, &define.Condition{
		Field: "id",
		Op:    define.OpEq,
		Value: pkValue,
	})

	sql, args := c.factory.BuildDelete(c.tableName, c.conds)
	if define.Debug {
		log.Printf("[SQL] %s %v", sql, args)
	}

	var sqlResult interface {
		LastInsertId() (int64, error)
		RowsAffected() (int64, error)
	}
	if c.tx != nil {
		sqlResult, err = c.tx.Exec(sql, args...)
	} else {
		sqlResult, err = c.db.DB.Exec(sql, args...)
	}
	if err != nil {
		return define.Result{Error: err}
	}
	affected, err := sqlResult.RowsAffected()
	if err != nil {
		return define.Result{Error: err}
	}
	return define.Result{Affected: affected}
}

// deleteMultipleModels deletes multiple models within a transaction
func (c *Chain) deleteMultipleModels(models []interface{}) define.Result {
	// 如果已经在事务中，直接使用现有事务
	if c.tx != nil {
		return c.executeMultipleDeletes(models)
	}

	// 开启新事务
	tx, err := c.db.DB.Begin()
	if err != nil {
		return define.Result{Error: fmt.Errorf("failed to begin transaction: %v", err)}
	}

	// 创建新的带事务的 Chain
	txChain := &Chain{
		db:        c.db,
		tx:        tx,
		tableName: c.tableName,
		conds:     c.conds,
		factory:   c.factory,
	}

	result := txChain.executeMultipleDeletes(models)
	if result.Error != nil {
		// 发生错误时回滚事务
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return define.Result{Error: fmt.Errorf("delete failed: %v, rollback failed: %v", result.Error, rollbackErr)}
		}
		return define.Result{Error: fmt.Errorf("delete failed: %v", result.Error)}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return define.Result{Error: fmt.Errorf("failed to commit transaction: %v", err)}
	}

	return result
}

// executeMultipleDeletes executes deletes for multiple models
func (c *Chain) executeMultipleDeletes(models []interface{}) define.Result {
	var totalAffected int64

	for i, model := range models {
		// 获取模型的主键值
		pkValue, err := c.extractPrimaryKeyValue(model)
		if err != nil {
			return define.Result{Error: fmt.Errorf("failed to extract primary key from model %d: %v", i+1, err)}
		}

		// 使用主键作为删除条件
		c.conds = append(c.conds, &define.Condition{
			Field: "id",
			Op:    define.OpEq,
			Value: pkValue,
		})

		sql, args := c.factory.BuildDelete(c.tableName, c.conds)
		if define.Debug {
			log.Printf("[SQL] %s %v", sql, args)
		}

		var sqlResult interface {
			LastInsertId() (int64, error)
			RowsAffected() (int64, error)
		}
		sqlResult, err = c.tx.Exec(sql, args...)
		if err != nil {
			return define.Result{Error: fmt.Errorf("failed to delete model %d: %v", i+1, err)}
		}

		affected, err := sqlResult.RowsAffected()
		if err != nil {
			return define.Result{Error: fmt.Errorf("failed to get affected rows for model %d: %v", i+1, err)}
		}

		if affected == 0 {
			return define.Result{Error: fmt.Errorf("no rows affected when deleting model %d", i+1)}
		}

		totalAffected += affected
	}

	return define.Result{Affected: totalAffected}
}

// extractPrimaryKeyValue extracts the primary key value from a model
func (c *Chain) extractPrimaryKeyValue(model interface{}) (interface{}, error) {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a struct or pointer to struct, got %v", modelType.Kind())
	}

	// 查找主键字段
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		value := modelValue.Field(i)

		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}

		parts := strings.Split(tag, ",")
		columnName := parts[0]

		for _, opt := range parts[1:] {
			if opt == "@" && columnName == "id" {
				if value.IsZero() {
					return nil, fmt.Errorf("primary key value is zero")
				}
				return value.Interface(), nil
			}
		}
	}

	return nil, fmt.Errorf("no primary key field found in model")
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

	// Get column types
	columnTypes, err := rows.ColumnTypes()
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

			// Handle type conversions based on column type
			if val != nil {
				colType := columnTypes[i]
				dbTypeName := strings.ToUpper(colType.DatabaseTypeName())

				switch {
				case strings.Contains(dbTypeName, "CHAR") ||
					strings.Contains(dbTypeName, "TEXT") ||
					strings.Contains(dbTypeName, "VARCHAR") ||
					strings.Contains(dbTypeName, "JSON"):
					if byteVal, ok := val.([]uint8); ok {
						val = string(byteVal)
					}
				case strings.Contains(dbTypeName, "TINYINT") ||
					strings.Contains(dbTypeName, "BOOL"):
					switch v := val.(type) {
					case []uint8:
						if string(v) == "1" || strings.ToLower(string(v)) == "true" {
							val = true
						} else {
							val = false
						}
					case int64:
						val = v != 0
					case int32:
						val = v != 0
					case int:
						val = v != 0
					case int8:
						val = v != 0
					case bool:
						val = v
					}
				case strings.Contains(dbTypeName, "INT") ||
					strings.Contains(dbTypeName, "BIGINT"):
					switch v := val.(type) {
					case []uint8:
						val, _ = strconv.ParseInt(string(v), 10, 64)
					case int32:
						val = int64(v)
					case int:
						val = int64(v)
					}
				case strings.Contains(dbTypeName, "FLOAT") ||
					strings.Contains(dbTypeName, "DOUBLE") ||
					strings.Contains(dbTypeName, "DECIMAL"):
					switch v := val.(type) {
					case []uint8:
						val, _ = strconv.ParseFloat(string(v), 64)
					case float32:
						val = float64(v)
					}
				case strings.Contains(dbTypeName, "DATE") ||
					strings.Contains(dbTypeName, "TIME"):
					switch v := val.(type) {
					case []uint8:
						formats := []string{
							"2006-01-02 15:04:05",
							"2006-01-02T15:04:05Z",
							time.RFC3339,
						}
						for _, format := range formats {
							if t, err := time.ParseInLocation(format, string(v), time.Local); err == nil {
								val = t
								break
							}
						}
					case time.Time:
						val = v.In(time.Local)
					}
				}
			}

			// Handle column aliases in JOIN queries
			colName := col
			if strings.Contains(colName, " AS ") {
				parts := strings.Split(colName, " AS ")
				colName = strings.TrimSpace(parts[len(parts)-1])
			} else if strings.Contains(colName, ".") {
				parts := strings.Split(colName, ".")
				colName = strings.TrimSpace(parts[len(parts)-1])
			}

			// Convert column name to snake_case if it's in CamelCase
			if strings.ToLower(colName) != colName {
				var result []rune
				for i, r := range colName {
					if i > 0 && r >= 'A' && r <= 'Z' {
						result = append(result, '_')
					}
					result = append(result, unicode.ToLower(r))
				}
				colName = string(result)
			}

			// Log column information for debugging
			if define.Debug {
				log.Printf("Column %s: value type=%T, value=%v", colName, val, val)
			}

			row[colName] = val
		}
		result = append(result, row)
	}

	if err = rows.Err(); err != nil {
		return &QueryResult{err: err}
	}

	return &QueryResult{
		Data:    result,
		Columns: columns,
	}
}

// RawExecute executes a raw SQL query
func (c *Chain) RawExecute(sql string, args ...interface{}) define.Result {
	if define.Debug {
		log.Printf("[SQL] %s %v", sql, args)
	}
	var sqlResult interface {
		LastInsertId() (int64, error)
		RowsAffected() (int64, error)
	}
	var err error
	if c.tx != nil {
		sqlResult, err = c.tx.Exec(sql, args...)
	} else {
		sqlResult, err = c.db.DB.Exec(sql, args...)
	}
	if err != nil {
		return define.Result{Error: err}
	}
	lastID, _ := sqlResult.LastInsertId()
	affected, err := sqlResult.RowsAffected()
	if err != nil {
		return define.Result{Error: err}
	}
	return define.Result{ID: lastID, Affected: affected}
}

// QueryResult represents a query result
type QueryResult struct {
	Data    []map[string]any `json:"data"`
	Columns []string         `json:"columns"`
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
		return &DBError{
			Op:      "Into",
			Err:     errors.New("dest must be a pointer"),
			Details: fmt.Sprintf("got %v", destValue.Kind()),
		}
	}

	sliceValue := destValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return &DBError{
			Op:      "Into",
			Err:     errors.New("dest must be a pointer to slice"),
			Details: fmt.Sprintf("got pointer to %v", sliceValue.Kind()),
		}
	}

	// Get the type of slice elements
	elemType := sliceValue.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	if isPtr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		return &DBError{
			Op:      "Into",
			Err:     errors.New("slice elements must be structs"),
			Details: fmt.Sprintf("got %v", elemType.Kind()),
		}
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
	newSlice := reflect.MakeSlice(sliceValue.Type(), 0, len(qr.Data))

	// Iterate over each row in the result
	for _, row := range qr.Data {
		// Create a new struct for this row
		newElem := reflect.New(elemType).Elem()

		// Set each field in the struct
		for colName, val := range row {
			// Convert column name to lowercase for case-insensitive matching
			colName = strings.ToLower(colName)

			// Try to find the field by column name or by field name
			field, ok := fieldMap[colName]
			if !ok {
				// If not found, try to find by removing common prefixes (e.g., "dept_" from "dept_name")
				parts := strings.Split(colName, "_")
				if len(parts) > 1 {
					colName = strings.Join(parts[1:], "_")
					field, ok = fieldMap[colName]
				}
			}
			if ok {
				fieldValue := newElem.FieldByName(field.Name)
				if !fieldValue.CanSet() {
					continue
				}

				// Handle type conversions
				if val == nil {
					continue
				}

				// Handle type conversions based on field type
				switch fieldValue.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					switch v := val.(type) {
					case int64:
						fieldValue.SetInt(v)
					case float64:
						fieldValue.SetInt(int64(v))
					case []uint8:
						if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
							fieldValue.SetInt(i)
						}
					}
				case reflect.Float32, reflect.Float64:
					switch v := val.(type) {
					case float64:
						fieldValue.SetFloat(v)
					case []uint8:
						if f, err := strconv.ParseFloat(string(v), 64); err == nil {
							fieldValue.SetFloat(f)
						}
					}
				case reflect.String:
					switch v := val.(type) {
					case string:
						fieldValue.SetString(v)
					case []uint8:
						fieldValue.SetString(string(v))
					}
				case reflect.Bool:
					switch v := val.(type) {
					case bool:
						fieldValue.SetBool(v)
					case int64:
						fieldValue.SetBool(v != 0)
					case []uint8:
						if b, err := strconv.ParseBool(string(v)); err == nil {
							fieldValue.SetBool(b)
						}
					}
				case reflect.Struct:
					if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
						switch v := val.(type) {
						case time.Time:
							fieldValue.Set(reflect.ValueOf(v))
						case []uint8:
							formats := []string{
								"2006-01-02 15:04:05",
								"2006-01-02T15:04:05Z",
								time.RFC3339,
							}
							for _, format := range formats {
								if t, err := time.ParseInLocation(format, string(v), time.Local); err == nil {
									fieldValue.Set(reflect.ValueOf(t))
									break
								}
							}
						}
					}
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

	// Set the result back to the destination slice
	sliceValue.Set(newSlice)
	return nil
}

// setFieldValue handles type conversion and setting of field values
func setFieldValue(field reflect.Value, value interface{}) error {
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := value.(type) {
		case int64:
			field.SetInt(v)
		case float64:
			field.SetInt(int64(v))
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				field.SetInt(i)
			}
		case []uint8:
			if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				field.SetInt(i)
			}
		}
	case reflect.Float32, reflect.Float64:
		switch v := value.(type) {
		case float64:
			field.SetFloat(v)
		case int64:
			field.SetFloat(float64(v))
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				field.SetFloat(f)
			}
		case []uint8:
			if f, err := strconv.ParseFloat(string(v), 64); err == nil {
				field.SetFloat(f)
			}
		}
	case reflect.String:
		switch v := value.(type) {
		case string:
			field.SetString(v)
		case []uint8:
			field.SetString(string(v))
		default:
			field.SetString(fmt.Sprint(v))
		}
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			field.SetBool(v)
		case int64:
			field.SetBool(v != 0)
		case int:
			field.SetBool(v != 0)
		case string:
			boolVal := strings.ToLower(v) == "true" || v == "1"
			field.SetBool(boolVal)
		}
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			switch v := value.(type) {
			case time.Time:
				field.Set(reflect.ValueOf(v))
			case []uint8:
				formats := []string{
					"2006-01-02 15:04:05",
					"2006-01-02T15:04:05Z",
					time.RFC3339,
				}
				for _, format := range formats {
					if t, err := time.ParseInLocation(format, string(v), time.Local); err == nil {
						field.Set(reflect.ValueOf(t))
						break
					}
				}
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

// Begin starts a new transaction
func (c *Chain) Begin() (*Chain, error) {
	if c.tx != nil {
		return nil, &DBError{
			Op:      "Begin",
			Err:     errors.New("transaction already started"),
			Details: "nested transaction not supported",
		}
	}

	tx, err := c.db.DB.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: c.isolationLevel,
	})
	if err != nil {
		return nil, &DBError{
			Op:      "Begin",
			Err:     err,
			Details: "failed to begin transaction",
		}
	}

	newChain := &Chain{
		db:            c.db,
		factory:       c.factory,
		tx:            tx,
		originalChain: c,
		tableName:     c.tableName,
		conds:         make([]*define.Condition, len(c.conds)),
		fieldList:     make([]string, len(c.fieldList)),
		orderByExprs:  make([]define.OrderBy, len(c.orderByExprs)),
		inTransaction: true,
	}

	copy(newChain.conds, c.conds)
	copy(newChain.fieldList, c.fieldList)
	copy(newChain.orderByExprs, c.orderByExprs)

	return newChain, nil
}

// Commit commits the transaction
func (c *Chain) Commit() error {
	if c.tx == nil {
		return fmt.Errorf("no transaction to commit")
	}
	err := c.tx.Commit()
	if err != nil {
		return err
	}
	c.tx = nil
	c.originalChain = nil
	return nil
}

// Rollback rolls back the transaction
func (c *Chain) Rollback() error {
	if c.tx == nil {
		return fmt.Errorf("no transaction to rollback")
	}
	err := c.tx.Rollback()
	if err != nil {
		return err
	}
	c.tx = nil
	c.originalChain = nil
	return nil
}

// Transaction executes the given function within a transaction
func (c *Chain) Transaction(fn func(*Chain) error) error {
	if c.tx != nil {
		return &DBError{
			Op:      "Transaction",
			Err:     errors.New("nested transaction not supported"),
			Details: "transaction already started",
		}
	}

	tx, err := c.db.DB.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: c.isolationLevel,
	})
	if err != nil {
		return &DBError{
			Op:      "Transaction",
			Err:     err,
			Details: "failed to begin transaction",
		}
	}

	txChain := &Chain{
		db:            c.db,
		factory:       c.factory,
		tx:            tx,
		originalChain: c,
		inTransaction: true,
		tableName:     c.tableName,
		conds:         make([]*define.Condition, len(c.conds)),
		fieldList:     make([]string, len(c.fieldList)),
		orderByExprs:  make([]define.OrderBy, len(c.orderByExprs)),
	}

	copy(txChain.conds, c.conds)
	copy(txChain.fieldList, c.fieldList)
	copy(txChain.orderByExprs, c.orderByExprs)

	err = fn(txChain)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return &DBError{
				Op:      "Transaction",
				Err:     err,
				Details: fmt.Sprintf("rollback failed: %v (original error: %v)", rollbackErr, err),
			}
		}
		return &DBError{
			Op:  "Transaction",
			Err: err,
		}
	}

	if err := tx.Commit(); err != nil {
		return &DBError{
			Op:      "Transaction",
			Err:     err,
			Details: "commit failed",
		}
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
	if cond != nil {
		cond.Join = define.JoinAnd
		c.conds = append(c.conds, cond)
	}
	return c
}

// Or adds a condition with OR join type
func (c *Chain) Or(cond *define.Condition) *Chain {
	if cond != nil {
		cond.Join = define.JoinOr
		c.conds = append(c.conds, cond)
	}
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

// Count returns the count of records
func (c *Chain) Count() (int64, error) {
	return c.Count2("*")
}

// Count2 returns the count of specified field
func (c *Chain) Count2(field string) (int64, error) {
	var count int64
	sqlStr, args := c.factory.BuildSelect(c.tableName, []string{fmt.Sprintf("COUNT(%s) as count", field)}, c.conds, "", 0, 0)
	if define.Debug {
		log.Printf("[SQL] %s %v\n", sqlStr, args)
	}
	var err error
	if c.tx != nil {
		err = c.tx.QueryRow(sqlStr, args...).Scan(&count)
	} else {
		err = c.db.DB.QueryRow(sqlStr, args...).Scan(&count)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return count, nil
}

// Sum calculates the sum of a specific field
func (c *Chain) Sum(field string) (float64, error) {
	var sum sql.NullFloat64
	sqlStr, args := c.factory.BuildSelect(c.tableName, []string{fmt.Sprintf("SUM(%s) as sum_value", field)}, c.conds, "", 0, 0)
	if define.Debug {
		log.Printf("[SQL] %s %v\n", sqlStr, args)
	}
	var err error
	if c.tx != nil {
		err = c.tx.QueryRow(sqlStr, args...).Scan(&sum)
	} else {
		err = c.db.DB.QueryRow(sqlStr, args...).Scan(&sum)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	if !sum.Valid {
		return 0, nil
	}
	return sum.Float64, nil
}

// Avg calculates the average of a specific field
func (c *Chain) Avg(field string) (float64, error) {
	var avg sql.NullFloat64
	sqlStr, args := c.factory.BuildSelect(c.tableName, []string{fmt.Sprintf("AVG(%s) as avg_value", field)}, c.conds, "", 0, 0)
	if define.Debug {
		log.Printf("[SQL] %s %v\n", sqlStr, args)
	}
	var err error
	if c.tx != nil {
		err = c.tx.QueryRow(sqlStr, args...).Scan(&avg)
	} else {
		err = c.db.DB.QueryRow(sqlStr, args...).Scan(&avg)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	if !avg.Valid {
		return 0, nil
	}
	return avg.Float64, nil
}

// PageInfo represents pagination information
type PageInfo struct {
	PageNum     int         `json:"pageNum"`     // 当前页码
	PageSize    int         `json:"pageSize"`    // 每页大小
	Total       int64       `json:"total"`       // 总记录数
	Pages       int         `json:"pages"`       // 总页数
	HasPrev     bool        `json:"hasPrev"`     // 是否有上一页
	HasNext     bool        `json:"hasNext"`     // 是否有下一页
	List        interface{} `json:"list"`        // 当前页数据
	IsFirstPage bool        `json:"isFirstPage"` // 是否是第一页
	IsLastPage  bool        `json:"isLastPage"`  // 是否是最后页
}

// PageInfo executes a paginated query and returns pagination information
func (c *Chain) PageInfo(model interface{}) (*PageInfo, error) {
	// 获取总记录数
	total, err := c.Count()
	if err != nil {
		return nil, err
	}

	// 如果没有设置页码和页大小，设置默认值
	if c.limitCount <= 0 {
		c.limitCount = 10
	}
	if c.offsetCount < 0 {
		c.offsetCount = 0
	}

	// 计算当前页码
	pageNum := (c.offsetCount / c.limitCount) + 1
	pageSize := c.limitCount

	// 计算总页数
	pages := int((total + int64(pageSize) - 1) / int64(pageSize))

	// 获取当前页数据
	var list interface{}
	if model != nil {
		// 如果提供了模型，使用模型类型创建切片
		sliceType := reflect.SliceOf(reflect.TypeOf(model))
		if sliceType.Kind() == reflect.Ptr {
			sliceType = reflect.SliceOf(sliceType.Elem())
		}
		slice := reflect.New(sliceType)
		err = c.Into(slice.Interface())
		if err != nil {
			return nil, err
		}
		list = slice.Elem().Interface()
	} else {
		// 如果没有提供模型，返回原始查询结果
		result := c.List()
		if result.err != nil {
			return nil, result.err
		}
		list = result.Data
	}

	// 构建分页信息
	pageInfo := &PageInfo{
		PageNum:     pageNum,
		PageSize:    pageSize,
		Total:       total,
		Pages:       pages,
		HasPrev:     pageNum > 1,
		HasNext:     pageNum < pages,
		List:        list,
		IsFirstPage: pageNum == 1,
		IsLastPage:  pageNum == pages,
	}

	return pageInfo, nil
}

// BatchInsert performs batch insert operation with the given batch size
func (c *Chain) BatchInsert(batchSize int) (int64, error) {
	// Validate batch size
	if batchSize <= 0 {
		return 0, &DBError{
			Op:  "BatchInsert",
			Err: fmt.Errorf("invalid batch size (batch size must be greater than 0, got %d)", batchSize),
		}
	}

	// Get values to insert
	values := c.batchValues
	if len(values) == 0 {
		return 0, &DBError{
			Op:  "BatchInsert",
			Err: errors.New("no values to insert"),
		}
	}

	// Create context with default timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create progress tracker
	progress := newProgressTracker(int64(len(values)))

	// Process function for each batch
	processBatch := func(ctx context.Context, batch []map[string]interface{}) error {
		// Start transaction for this batch
		tx, err := c.db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}
		defer tx.Rollback()

		// Create chain for transaction
		txChain := c.clone()
		txChain.tx = tx

		// Build and execute insert query
		affected, err := txChain.batchInsertChunk(batch)
		if err != nil {
			return err
		}

		// Update progress
		progress.increment(affected)

		// Commit transaction
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		return nil
	}

	// Process all batches with concurrency and timeout
	err := processBatchesWithTimeout(
		ctx,
		values,
		batchSize,
		4, // Use 4 concurrent goroutines
		30*time.Second,
		processBatch,
	)

	if err != nil {
		return 0, &DBError{
			Op:  "BatchInsert",
			Err: err,
		}
	}

	processed, _ := progress.getProgress()
	return processed, nil
}

// batchInsertChunk performs the actual insert operation for a batch of values
func (c *Chain) batchInsertChunk(values []map[string]interface{}) (int64, error) {
	if len(values) == 0 {
		return 0, nil
	}

	// Get table name
	tableName := c.tableName
	if tableName == "" {
		return 0, errors.New("table name is required")
	}

	// Build insert query
	query, args := c.factory.BuildBatchInsert(tableName, values)

	// Execute query
	var result sql.Result
	var err error
	if c.tx != nil {
		result, err = c.tx.Exec(query, args...)
	} else {
		result, err = c.db.DB.Exec(query, args...)
	}

	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// extractModelFields extracts fields from a model
func (c *Chain) extractModelFields(model interface{}) (map[string]interface{}, error) {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a struct or pointer to struct, got %v", modelType.Kind())
	}

	fields := make(map[string]interface{})

	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		value := modelValue.Field(i)

		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}

		parts := strings.Split(tag, ",")
		columnName := parts[0]

		isPrimary := false
		for _, opt := range parts[1:] {
			if opt == "@" {
				isPrimary = true
				break
			}
		}
		if isPrimary {
			continue
		}

		if !value.IsZero() {
			fields[columnName] = value.Interface()
			if !contains(c.fieldOrder, columnName) {
				c.fieldOrder = append(c.fieldOrder, columnName)
			}
		} else if !strings.HasSuffix(modelType.Name(), "Query") {
			// For non-query models, include zero-value fields in INSERT operations
			if field.Name != "ID" {
				if c.fieldMap == nil {
					c.fieldMap = make(map[string]interface{})
				}
				c.fieldMap[columnName] = value.Interface()
				if !contains(c.fieldOrder, columnName) {
					c.fieldOrder = append(c.fieldOrder, columnName)
				}
			}
		}
	}

	return fields, nil
}

// GroupBy adds a GROUP BY clause
func (c *Chain) GroupBy(fields ...string) *Chain {
	if c.fieldList == nil {
		c.fieldList = make([]string, 0)
	}
	groupByClause := fmt.Sprintf("GROUP BY %s", strings.Join(fields, ", "))
	c.fieldList = append(c.fieldList, groupByClause)
	return c
}

// Having adds a HAVING clause
func (c *Chain) Having(condition interface{}, args ...interface{}) *Chain {
	if c.fieldList == nil {
		c.fieldList = make([]string, 0)
	}

	switch v := condition.(type) {
	case string:
		// Raw SQL condition
		if len(args) > 0 {
			c.conds = append(c.conds, &define.Condition{
				Field: "HAVING " + v,
				Op:    define.OpCustom,
				Value: args,
			})
		} else {
			c.conds = append(c.conds, &define.Condition{
				Field: "HAVING " + v,
				Op:    define.OpCustom,
			})
		}
	case *define.Condition:
		// Single condition
		var op string
		switch v.Op {
		case define.OpEq:
			op = "="
		case define.OpGt:
			op = ">"
		case define.OpLt:
			op = "<"
		case define.OpNe:
			op = "!="
		default:
			op = "="
		}
		havingClause := fmt.Sprintf("HAVING %s %s ?", v.Field, op)
		c.conds = append(c.conds, &define.Condition{
			Field: havingClause,
			Op:    define.OpCustom,
			Value: []interface{}{v.Value},
		})
	default:
		// Invalid condition type
		return c
	}

	return c
}

// buildOrderBy builds the ORDER BY clause
func (c *Chain) buildOrderBy() string {
	if len(c.orderByExprs) == 0 {
		return ""
	}
	return c.factory.BuildOrderBy(c.orderByExprs)
}

// contains checks if a string slice contains a value
func contains(slice []string, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

// clone creates a copy of the chain
func (c *Chain) clone() *Chain {
	return &Chain{
		db:             c.db,
		tx:             c.tx,
		tableName:      c.tableName,
		batchValues:    c.batchValues,
		isolationLevel: c.isolationLevel,
	}
}

// Begin starts a new transaction
func (db *DB) Begin() (*sql.Tx, error) {
	return db.DB.Begin()
}
