package gom

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/kmlixh/gom/v4/define"
)

// QueryStats tracks statistics for a single query execution
type QueryStats struct {
	SQL          string
	Duration     time.Duration
	RowsAffected int64
	StartTime    time.Time
	Args         []interface{}
}

// txStack manages nested transactions using savepoints
type txStack struct {
	savepoints []string
	level      int
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

	// Sensitive data handling
	sensitiveFields map[string]SensitiveOptions

	// Context for transaction
	ctx context.Context

	// Query performance tracking
	queryStats *QueryStats

	// Transaction management
	txStack *txStack
}

// SensitiveType defines the type of sensitive data
type SensitiveType int

const (
	// SensitiveNone indicates no sensitivity
	SensitiveNone SensitiveType = iota
	// SensitivePhone for phone numbers
	SensitivePhone
	// SensitiveEmail for email addresses
	SensitiveEmail
	// SensitiveIDCard for ID card numbers
	SensitiveIDCard
	// SensitiveBankCard for bank card numbers
	SensitiveBankCard
	// SensitiveAddress for addresses
	SensitiveAddress
	// SensitiveEncrypted for encrypted data
	SensitiveEncrypted
)

// EncryptionAlgorithm defines the encryption algorithm to use
type EncryptionAlgorithm string

const (
	// AES256 uses AES-256 encryption
	AES256 EncryptionAlgorithm = "AES256"
	// AES192 uses AES-192 encryption
	AES192 EncryptionAlgorithm = "AES192"
	// AES128 uses AES-128 encryption
	AES128 EncryptionAlgorithm = "AES128"
)

// KeySource defines where encryption keys are sourced from
type KeySource string

const (
	// KeySourceEnv sources keys from environment variables
	KeySourceEnv KeySource = "env"
	// KeySourceFile sources keys from files
	KeySourceFile KeySource = "file"
	// KeySourceVault sources keys from a key vault
	KeySourceVault KeySource = "vault"
)

// EncryptionConfig represents configuration for data encryption
type EncryptionConfig struct {
	Algorithm       EncryptionAlgorithm `json:"algorithm"`
	KeyRotation     time.Duration       `json:"key_rotation"`
	KeySource       KeySource           `json:"key_source"`
	KeySourceConfig map[string]string   `json:"key_source_config"`
}

// SensitiveOptions represents options for sensitive data handling
type SensitiveOptions struct {
	Type       SensitiveType
	Encryption *EncryptionConfig
	Mask       string
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
	if result.err == nil && len(result.Data) > 0 {
		if err := c.processSensitiveResults(result.Data); err != nil {
			result.err = err
			return result
		}
	}
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

	c.startQueryStats(sqlStr, args)
	defer func() {
		if c.queryStats != nil {
			c.endQueryStats(0) // For SELECT queries, we don't track rows affected
		}
	}()

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

	// Print column type information
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

			// Print actual value type
			if define.Debug {
				log.Printf("Column %s: value type=%T, value=%v", col, val, val)
			}

			// Handle type conversions based on column type
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
					// Convert to bool
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
					// Special handling for sql.NullInt64 or sql.NullBool
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
					// Convert to int64
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
					// Convert to float64
					switch v := val.(type) {
					case []uint8:
						val, _ = strconv.ParseFloat(string(v), 64)
					case float32:
						val = float64(v)
					}
				case strings.Contains(dbTypeName, "DATE"):
					// Convert to time.Time, keep only date part
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
						// Special handling for MySQL DATE type timezone
						t := v.In(time.Local)
						year, month, day := t.Date()
						val = time.Date(year, month, day, 0, 0, 0, 0, time.Local)
					}
				case strings.Contains(dbTypeName, "TIMESTAMP") ||
					strings.Contains(dbTypeName, "DATETIME"):
					// Convert to time.Time
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

			row[col] = val
		}
		result = append(result, row)
	}

	return &QueryResult{Data: result}
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
	// Process sensitive data before saving
	if c.fieldMap != nil {
		if err := c.processSensitiveData(c.fieldMap); err != nil {
			return define.Result{Error: err}
		}
	}

	// Call the original Save logic
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

	// Process provided models
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
	return c.BeginChain()
}

// BeginChain starts a new transaction and returns a Chain
func (c *Chain) BeginChain() (*Chain, error) {
	if c.inTransaction {
		return nil, errors.New("transaction already started")
	}

	tx, err := c.db.DB.Begin()
	if err != nil {
		return nil, err
	}

	return &Chain{
		db:             c.db,
		factory:        c.factory,
		tx:             tx,
		originalChain:  c,
		isolationLevel: c.isolationLevel,
		inTransaction:  true,
	}, nil
}

// Commit commits the current transaction
func (c *Chain) Commit() error {
	if !c.inTransaction || c.tx == nil {
		return errors.New("not in transaction")
	}
	return c.tx.Commit()
}

// Rollback rolls back the current transaction
func (c *Chain) Rollback() error {
	if !c.inTransaction || c.tx == nil {
		return errors.New("not in transaction")
	}
	return c.tx.Rollback()
}

// Count returns the count of records matching the current conditions
func (c *Chain) Count() (int64, error) {
	return c.Count2("*")
}

// Count2 returns the count of records for a specific field
func (c *Chain) Count2(field string) (int64, error) {
	countChain := &Chain{
		db:        c.db,
		factory:   c.factory,
		tx:        c.tx,
		tableName: c.tableName,
		conds:     c.conds,
		fieldList: []string{fmt.Sprintf("COUNT(%s) as count", field)},
	}

	result := countChain.list()
	if result.err != nil {
		return 0, result.err
	}

	if len(result.Data) == 0 {
		return 0, nil
	}

	count, ok := result.Data[0]["count"]
	if !ok {
		return 0, errors.New("count field not found in result")
	}

	switch v := count.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("unexpected count type: %T", v)
	}
}

// GroupBy adds GROUP BY clause to the query
func (c *Chain) GroupBy(fields ...string) *Chain {
	if len(fields) > 0 {
		groupByClause := fmt.Sprintf("GROUP BY %s", strings.Join(fields, ", "))
		c.fieldList = append(c.fieldList, groupByClause)
	}
	return c
}

// Having adds HAVING clause to the query
func (c *Chain) Having(condition interface{}, args ...interface{}) *Chain {
	var havingStr string
	switch v := condition.(type) {
	case string:
		havingStr = fmt.Sprintf("HAVING %s", v)
	case *define.Condition:
		havingStr = fmt.Sprintf("HAVING %v", v)
	default:
		return c
	}

	havingCond := define.NewCondition(havingStr, define.OpCustom, args)
	c.conds = append(c.conds, havingCond)
	return c
}

// Transaction executes a function within a transaction
func (c *Chain) Transaction(fn func(tx *Chain) error) error {
	if c.IsInTransaction() {
		// If already in a transaction, create a savepoint
		savepointName := fmt.Sprintf("sp_%d", time.Now().UnixNano())
		if err := c.Savepoint(savepointName); err != nil {
			return err
		}

		// Create a new chain with the same transaction
		txChain := &Chain{
			db:            c.db,
			factory:       c.factory,
			tx:            c.tx,
			inTransaction: true,
		}

		err := fn(txChain)
		if err != nil {
			// Rollback to savepoint if there's an error
			if rbErr := c.RollbackTo(savepointName); rbErr != nil {
				return fmt.Errorf("error rolling back to savepoint: %v (original error: %v)", rbErr, err)
			}
			return err
		}

		// Release savepoint if successful
		if err := c.ReleaseSavepoint(savepointName); err != nil {
			return err
		}
		return nil
	}

	// Start a new transaction if not already in one
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	// Create a new chain with the transaction
	txChain := &Chain{
		db:            c.db,
		factory:       c.factory,
		tx:            tx,
		inTransaction: true,
	}

	err = fn(txChain)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("error rolling back: %v (original error: %v)", rbErr, err)
		}
		return err
	}

	return tx.Commit()
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
		cond.JoinType = define.JoinAnd
		c.conds = append(c.conds, cond)
	}
	return c
}

// OrCond adds a condition with OR join type
func (c *Chain) OrCond(cond *define.Condition) *Chain {
	if cond != nil {
		cond.JoinType = define.JoinOr
		c.conds = append(c.conds, cond)
	}
	return c
}

// NewCondition creates a new condition with AND join type
func (c *Chain) NewCondition() *define.Condition {
	return &define.Condition{JoinType: define.JoinAnd}
}

// WhereGroup starts a new condition group with AND join type
func (c *Chain) WhereGroup() *define.Condition {
	cond := &define.Condition{JoinType: define.JoinAnd, IsSubGroup: true}
	c.conds = append(c.conds, cond)
	return cond
}

// OrWhereGroup starts a new condition group with OR join type
func (c *Chain) OrWhereGroup() *define.Condition {
	cond := &define.Condition{JoinType: define.JoinOr, IsSubGroup: true}
	c.conds = append(c.conds, cond)
	return cond
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
		db:              c.db,
		factory:         c.factory,
		tx:              c.tx,
		tableName:       c.tableName,
		conds:           c.conds,
		fieldList:       c.fieldList,
		orderByExprs:    c.orderByExprs,
		limitCount:      c.limitCount,
		offsetCount:     c.offsetCount,
		fieldMap:        c.fieldMap,
		fieldOrder:      c.fieldOrder,
		batchValues:     c.batchValues,
		isolationLevel:  c.isolationLevel,
		sensitiveFields: c.sensitiveFields,
		ctx:             c.ctx,
	}
}

// Begin starts a new transaction
func (db *DB) Begin() (*sql.Tx, error) {
	return db.DB.Begin()
}

// BeginChain starts a new transaction and returns a Chain
func (db *DB) BeginChain() (*Chain, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}

	return &Chain{
		db:            db,
		factory:       db.Factory,
		tx:            tx,
		inTransaction: true,
	}, nil
}

// BatchUpdate performs batch update operation with the given batch size
func (c *Chain) BatchUpdate(batchSize int) (int64, error) {
	if batchSize <= 0 {
		return 0, &DBError{
			Op:  "BatchUpdate",
			Err: fmt.Errorf("invalid batch size (must be greater than 0, got %d)", batchSize),
		}
	}

	if len(c.batchValues) == 0 {
		return 0, &DBError{
			Op:  "BatchUpdate",
			Err: errors.New("no values to update"),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	progress := newProgressTracker(int64(len(c.batchValues)))

	processBatch := func(ctx context.Context, batch []map[string]interface{}) error {
		tx, err := c.db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}
		defer tx.Rollback()

		txChain := c.clone()
		txChain.tx = tx

		for _, item := range batch {
			// Extract primary key for update condition
			var pkField, pkValue = "", interface{}(nil)
			for k, v := range item {
				if strings.HasSuffix(strings.ToLower(k), "id") {
					pkField = k
					pkValue = v
					break
				}
			}

			if pkField == "" {
				return fmt.Errorf("primary key field not found in update data")
			}

			// Create update chain
			updateChain := txChain.clone()
			updateChain.Where(pkField, define.OpEq, pkValue)
			updateChain.fieldMap = item

			result := updateChain.executeUpdate()
			if result.Error != nil {
				return result.Error
			}

			progress.increment(result.Affected)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		return nil
	}

	err := processBatchesWithTimeout(
		ctx,
		c.batchValues,
		batchSize,
		4, // Use 4 concurrent goroutines
		30*time.Second,
		processBatch,
	)

	if err != nil {
		return 0, &DBError{
			Op:  "BatchUpdate",
			Err: err,
		}
	}

	processed, _ := progress.getProgress()
	return processed, nil
}

// BatchDelete performs batch delete operation with the given batch size
func (c *Chain) BatchDelete(batchSize int) (int64, error) {
	if batchSize <= 0 {
		return 0, &DBError{
			Op:  "BatchDelete",
			Err: fmt.Errorf("invalid batch size (must be greater than 0, got %d)", batchSize),
		}
	}

	// If batchValues is empty but we have conditions, use conditions for delete
	if len(c.batchValues) == 0 && len(c.conds) > 0 {
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
			return 0, &DBError{
				Op:  "BatchDelete",
				Err: err,
			}
		}

		affected, err := sqlResult.RowsAffected()
		if err != nil {
			return 0, &DBError{
				Op:  "BatchDelete",
				Err: err,
			}
		}

		return affected, nil
	}

	if len(c.batchValues) == 0 {
		return 0, &DBError{
			Op:  "BatchDelete",
			Err: errors.New("no values to delete"),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	progress := newProgressTracker(int64(len(c.batchValues)))

	processBatch := func(ctx context.Context, batch []map[string]interface{}) error {
		tx, err := c.db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}
		defer tx.Rollback()

		txChain := c.clone()
		txChain.tx = tx

		for _, item := range batch {
			// Extract primary key for delete condition
			var pkField, pkValue = "", interface{}(nil)
			for k, v := range item {
				if strings.HasSuffix(strings.ToLower(k), "id") {
					pkField = k
					pkValue = v
					break
				}
			}

			if pkField == "" {
				return fmt.Errorf("primary key field not found in delete data")
			}

			// Create delete chain
			deleteChain := txChain.clone()
			deleteChain.Where(pkField, define.OpEq, pkValue)

			sql, args := deleteChain.factory.BuildDelete(deleteChain.tableName, deleteChain.conds)
			if define.Debug {
				log.Printf("[SQL] %s %v", sql, args)
			}

			result, err := tx.Exec(sql, args...)
			if err != nil {
				return err
			}

			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}

			progress.increment(affected)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		return nil
	}

	err := processBatchesWithTimeout(
		ctx,
		c.batchValues,
		batchSize,
		4, // Use 4 concurrent goroutines
		30*time.Second,
		processBatch,
	)

	if err != nil {
		return 0, &DBError{
			Op:  "BatchDelete",
			Err: err,
		}
	}

	processed, _ := progress.getProgress()
	return processed, nil
}

// TransactionOptions represents options for transaction
type TransactionOptions struct {
	Timeout         time.Duration
	IsolationLevel  sql.IsolationLevel
	PropagationMode TransactionPropagation
	ReadOnly        bool
}

// TransactionPropagation defines transaction propagation behavior
type TransactionPropagation int

const (
	// PropagationRequired starts a new transaction if none exists
	PropagationRequired TransactionPropagation = iota
	// PropagationRequiresNew always starts a new transaction
	PropagationRequiresNew
	// PropagationNested starts a nested transaction if possible
	PropagationNested
	// PropagationSupports uses existing transaction if available
	PropagationSupports
	// PropagationNotSupported suspends current transaction if exists
	PropagationNotSupported
	// PropagationNever throws exception if transaction exists
	PropagationNever
	// PropagationMandatory throws exception if no transaction exists
	PropagationMandatory
)

// TransactionWithOptions starts a new transaction with options
func (c *Chain) TransactionWithOptions(opts define.TransactionOptions, fn func(tx *Chain) error) error {
	// Handle propagation behavior
	switch opts.PropagationMode {
	case define.PropagationRequired:
		if c.inTransaction {
			return fn(c)
		}
	case define.PropagationRequiresNew:
		// Always start new transaction, but first suspend current transaction if it exists
		if c.inTransaction {
			// Create a new chain without transaction
			newChain := c.clone()
			newChain.tx = nil
			newChain.inTransaction = false
			return newChain.TransactionWithOptions(opts, fn)
		}
	case define.PropagationNever:
		if c.inTransaction || c.tx != nil {
			return &DBError{
				Op:  "TransactionWithOptions",
				Err: errors.New("transaction already exists for propagation never"),
			}
		}
		return fn(c)
	case define.PropagationNested:
		if c.inTransaction {
			// Create savepoint
			savepointName := fmt.Sprintf("sp_%d", time.Now().UnixNano())
			if err := c.Savepoint(savepointName); err != nil {
				return err
			}
			defer c.ReleaseSavepoint(savepointName)

			err := fn(c)
			if err != nil {
				if rbErr := c.RollbackTo(savepointName); rbErr != nil {
					return fmt.Errorf("error rolling back to savepoint: %v (original error: %v)", rbErr, err)
				}
				return err
			}
			return nil
		}
	case define.PropagationSupports:
		if c.inTransaction {
			return fn(c)
		}
		return fn(c)
	case define.PropagationNotSupported:
		if c.inTransaction {
			// Execute non-transactionally
			newChain := c.clone()
			newChain.tx = nil
			newChain.inTransaction = false
			return fn(newChain)
		}
		return fn(c)
	case define.PropagationMandatory:
		if !c.inTransaction {
			return &DBError{
				Op:  "TransactionWithOptions",
				Err: errors.New("no existing transaction for propagation mandatory"),
			}
		}
		return fn(c)
	}

	// Start new transaction with timeout
	ctx := context.Background()
	var cancel context.CancelFunc
	if opts.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	// Begin transaction
	tx, err := c.db.DB.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(opts.IsolationLevel),
		ReadOnly:  opts.ReadOnly,
	})
	if err != nil {
		return &DBError{
			Op:  "TransactionWithOptions",
			Err: fmt.Errorf("failed to begin transaction: %w", err),
		}
	}

	newChain := &Chain{
		db:             c.db,
		factory:        c.factory,
		tx:             tx,
		originalChain:  c,
		isolationLevel: sql.IsolationLevel(opts.IsolationLevel),
		inTransaction:  true,
		ctx:            ctx,
	}

	err = fn(newChain)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("error rolling back: %v (original error: %v)", rbErr, err)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return &DBError{
			Op:  "TransactionWithOptions",
			Err: fmt.Errorf("failed to commit transaction: %w", err),
		}
	}

	return nil
}

// AddSensitiveField adds a sensitive field to the chain
func (c *Chain) AddSensitiveField(field string, options SensitiveOptions) *Chain {
	if c.sensitiveFields == nil {
		c.sensitiveFields = make(map[string]SensitiveOptions)
	}
	c.sensitiveFields[field] = options
	return c
}

// maskValue masks a sensitive value based on its type
func maskValue(value string, typ SensitiveType) string {
	switch typ {
	case SensitivePhone:
		if len(value) > 7 {
			return value[:3] + "****" + value[len(value)-4:]
		}
	case SensitiveEmail:
		parts := strings.Split(value, "@")
		if len(parts) == 2 {
			username := parts[0]
			if len(username) > 2 {
				return username[:2] + "****" + "@" + parts[1]
			}
		}
	case SensitiveIDCard:
		if len(value) > 10 {
			return value[:6] + "********" + value[len(value)-4:]
		}
	case SensitiveBankCard:
		if len(value) > 8 {
			return value[:4] + "********" + value[len(value)-4:]
		}
	case SensitiveAddress:
		parts := strings.Split(value, " ")
		if len(parts) > 2 {
			return parts[0] + " ****"
		}
	}
	return value
}

// processSensitiveData processes sensitive data before saving
func (c *Chain) processSensitiveData(data map[string]interface{}) error {
	for field, value := range data {
		if options, ok := c.sensitiveFields[field]; ok {
			strValue, ok := value.(string)
			if !ok {
				continue
			}

			switch options.Type {
			case SensitiveEncrypted:
				if len(options.Encryption.KeySourceConfig["key_name"]) == 0 {
					return fmt.Errorf("encryption key not provided for field %s", field)
				}
				encrypted, err := encryptValue(strValue, options.Encryption)
				if err != nil {
					return fmt.Errorf("failed to encrypt field %s: %v", field, err)
				}
				data[field] = encrypted
			default:
				data[field] = maskValue(strValue, options.Type)
			}
		}
	}
	return nil
}

// processSensitiveResults processes sensitive data after querying
func (c *Chain) processSensitiveResults(results []map[string]interface{}) error {
	for _, row := range results {
		for field, value := range row {
			if options, ok := c.sensitiveFields[field]; ok {
				strValue, ok := value.(string)
				if !ok {
					continue
				}

				if options.Type == SensitiveEncrypted {
					if len(options.Encryption.KeySourceConfig["key_name"]) == 0 {
						return fmt.Errorf("encryption key not provided for field %s", field)
					}
					decrypted, err := decryptValue(strValue, options.Encryption)
					if err != nil {
						return fmt.Errorf("failed to decrypt field %s: %v", field, err)
					}
					row[field] = decrypted
				}
			}
		}
	}
	return nil
}

// encryptValue encrypts a value using the specified configuration
func encryptValue(value string, config *EncryptionConfig) (string, error) {
	if config == nil {
		return "", newDBError(ErrConfiguration, "encryptValue", nil, "encryption configuration is required")
	}

	key, err := getEncryptionKey(config)
	if err != nil {
		return "", newDBError(ErrConfiguration, "encryptValue", err, "failed to get encryption key")
	}

	var block cipher.Block
	switch config.Algorithm {
	case AES256:
		if len(key) != 32 {
			return "", newDBError(ErrConfiguration, "encryptValue", nil, "AES-256 requires a 32-byte key")
		}
		block, err = aes.NewCipher(key)
	case AES192:
		if len(key) != 24 {
			return "", newDBError(ErrConfiguration, "encryptValue", nil, "AES-192 requires a 24-byte key")
		}
		block, err = aes.NewCipher(key)
	case AES128:
		if len(key) != 16 {
			return "", newDBError(ErrConfiguration, "encryptValue", nil, "AES-128 requires a 16-byte key")
		}
		block, err = aes.NewCipher(key)
	default:
		return "", newDBError(ErrConfiguration, "encryptValue", nil, "unsupported encryption algorithm")
	}

	if err != nil {
		return "", newDBError(ErrConfiguration, "encryptValue", err, "failed to create cipher")
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", newDBError(ErrConfiguration, "encryptValue", err, "failed to create GCM")
	}

	// Create nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", newDBError(ErrConfiguration, "encryptValue", err, "failed to generate nonce")
	}

	// Encrypt and combine nonce with ciphertext
	ciphertext := gcm.Seal(nonce, nonce, []byte(value), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptValue decrypts a value using the specified configuration
func decryptValue(encryptedValue string, config *EncryptionConfig) (string, error) {
	if config == nil {
		return "", newDBError(ErrConfiguration, "decryptValue", nil, "encryption configuration is required")
	}

	key, err := getEncryptionKey(config)
	if err != nil {
		return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to get encryption key")
	}

	var block cipher.Block
	switch config.Algorithm {
	case AES256, AES192, AES128:
		block, err = aes.NewCipher(key)
	default:
		return "", newDBError(ErrConfiguration, "decryptValue", nil, "unsupported encryption algorithm")
	}

	if err != nil {
		return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to create cipher")
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to create GCM")
	}

	// Decode base64
	ciphertext, err := base64.StdEncoding.DecodeString(encryptedValue)
	if err != nil {
		return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to decode base64")
	}

	// Extract nonce and decrypt
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", newDBError(ErrConfiguration, "decryptValue", nil, "ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to decrypt")
	}

	return string(plaintext), nil
}

// getEncryptionKey retrieves the encryption key from the configured source
func getEncryptionKey(config *EncryptionConfig) ([]byte, error) {
	switch config.KeySource {
	case KeySourceEnv:
		keyName := config.KeySourceConfig["key_name"]
		if keyName == "" {
			return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "key_name not specified for env source")
		}
		key := os.Getenv(keyName)
		if key == "" {
			return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "encryption key not found in environment")
		}
		return base64.StdEncoding.DecodeString(key)

	case KeySourceFile:
		keyPath := config.KeySourceConfig["key_path"]
		if keyPath == "" {
			return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "key_path not specified for file source")
		}
		return os.ReadFile(keyPath)

	case KeySourceVault:
		// Implementation for key vault would go here
		return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "vault key source not implemented")

	default:
		return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "unsupported key source")
	}
}

// GetLastQueryStats returns the statistics of the last executed query
func (c *Chain) GetLastQueryStats() *QueryStats {
	return c.queryStats
}

// startQueryStats begins tracking query execution
func (c *Chain) startQueryStats(sql string, args []interface{}) {
	c.queryStats = &QueryStats{
		SQL:       sql,
		Args:      args,
		StartTime: time.Now(),
	}
}

// endQueryStats finalizes the tracking of query execution
func (c *Chain) endQueryStats(rowsAffected int64) {
	if c.queryStats != nil {
		c.queryStats.Duration = time.Since(c.queryStats.StartTime)
		c.queryStats.RowsAffected = rowsAffected
		if define.Debug {
			log.Printf("[QueryStats] SQL: %s, Duration: %v, RowsAffected: %d",
				c.queryStats.SQL, c.queryStats.Duration, c.queryStats.RowsAffected)
		}
	}
}

// BeginNested starts a new nested transaction
func (c *Chain) BeginNested() (*Chain, error) {
	if c.txStack == nil {
		c.txStack = &txStack{
			savepoints: make([]string, 0),
			level:      0,
		}
	}

	// If not in a transaction, start a new one
	if !c.inTransaction {
		chain, err := c.Begin()
		if err != nil {
			return nil, newDBError(ErrTransaction, "BeginNested", err, "failed to start initial transaction")
		}
		return chain, nil
	}

	// Create a new savepoint
	c.txStack.level++
	savepointName := fmt.Sprintf("sp_%d", c.txStack.level)
	err := c.Savepoint(savepointName)
	if err != nil {
		c.txStack.level--
		return nil, newDBError(ErrTransaction, "BeginNested", err, "failed to create savepoint")
	}

	c.txStack.savepoints = append(c.txStack.savepoints, savepointName)

	// Create a new chain for this nested transaction
	newChain := c.clone()
	newChain.txStack = c.txStack
	return newChain, nil
}

// CommitNested commits the current nested transaction
func (c *Chain) CommitNested() error {
	if c.txStack == nil || len(c.txStack.savepoints) == 0 {
		return c.Commit()
	}

	// Release the current savepoint
	savepointName := c.txStack.savepoints[len(c.txStack.savepoints)-1]
	err := c.ReleaseSavepoint(savepointName)
	if err != nil {
		return newDBError(ErrTransaction, "CommitNested", err, "failed to release savepoint")
	}

	// Pop the savepoint
	c.txStack.savepoints = c.txStack.savepoints[:len(c.txStack.savepoints)-1]
	c.txStack.level--

	// If this was the last nested transaction, commit the main transaction
	if len(c.txStack.savepoints) == 0 {
		return c.Commit()
	}

	return nil
}

// RollbackNested rolls back to the last savepoint
func (c *Chain) RollbackNested() error {
	if c.txStack == nil || len(c.txStack.savepoints) == 0 {
		return c.Rollback()
	}

	// Rollback to the current savepoint
	savepointName := c.txStack.savepoints[len(c.txStack.savepoints)-1]
	err := c.RollbackTo(savepointName)
	if err != nil {
		return newDBError(ErrTransaction, "RollbackNested", err, "failed to rollback to savepoint")
	}

	// Pop the savepoint
	c.txStack.savepoints = c.txStack.savepoints[:len(c.txStack.savepoints)-1]
	c.txStack.level--

	// If this was the last nested transaction, rollback the main transaction
	if len(c.txStack.savepoints) == 0 {
		return c.Rollback()
	}

	return nil
}

// GetTransactionLevel returns the current transaction nesting level
func (c *Chain) GetTransactionLevel() int {
	if c.txStack == nil {
		return 0
	}
	return c.txStack.level
}

// And adds a condition with AND join
func (c *Chain) And(field string, op define.OpType, value interface{}) *Chain {
	cond := &define.Condition{
		Field:    field,
		Op:       op,
		Value:    value,
		JoinType: define.JoinAnd,
	}
	c.conds = append(c.conds, cond)
	return c
}

// Or adds a condition with OR join
func (c *Chain) Or(field string, op define.OpType, value interface{}) *Chain {
	cond := &define.Condition{
		Field:    field,
		Op:       op,
		Value:    value,
		JoinType: define.JoinOr,
	}
	c.conds = append(c.conds, cond)
	return c
}

// AndGroup adds a group of conditions with AND join
func (c *Chain) AndGroup(conditions []*define.Condition) *Chain {
	if len(conditions) == 0 {
		return c
	}
	cond := &define.Condition{
		IsSubGroup: true,
		SubConds:   conditions,
		JoinType:   define.JoinAnd,
	}
	c.conds = append(c.conds, cond)
	return c
}

// OrGroup adds a group of conditions with OR join
func (c *Chain) OrGroup(conditions []*define.Condition) *Chain {
	if len(conditions) == 0 {
		return c
	}
	cond := &define.Condition{
		IsSubGroup: true,
		SubConds:   conditions,
		JoinType:   define.JoinOr,
	}
	c.conds = append(c.conds, cond)
	return c
}

// OrWhere adds a condition with OR join
func (c *Chain) OrWhere(field string, op define.OpType, value interface{}) *Chain {
	cond := &define.Condition{
		Field:    field,
		Op:       op,
		Value:    value,
		JoinType: define.JoinOr,
	}
	c.conds = append(c.conds, cond)
	return c
}
