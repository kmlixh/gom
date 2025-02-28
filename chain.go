package gom

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/security"
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
	sensitiveFields  map[string]SensitiveOptions
	encryptionConfig *EncryptionConfig

	// Context for transaction
	ctx context.Context

	// Query performance tracking
	queryStats *QueryStats

	// Transaction management
	txStack *txStack

	// Raw SQL fields
	rawSQL string
	args   []interface{}
	err    error
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
	// SensitiveMasked for masked data
	SensitiveMasked
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
	Encryption *security.EncryptionConfig
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

// Where adds a WHERE condition to the chain
func (c *Chain) Where(field string, op define.OpType, value interface{}) *Chain {
	if value == nil && op != define.OpIsNull && op != define.OpIsNotNull {
		c.err = fmt.Errorf("invalid condition: nil value not allowed")
		return c
	}
	if op > define.OpCustom {
		c.err = fmt.Errorf("invalid operator")
		return c
	}
	cond := &define.Condition{
		Field:    field,
		Op:       op,
		Value:    value,
		JoinType: define.JoinAnd,
	}
	c.conds = append(c.conds, cond)
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

// From sets the model for the chain operation
func (c *Chain) From(model interface{}) *Chain {
	// Store the model for potential ID callback
	c.model = model

	// Handle string type parameter
	if tableName, ok := model.(string); ok {
		c.tableName = tableName
		return c
	}
	if c.tableName == "" && model.(define.ITableModel).TableName() != "" {
		c.tableName = model.(define.ITableModel).TableName()

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

	// If no table name is set, use the model type name
	if c.tableName == "" {
		c.tableName = strings.ToLower(modelType.Name())
	}

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

		// Skip auto-increment fields for insert
		isAuto := false
		for _, part := range parts[1:] {
			if part == "auto" || part == "@" {
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
					// Set default value for time fields
					if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
						fieldValue.Set(reflect.ValueOf(time.Now()))
					} else if fieldValue.Type().Kind() == reflect.Bool {
						fieldValue.SetBool(true)
					}
					break
				}
			}
			if hasDefault {
				if c.fieldMap == nil {
					c.fieldMap = make(map[string]interface{})
				}
				c.fieldMap[columnName] = fieldValue.Interface()
				if !contains(c.fieldOrder, columnName) {
					c.fieldOrder = append(c.fieldOrder, columnName)
				}
				continue
			}
		}

		// Add field to fieldMap
		if c.fieldMap == nil {
			c.fieldMap = make(map[string]interface{})
		}
		c.fieldMap[columnName] = fieldValue.Interface()

		// Add to fieldOrder if not already present
		if !contains(c.fieldOrder, columnName) {
			c.fieldOrder = append(c.fieldOrder, columnName)
		}
	}

	return c
}

// List executes a SELECT query and returns all results
func (c *Chain) List(dest ...interface{}) *define.Result {
	result := c.list()
	if result.Error == nil && len(result.Data) > 0 {
		if err := c.processSensitiveResults(result.Data); err != nil {
			result.Error = err
			return result
		}
	}
	if len(dest) > 0 && result.Error == nil {
		result.Error = result.Into(dest[0])
	}
	return result
}

// list is the internal implementation of List
func (c *Chain) list() *define.Result {
	if c.err != nil {
		return &define.Result{Error: c.err}
	}

	sqlProto := c.BuildSelect()
	if sqlProto.Error != nil {
		return &define.Result{Error: sqlProto.Error}
	}

	return c.executeSqlProto(sqlProto)
}

// createScanner creates an appropriate scanner for the given column type
func createScanner(ct *sql.ColumnType) interface{} {
	if ct == nil {
		return new(interface{})
	}

	scanType := ct.ScanType()
	if scanType == nil {
		// 如果ScanType为nil，使用默认的string类型
		return new(sql.NullString)
	}

	// 创建scanType类型的指针
	return reflect.New(scanType).Interface()
}

// extractValue extracts the actual value from a scanner
func extractValue(scanner interface{}, ct *sql.ColumnType) interface{} {
	if scanner == nil {
		return nil
	}

	// 获取scanner的反射值
	value := reflect.ValueOf(scanner).Elem()

	// 处理sql.Null*类型
	if value.Type().Implements(reflect.TypeOf((*sql.Scanner)(nil)).Elem()) {
		// 检查是否为sql.Null*类型
		if validField := value.FieldByName("Valid"); validField.IsValid() {
			if !validField.Bool() {
				return nil
			}
			// 返回实际值
			return value.FieldByName("Value").Interface()
		}
	}

	// 处理[]byte类型的特殊情况
	if value.Type() == reflect.TypeOf([]byte{}) {
		bytes := value.Bytes()
		if bytes == nil {
			return nil
		}
		// 处理JSON类型
		if strings.ToLower(ct.DatabaseTypeName()) == "json" || strings.ToLower(ct.DatabaseTypeName()) == "jsonb" {
			var js interface{}
			if err := json.Unmarshal(bytes, &js); err == nil {
				return js
			}
		}
		return string(bytes)
	}

	// 返回普通类型的值
	return value.Interface()
}

// First returns the first result
func (c *Chain) First(dest ...interface{}) *define.Result {
	c.Limit(1)
	result := c.list()
	if result.Error != nil {
		return result
	}
	if len(result.Data) == 0 {
		return &define.Result{Error: sql.ErrNoRows}
	}
	if len(dest) > 0 {
		result.Error = result.Into(dest[0])
	}
	return result
}

// One returns exactly one result
func (c *Chain) One(dest ...interface{}) *define.Result {
	result := c.list()
	if result.Size() != 1 {
		result.Error = fmt.Errorf("expected 1 result, got %d", result.Size())
		return result
	}
	if len(dest) > 0 && result.Error == nil {
		result.Error = result.Into(dest[0])
	}
	return result
}

// Save saves (inserts or updates) records with the given fields or model
func (c *Chain) Save(fieldsOrModel ...interface{}) *define.Result {
	switch {
	case len(fieldsOrModel) == 0:
		return c.saveFromFieldMap()
	case fieldsOrModel[0] == nil:
		return &define.Result{Error: fmt.Errorf("nil pointer")}
	default:
		modelValue := reflect.ValueOf(fieldsOrModel[0])
		if modelValue.Kind() != reflect.Ptr {
			return &define.Result{Error: fmt.Errorf("non-pointer")}
		}
		if modelValue.IsNil() {
			return &define.Result{Error: fmt.Errorf("nil pointer")}
		}
		return c.saveModel(fieldsOrModel[0])
	}
}

// 核心保存逻辑
func (c *Chain) saveModel(model interface{}) *define.Result {
	transfer, err := c.validateModel(model)
	if err != nil {
		return errorResult(err)
	}

	fields, err := c.prepareModelFields(transfer, model)
	if err != nil {
		return errorResult(err)
	}

	return c.determineModelOperation(transfer, model, fields)
}

// 模型验证
func (c *Chain) validateModel(model interface{}) (*define.Transfer, error) {
	if transfer := define.GetTransfer(model); transfer != nil {
		if transfer.PrimaryKey == nil {
			return nil, errors.New("model missing primary key")
		}
		c.initTableName(transfer)
		return transfer, nil
	}
	return nil, fmt.Errorf("unsupported model type: %T", model)
}

// 准备模型字段
func (c *Chain) prepareModelFields(transfer *define.Transfer, model interface{}) (map[string]interface{}, error) {
	fields := transfer.ToMap(model)
	if len(fields) == 0 {
		return nil, errors.New("no fields to save")
	}

	c.processEncryption(transfer, fields)
	c.convertBoolValues(fields)
	return fields, nil
}

// 决策操作类型
func (c *Chain) determineModelOperation(transfer *define.Transfer, model interface{}, fields map[string]interface{}) *define.Result {
	pk := transfer.PrimaryKey
	idValue, isAutoInc := c.getPrimaryKeyInfo(model, pk)

	if shouldUpdate(idValue, isAutoInc) {
		return c.buildUpdateOperation(pk.Column, idValue, fields)
	}
	return c.buildInsertOperation(pk, fields, model)
}

// 获取主键信息
func (c *Chain) getPrimaryKeyInfo(model interface{}, pk *define.FieldInfo) (interface{}, bool) {
	modelValue := reflect.ValueOf(model).Elem()
	idField := modelValue.FieldByName(pk.Name)
	return idField.Interface(), c.isAutoIncrement(pk)
}

// 构建更新操作
func (c *Chain) buildUpdateOperation(column string, idValue interface{}, fields map[string]interface{}) *define.Result {
	c.Where(column, define.OpEq, idValue)
	return c.Sets(fields).executeUpdate()
}

// 构建插入操作
func (c *Chain) buildInsertOperation(pk *define.FieldInfo, fields map[string]interface{}, model interface{}) *define.Result {
	if pk.IsAuto {
		delete(fields, pk.Column)
	}

	result := c.Sets(fields).executeInsert()
	if result.Error == nil && pk.IsAuto {
		c.setModelID(model, pk.Name, result.ID)
	}
	return result
}

// 工具函数
func (c *Chain) initTableName(transfer *define.Transfer) {
	if c.tableName == "" {
		c.tableName = transfer.GetTableName()
	}
}

func (c *Chain) isAutoIncrement(pk *define.FieldInfo) bool {
	return pk.IsAuto
}

func shouldUpdate(idValue interface{}, isAutoInc bool) bool {
	return !isZeroValue(idValue) && !isAutoInc
}

func isZeroValue(value interface{}) bool {
	return reflect.DeepEqual(value, reflect.Zero(reflect.TypeOf(value)).Interface())
}

func errorResult(msg interface{}) *define.Result {
	return &define.Result{Error: fmt.Errorf("%v", msg)}
}

// executeInsert executes an INSERT query
func (c *Chain) executeInsert() *define.Result {
	if len(c.fieldMap) == 0 {
		return &define.Result{Error: fmt.Errorf("no fields to insert")}
	}

	if c.factory == nil {
		return &define.Result{Error: fmt.Errorf("SQL factory is not initialized")}
	}

	if c.db == nil {
		return &define.Result{Error: fmt.Errorf("database connection is not initialized")}
	}

	// 处理敏感字段
	if len(c.sensitiveFields) > 0 {
		if err := c.processSensitiveData(c.fieldMap); err != nil {
			return &define.Result{Error: err}
		}
	}

	// 生成 SQL 和参数
	sqlProto := c.factory.BuildInsert(c.tableName, c.fieldMap, c.fieldOrder)

	return c.executeSqlProto(sqlProto)
}

// deleteSingleModel deletes a single model without transaction
func (c *Chain) deleteSingleModel(model interface{}) *define.Result {
	// Try to get transfer for struct-based model
	if transfer := define.GetTransfer(model); transfer != nil {
		// Set table name if not set
		if c.tableName == "" {
			c.tableName = transfer.GetTableName()
		}

		// Extract ID for condition if no conditions are set
		if len(c.conds) == 0 {
			if pkValue, _ := transfer.GetPrimaryKeyValue(model); pkValue != nil {
				c.Where(transfer.PrimaryKey.Column, define.OpEq, pkValue)
			} else {
				// If no primary key, try to use other non-zero fields as conditions
				fields := transfer.ToMap(model)
				if len(fields) == 0 {
					return &define.Result{Error: fmt.Errorf("no fields available for delete condition")}
				}
				for field, value := range fields {
					if !reflect.ValueOf(value).IsZero() {
						c.Where(field, define.OpEq, value)
					}
				}
				if len(c.conds) == 0 {
					return &define.Result{Error: fmt.Errorf("delete without conditions is not allowed")}
				}
			}
		}

		return c.Delete()
	}

	// Handle map-based model
	if fields, ok := model.(map[string]interface{}); ok {
		// Extract primary key for delete condition
		var pkField, pkValue = "", interface{}(nil)
		for k, v := range fields {
			if strings.HasSuffix(strings.ToLower(k), "id") {
				pkField = k
				pkValue = v
				break
			}
		}

		if len(c.conds) == 0 {
			if pkField != "" && pkValue != nil {
				c.Where(pkField, define.OpEq, pkValue)
			} else {
				// If no primary key, use other non-zero fields as conditions
				for field, value := range fields {
					if !reflect.ValueOf(value).IsZero() {
						c.Where(field, define.OpEq, value)
					}
				}
				if len(c.conds) == 0 {
					return &define.Result{Error: fmt.Errorf("delete without conditions is not allowed")}
				}
			}
		}

		return c.Delete()
	}

	return &define.Result{Error: fmt.Errorf("unsupported model type: %T", model)}
}

// deleteMultipleModels deletes multiple models within a transaction
func (c *Chain) deleteMultipleModels(models []interface{}) *define.Result {
	// If already in transaction, use existing transaction
	if c.tx != nil {
		return c.executeMultipleDeletes(models)
	}

	// Start new transaction
	tx, err := c.db.DB.Begin()
	if err != nil {
		return &define.Result{Error: fmt.Errorf("failed to begin transaction: %v", err)}
	}

	// Create new Chain with transaction
	txChain := c.clone()
	txChain.tx = tx

	result := txChain.executeMultipleDeletes(models)
	if result.Error != nil {
		// Rollback on error
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return &define.Result{Error: fmt.Errorf("delete failed: %v, rollback failed: %v", result.Error, rollbackErr)}
		}
		return &define.Result{Error: fmt.Errorf("delete failed: %v", result.Error)}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return &define.Result{Error: fmt.Errorf("failed to commit transaction: %v", err)}
	}

	return result
}

// executeMultipleDeletes executes deletes for multiple models
func (c *Chain) executeMultipleDeletes(models []interface{}) *define.Result {
	var totalAffected int64

	for _, model := range models {
		result := c.clone().deleteSingleModel(model)
		if result.Error != nil {
			return result
		}
		totalAffected += result.Affected
	}

	return &define.Result{Affected: totalAffected}
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
	if result.Error != nil {
		return result.Error
	}
	return result.Into(dest)
}

// RawQuery executes a raw SQL query
func (c *Chain) RawQuery(sqlStr string, args ...interface{}) *define.Result {
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
		return &define.Result{Error: err}
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &define.Result{Error: err}
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		err = rows.Scan(values...)
		if err != nil {
			return &define.Result{Error: err}
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			row[col] = val
		}
		result = append(result, row)
	}

	if err = rows.Err(); err != nil {
		return &define.Result{Error: err}
	}

	return &define.Result{
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
			} else {
				field.SetInt(0) // Set to zero for invalid values
			}
		case []uint8:
			if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				field.SetInt(i)
			} else {
				field.SetInt(0) // Set to zero for invalid values
			}
		default:
			field.SetInt(0) // Set to zero for unsupported types
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
			sqlProto := &define.SqlProto{
				SqlType: define.Exec,
				Sql:     createSql,
				Args:    nil,
				Error:   nil,
			}
			result := c.executeSqlProto(sqlProto)
			if result.Error != nil {
				return result.Error
			}
			return nil
		}
	}

	// Build and execute CREATE TABLE statement using default logic
	sqlProto := c.factory.BuildCreateTable(c.tableName, modelType)
	result := c.executeSqlProto(sqlProto)
	return result.Error
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
	if result.Error != nil {
		return 0, result.Error
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
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, nil
		}
		return 0, fmt.Errorf("failed to convert string count to int64: %s", v)
	case []uint8:
		if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			return i, nil
		}
		return 0, fmt.Errorf("failed to convert []uint8 count to int64: %s", string(v))
	case nil:
		return 0, nil
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
func (c *Chain) PageInfo(models ...interface{}) (*PageInfo, error) {
	if len(models) > 1 {
		return nil, errors.New("only one model can be provided for PageInfo")
	} else if len(models) == 1 {
		c.From(models[0])
	}
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
	if c.model != nil {
		// 如果提供了模型，使用模型类型创建切片
		sliceType := reflect.SliceOf(reflect.TypeOf(c.model))
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
		if result.Error != nil {
			return nil, result.Error
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
func (c *Chain) BatchInsert(batchSize int, enableConcurrent bool) (int64, error) {
	// 参数校验
	if batchSize <= 0 {
		return 0, &define.DBError{
			Op:  "BatchInsert",
			Err: fmt.Errorf("invalid batch size: %d (must be > 0)", batchSize),
		}
	}

	// 数据校验
	if len(c.batchValues) == 0 {
		return 0, &define.DBError{
			Op:  "BatchInsert",
			Err: errors.New("no values to insert"),
		}
	}

	// 并发控制逻辑
	numGoroutines := 1
	if enableConcurrent {
		// 智能计算并发数
		numGoroutines = calculateOptimalGoroutines(len(c.batchValues), batchSize)
		// 限制最大并发数
		numGoroutines = min(numGoroutines, 8)
		// 连接池检查
		if c.db.Stats().OpenConnections < numGoroutines {
			c.db.SetMaxOpenConns(numGoroutines + 2)
		}
	}

	// 超时控制
	ctx := c.ctx
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
	}

	// 批处理逻辑
	var processBatch func(context.Context, []map[string]interface{}) *define.Result
	if enableConcurrent {
		processBatch = c.concurrentProcessBatch
	} else {
		processBatch = c.sequentialProcessBatch
	}

	// 执行批处理
	result := processBatchesWithTimeout(
		ctx,
		c.batchValues,
		batchSize,
		numGoroutines,
		60*time.Second,
		processBatch,
	)

	// 错误处理
	if result.Error != nil {
		if errors.Is(result.Error, context.DeadlineExceeded) {
			return 0, context.DeadlineExceeded
		}
		return 0, &define.DBError{
			Op:  "BatchInsert",
			Err: result.Error,
		}
	}
	if result.Data != nil && len(result.Data) > 0 {
		result.Affected = int64(len(result.Data))
	}
	return result.Affected, nil
}

// 顺序处理批次
func (c *Chain) sequentialProcessBatch(ctx context.Context, batch []map[string]interface{}) *define.Result {
	tx, err := c.db.Begin()
	if err != nil {
		return &define.Result{Error: err}
	}

	sqlProto := c.factory.BuildBatchInsert(c.tableName, batch)
	result := c.WithContext(ctx).executeSqlProto(sqlProto)
	if result.Error != nil {
		tx.Rollback()
		return result
	}
	err = tx.Commit()
	return &define.Result{
		Affected: result.Affected,
		ID:       result.ID,
		Data:     result.Data,
		Error:    err,
	}
}

// 并发处理批次
func (c *Chain) concurrentProcessBatch(ctx context.Context, batch []map[string]interface{}) *define.Result {
	txChain := c.clone()
	tx, err := c.db.Begin()
	if err != nil {
		return &define.Result{Error: err}
	}
	txChain.tx = tx

	sqlProto := txChain.factory.BuildBatchInsert(txChain.tableName, batch)
	result := txChain.WithContext(ctx).executeSqlProto(sqlProto)
	if result.Error != nil {
		tx.Rollback()
		return result
	}
	err = tx.Commit()
	result.Error = err
	return result
}

// 统一上下文获取
func (c *Chain) getContext() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
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
	sqlProto := c.factory.BuildBatchInsert(tableName, values)

	result := c.executeSqlProto(sqlProto)

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

	processBatch := func(ctx context.Context, batch []map[string]interface{}) *define.Result {
		tx, err := c.db.Begin()
		if err != nil {
			return &define.Result{Error: fmt.Errorf("failed to start transaction: %w", err)}
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
				return &define.Result{Error: fmt.Errorf("primary key field not found in update data")}
			}

			// Create update chain
			updateChain := txChain.clone()
			updateChain.Where(pkField, define.OpEq, pkValue)
			updateChain.fieldMap = item

			result := updateChain.executeUpdate()
			if result.Error != nil {
				return result
			}

			progress.increment(result.Affected)
		}

		if err := tx.Commit(); err != nil {
			return &define.Result{Error: fmt.Errorf("failed to commit transaction: %w", err)}
		}

		return &define.Result{Error: nil}
	}

	result := processBatchesWithTimeout(
		ctx,
		c.batchValues,
		batchSize,
		4, // Use 4 concurrent goroutines
		30*time.Second,
		processBatch,
	)

	if result.Error != nil {
		return 0, &DBError{
			Op:  "BatchUpdate",
			Err: result.Error,
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
		sqlProto := c.factory.BuildDelete(c.tableName, c.conds)
		result := c.executeSqlProto(sqlProto)
		affected := result.Affected

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

	processBatch := func(ctx context.Context, batch []map[string]interface{}) *define.Result {
		tx, err := c.db.Begin()
		if err != nil {
			return &define.Result{Error: fmt.Errorf("failed to start transaction: %w", err)}
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
				return &define.Result{Error: fmt.Errorf("primary key field not found in delete data")}
			}

			// Create delete chain
			deleteChain := txChain.clone()
			deleteChain.Where(pkField, define.OpEq, pkValue)

			sqlProto := deleteChain.factory.BuildDelete(deleteChain.tableName, deleteChain.conds)
			result := deleteChain.executeSqlProto(sqlProto)

			affected, err := result.RowsAffected()
			if err != nil {
				return &define.Result{Error: err}
			}

			progress.increment(affected)
		}

		if err := tx.Commit(); err != nil {
			return &define.Result{Error: fmt.Errorf("failed to commit transaction: %w", err)}
		}

		return &define.Result{Error: nil}
	}

	result := processBatchesWithTimeout(
		ctx,
		c.batchValues,
		batchSize,
		4, // Use 4 concurrent goroutines
		30*time.Second,
		processBatch,
	)

	if result.Error != nil {
		return 0, &DBError{
			Op:  "BatchDelete",
			Err: result.Error,
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

			if options.Type == SensitiveEncrypted {
				if options.Encryption == nil {
					return &security.DBError{
						Code:    security.ErrConfiguration,
						Op:      "processSensitiveData",
						Message: fmt.Sprintf("encryption configuration required for field %s", field),
					}
				}

				// 验证加密配置
				if options.Encryption.Algorithm == "" {
					return &security.DBError{
						Code:    security.ErrConfiguration,
						Op:      "processSensitiveData",
						Message: fmt.Sprintf("encryption algorithm is required for field %s", field),
					}
				}

				// 验证加密算法
				switch options.Encryption.Algorithm {
				case security.AES256, security.AES192, security.AES128, security.ChaCha20Poly1305:
					// 有效的算法
				default:
					return &security.DBError{
						Code:    security.ErrConfiguration,
						Op:      "processSensitiveData",
						Message: fmt.Sprintf("invalid algorithm for field %s: %s", field, options.Encryption.Algorithm),
					}
				}

				// 验证密钥源
				if options.Encryption.KeySource == "" {
					return &security.DBError{
						Code:    security.ErrConfiguration,
						Op:      "processSensitiveData",
						Message: fmt.Sprintf("encryption key source is required for field %s", field),
					}
				}

				// 验证密钥源类型
				switch options.Encryption.KeySource {
				case string(KeySourceEnv), string(KeySourceFile), string(KeySourceVault):
					// 有效的密钥源
				default:
					return &security.DBError{
						Code:    security.ErrConfiguration,
						Op:      "processSensitiveData",
						Message: fmt.Sprintf("unsupported key source for field %s: %s", field, options.Encryption.KeySource),
					}
				}

				encrypted, err := security.EncryptValue(strValue, options.Encryption)
				if err != nil {
					if dbErr, ok := err.(*security.DBError); ok {
						return dbErr
					}
					return &security.DBError{
						Code:    security.ErrEncryption,
						Op:      "processSensitiveData",
						Err:     err,
						Message: fmt.Sprintf("failed to encrypt field %s", field),
					}
				}
				data[field] = encrypted
			} else if options.Type == SensitiveMasked {
				data[field] = maskValue(strValue, options.Type)
			}
		}
	}
	return nil
}

// processSensitiveResults processes sensitive data after querying
func (c *Chain) processSensitiveResults(results []map[string]interface{}) error {
	for _, result := range results {
		for field, value := range result {
			if options, ok := c.sensitiveFields[field]; ok {
				strValue, ok := value.(string)
				if !ok {
					continue
				}

				if options.Type == SensitiveEncrypted {
					if options.Encryption == nil {
						return fmt.Errorf("encryption configuration required for field %s", field)
					}
					decrypted, err := security.DecryptValue(strValue, options.Encryption)
					if err != nil {
						return fmt.Errorf("failed to decrypt field %s: %v", field, err)
					}
					result[field] = decrypted
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

// Raw 执行原始 SQL 查询
func (c *Chain) Raw(query string, args ...interface{}) *Chain {
	c.rawSQL = query
	c.args = args
	return c
}

// Exec 执行原始 SQL 语句
func (c *Chain) Exec() *define.Result {
	if c.rawSQL == "" {
		return &define.Result{Error: errors.New("raw SQL is empty")}
	}

	result, err := c.db.DB.Exec(c.rawSQL, c.args...)
	if err != nil {
		return &define.Result{Error: err}
	}

	lastInsertID, _ := result.LastInsertId()
	rowsAffected, _ := result.RowsAffected()

	return &define.Result{
		ID:       lastInsertID,
		Affected: rowsAffected,
	}
}

// Sets allows batch setting of field values
func (c *Chain) Sets(fields map[string]interface{}) *Chain {
	if fields == nil {
		return c
	}
	if c.fieldMap == nil {
		c.fieldMap = make(map[string]interface{})
	}
	for k, v := range fields {
		c.fieldMap[k] = v
	}
	c.fieldOrder = make([]string, 0, len(c.fieldMap))
	for field := range c.fieldMap {
		c.fieldOrder = append(c.fieldOrder, field)
	}
	return c
}

// NewChain creates a new Chain instance with the given database and factory
func NewChain(db *DB, factory define.SQLFactory) *Chain {
	return &Chain{
		db:      db,
		factory: factory,
	}
}

// BuildSelect builds a SELECT query
func (c *Chain) BuildSelect() *define.SqlProto {
	if c.err != nil {
		return &define.SqlProto{Error: c.err}
	}

	if c.tableName == "" {
		return &define.SqlProto{Error: define.ErrEmptyTableName}
	}

	return c.factory.BuildSelect(c.tableName, c.fieldList, c.conds, c.buildOrderBy(), c.limitCount, c.offsetCount)
}

// encryptField encrypts a field value using the configured encryption settings
func (c *Chain) encryptField(value interface{}) (string, error) {
	if c.encryptionConfig == nil {
		return "", fmt.Errorf("encryption configuration is not set")
	}

	// Convert value to string
	var strValue string
	switch v := value.(type) {
	case string:
		strValue = v
	case []byte:
		strValue = string(v)
	default:
		strValue = fmt.Sprintf("%v", v)
	}

	// Get encryption key
	key, err := c.getEncryptionKey()
	if err != nil {
		return "", err
	}

	// Encrypt the value
	encrypted, err := c.encryptAES([]byte(strValue), key)
	if err != nil {
		return "", err
	}

	// Return base64 encoded encrypted value
	return base64.StdEncoding.EncodeToString(encrypted), nil
}

// getEncryptionKey retrieves the encryption key from the configured source
func (c *Chain) getEncryptionKey() ([]byte, error) {
	if c.encryptionConfig == nil {
		return nil, fmt.Errorf("encryption configuration is not set")
	}

	switch c.encryptionConfig.KeySource {
	case KeySourceEnv:
		key := os.Getenv(c.encryptionConfig.KeySourceConfig["key_name"])
		if key == "" {
			return nil, fmt.Errorf("encryption key not found in environment variable: %s", c.encryptionConfig.KeySourceConfig["key_name"])
		}
		return []byte(key), nil
	case KeySourceFile:
		keyPath := c.encryptionConfig.KeySourceConfig["key_path"]
		key, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read encryption key from file: %v", err)
		}
		return key, nil
	default:
		return nil, fmt.Errorf("unsupported key source: %s", c.encryptionConfig.KeySource)
	}
}

// encryptAES encrypts data using AES in GCM mode
func (c *Chain) encryptAES(data, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, data, nil), nil
}

// processEncryptedFields processes encrypted fields in the model
func (c *Chain) processEncryptedFields(model interface{}) error {
	if c.encryptionConfig == nil {
		return nil
	}

	transfer := define.GetTransfer(model)
	if transfer == nil {
		return fmt.Errorf("failed to get transfer for model")
	}

	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
	}

	modelType := modelValue.Type()
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		fieldValue := modelValue.Field(i)

		tag := field.Tag.Get("gom")
		if tag == "" {
			continue
		}

		for _, opt := range strings.Split(tag, ",") {
			if opt == "encrypt" {
				encryptedValue, err := c.encryptField(fieldValue.Interface())
				if err != nil {
					return fmt.Errorf("failed to encrypt field %s: %v", field.Name, err)
				}
				if fieldValue.CanSet() {
					fieldValue.SetString(encryptedValue)
				}
				break
			}
		}
	}

	return nil
}

// Insert inserts a new record into the database
func (c *Chain) Insert(model interface{}) *define.Result {
	if c.err != nil {
		return &define.Result{Error: c.err}
	}

	// Process encrypted fields
	if err := c.processEncryptedFields(model); err != nil {
		return &define.Result{Error: err}
	}

	// Convert model to map
	transfer := define.GetTransfer(model)
	if transfer == nil {
		return &define.Result{Error: fmt.Errorf("failed to get transfer for model")}
	}

	fields := transfer.ToMap(model)
	if fields == nil || len(fields) == 0 {
		return &define.Result{Error: fmt.Errorf("no fields to insert")}
	}

	// Set table name if not set
	if c.tableName == "" {
		c.tableName = transfer.GetTableName()
	}

	// Store model for potential ID callback
	c.model = model

	// Perform insert
	return c.Sets(fields).executeInsert()
}

// SetEncryptionConfig sets the encryption configuration for the chain
func (c *Chain) SetEncryptionConfig(config *EncryptionConfig) *Chain {
	c.encryptionConfig = config
	return c
}

// executeUpdate executes an UPDATE query
func (c *Chain) executeUpdate() *define.Result {
	if len(c.fieldMap) == 0 {
		return &define.Result{Error: fmt.Errorf("no fields to update")}
	}

	if c.factory == nil {
		return &define.Result{Error: fmt.Errorf("SQL factory is not initialized")}
	}

	if c.db == nil {
		return &define.Result{Error: fmt.Errorf("database connection is not initialized")}
	}

	// 处理敏感字段
	if len(c.sensitiveFields) > 0 {
		if err := c.processSensitiveData(c.fieldMap); err != nil {
			return &define.Result{Error: err}
		}
	}

	// 生成 SQL 和参数
	sqlProto := c.factory.BuildUpdate(c.tableName, c.fieldMap, c.fieldOrder, c.conds)
	return c.executeSqlProto(sqlProto)
}

// Update updates records based on the current conditions
func (c *Chain) Update(models ...interface{}) *define.Result {
	if len(models) > 0 {
		// If model is provided, use it to set fields
		return c.From(models[0]).executeUpdate()
	}
	return c.executeUpdate()
}

// Delete deletes records based on the current conditions
func (c *Chain) Delete(models ...interface{}) *define.Result {
	if len(models) > 0 {
		// If model is provided, use it to set conditions
		return c.From(models[0]).Delete()
	}

	if c.factory == nil {
		return &define.Result{Error: fmt.Errorf("SQL factory is not initialized")}
	}

	if c.db == nil {
		return &define.Result{Error: fmt.Errorf("database connection is not initialized")}
	}

	sqlProto := c.factory.BuildDelete(c.tableName, c.conds)
	return c.executeSqlProto(sqlProto)

}
func (c *Chain) executeSqlProto(sqlProto *define.SqlProto) *define.Result {
	if define.Debug {
		log.Printf("[SQL] %s %s %v %v", sqlProto.SqlType, sqlProto.Sql, sqlProto.Args, sqlProto.Error)
	}

	if sqlProto.Error != nil {
		return &define.Result{Error: sqlProto.Error}
	}
	if sqlProto.SqlType == define.Query {

		// Start query stats
		c.startQueryStats(sqlProto.Sql, sqlProto.Args)

		// Execute query
		var rows *sql.Rows
		var err error
		if c.tx != nil {
			if c.ctx == nil {
				rows, err = c.tx.Query(sqlProto.Sql, sqlProto.Args...)
			} else {
				rows, err = c.tx.QueryContext(c.ctx, sqlProto.Sql, sqlProto.Args...)
			}
		} else {
			if c.ctx == nil {
				rows, err = c.db.DB.Query(sqlProto.Sql, sqlProto.Args...)
			} else {
				rows, err = c.db.DB.QueryContext(c.ctx, sqlProto.Sql, sqlProto.Args...)
			}
		}
		if err != nil {
			return &define.Result{Error: err}
		}
		defer rows.Close()

		// Create result
		result := &define.Result{}
		err = result.Scan(rows)
		if err != nil {
			return &define.Result{Error: err}
		}
		result.Affected = int64(len(result.Data))
		// End query stats
		c.endQueryStats(result.Affected)
		return result

	} else {
		var sqlResult interface {
			LastInsertId() (int64, error)
			RowsAffected() (int64, error)
		}
		var err error
		if c.tx != nil {
			if c.ctx == nil {
				sqlResult, err = c.tx.Exec(sqlProto.Sql, sqlProto.Args...)
			} else {
				sqlResult, err = c.tx.ExecContext(c.ctx, sqlProto.Sql, sqlProto.Args...)
			}
		} else {
			if c.ctx == nil {
				sqlResult, err = c.db.DB.Exec(sqlProto.Sql, sqlProto.Args...)
			} else {
				sqlResult, err = c.db.DB.ExecContext(c.ctx, sqlProto.Sql, sqlProto.Args...)
			}
		}
		if err != nil {
			return &define.Result{Error: err}
		}

		affected, _ := sqlResult.RowsAffected()
		if err != nil {
			return &define.Result{Error: err}
		}
		lastId, _ := sqlResult.LastInsertId()
		return &define.Result{Affected: affected, ID: lastId}
	}

}

func (c *Chain) saveFromFieldMap() *define.Result {
	if len(c.fieldMap) == 0 {
		return &define.Result{Error: fmt.Errorf("no fields to save")}
	}

	// Process sensitive fields if any
	if len(c.sensitiveFields) > 0 {
		if err := c.processSensitiveData(c.fieldMap); err != nil {
			return &define.Result{Error: err}
		}
	}

	if len(c.conds) > 0 {
		return c.executeUpdate()
	}
	return c.executeInsert()
}

func (c *Chain) processEncryption(transfer *define.Transfer, fields map[string]interface{}) {
	if c.encryptionConfig == nil {
		return
	}

	for field, value := range fields {
		if tag := transfer.GetFieldTag(field); strings.Contains(tag, "encrypt") {
			if encryptedValue, err := c.encryptField(value); err == nil {
				fields[field] = encryptedValue
			}
		}
	}
}

func (c *Chain) convertBoolValues(fields map[string]interface{}) {
	for k, v := range fields {
		if b, ok := v.(bool); ok {
			if b {
				fields[k] = 1
			} else {
				fields[k] = 0
			}
		}
	}
}
func (c *Chain) WithContext(ctx context.Context) *Chain {
	c.ctx = ctx // 设置新上下文
	return c    // 返回新对象保持链式调用
}

func (c *Chain) setModelID(model interface{}, fieldName string, id int64) {
	if modelValue := reflect.ValueOf(model); modelValue.Kind() == reflect.Ptr {
		if idField := modelValue.Elem().FieldByName(fieldName); idField.IsValid() && idField.CanSet() {
			idField.SetInt(id)
		}
	}
}

// calculateOptimalGoroutines calculates the optimal number of concurrent workers
func calculateOptimalGoroutines(totalRecords, batchSize int) int {
	batches := (totalRecords + batchSize - 1) / batchSize // Ceiling division
	return min(batches, 16)                               // Cap at 16 goroutines
}

// WhereRaw adds a raw SQL expression as a WHERE condition
func (c *Chain) WhereRaw(expr string, args ...interface{}) *Chain {
	cond := define.Raw(expr, args...)
	cond.JoinType = define.JoinAnd
	c.conds = append(c.conds, cond)
	return c
}

// OrWhereRaw adds a raw SQL expression as an OR WHERE condition
func (c *Chain) OrWhereRaw(expr string, args ...interface{}) *Chain {
	cond := define.Raw(expr, args...)
	cond.JoinType = define.JoinOr
	c.conds = append(c.conds, cond)
	return c
}
