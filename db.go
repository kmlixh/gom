package gom

import (
	"database/sql"
	"fmt"
	"github.com/kmlixh/gom/v4/define"
	"log"
	"reflect"
	"sync/atomic"
	"time"
)

var routineIDCounter int64

// SQLFactory defines the interface for different SQL dialects

// DB represents the database connection
type DB struct {
	DB        *sql.DB
	Tx        *sql.Tx
	Factory   define.SQLFactory
	RoutineID int64
}

// cloneSelfIfDifferentGoRoutine ensures thread safety by cloning DB instance if needed
func (db *DB) cloneSelfIfDifferentGoRoutine() *DB {
	currentID := atomic.AddInt64(&routineIDCounter, 1)
	if db.RoutineID == 0 {
		atomic.StoreInt64(&db.RoutineID, currentID)
		return db
	}
	if db.RoutineID != currentID {
		newDB := &DB{
			DB:        db.DB,
			Factory:   db.Factory,
			RoutineID: currentID,
		}
		return newDB
	}
	return db
}

// Query starts a new query chain
func (db *DB) Query(table string) *QueryChain {
	db = db.cloneSelfIfDifferentGoRoutine()
	return &QueryChain{
		Chain: &Chain{
			db:        db,
			tableName: table,
			factory:   db.Factory,
		},
	}
}

// Update starts a new update chain
func (db *DB) Update(table string) *UpdateChain {
	db = db.cloneSelfIfDifferentGoRoutine()
	return &UpdateChain{
		Chain: &Chain{
			db:        db,
			tableName: table,
			factory:   db.Factory,
		},
	}
}

// Insert starts a new insert chain
func (db *DB) Insert(table string) *InsertChain {
	db = db.cloneSelfIfDifferentGoRoutine()
	return &InsertChain{
		Chain: &Chain{
			db:        db,
			tableName: table,
			factory:   db.Factory,
		},
	}
}

// Delete starts a new delete chain
func (db *DB) Delete(table string) *DeleteChain {
	db = db.cloneSelfIfDifferentGoRoutine()
	return &DeleteChain{
		Chain: &Chain{
			db:        db,
			tableName: table,
			factory:   db.Factory,
		},
	}
}

// Begin starts a new transaction
func (db *DB) Begin() error {
	db = db.cloneSelfIfDifferentGoRoutine()
	if define.Debug {
		log.Printf("[SQL] -------- Transaction Start --------\n")
	}
	tx, err := db.DB.Begin()
	if err != nil {
		if define.Debug {
			log.Printf("[SQL] Transaction Failed: %v\n", err)
			log.Printf("[SQL] -------- Transaction End --------\n")
		}
		return err
	}
	db.Tx = tx
	return nil
}

// Commit commits the current transaction
func (db *DB) Commit() error {
	if db.Tx == nil {
		return nil
	}
	if define.Debug {
		log.Printf("[SQL] -------- Commit Start --------\n")
	}
	err := db.Tx.Commit()
	if define.Debug {
		if err != nil {
			log.Printf("[SQL] Commit Failed: %v\n", err)
		} else {
			log.Printf("[SQL] Commit Successful\n")
		}
		log.Printf("[SQL] -------- Commit End --------\n")
	}
	db.Tx = nil
	return err
}

// Rollback rolls back the current transaction
func (db *DB) Rollback() error {
	if db.Tx == nil {
		return nil
	}
	if define.Debug {
		log.Printf("[SQL] -------- Rollback Start --------\n")
	}
	err := db.Tx.Rollback()
	if define.Debug {
		if err != nil {
			log.Printf("[SQL] Rollback Failed: %v\n", err)
		} else {
			log.Printf("[SQL] Rollback Successful\n")
		}
		log.Printf("[SQL] -------- Rollback End --------\n")
	}
	db.Tx = nil
	return err
}

// Close closes the database connection
func (db *DB) Close() error {
	if db.Tx != nil {
		if err := db.Rollback(); err != nil {
			return err
		}
	}
	return db.DB.Close()
}

// logQuery logs SQL query and arguments if debug mode is enabled
func (db *DB) logQuery(query string, args ...interface{}) {
	if define.Debug {
		log.Printf("[SQL] -------- Query Start --------\n")
		log.Printf("[SQL] Statement: %s\n", query)
		if len(args) > 0 {
			log.Printf("[SQL] Arguments: %v\n", args)
		}
		log.Printf("[SQL] -------- Query End --------\n")
	}
}

// logResult logs SQL execution result if debug mode is enabled
func (db *DB) logResult(result sql.Result, err error, duration time.Duration) {
	if !define.Debug {
		return
	}

	log.Printf("[SQL] -------- Result Start --------\n")
	log.Printf("[SQL] Execution Time: %v\n", duration)

	if err != nil {
		log.Printf("[SQL] Error: %v\n", err)
	} else {
		if affected, e := result.RowsAffected(); e == nil {
			log.Printf("[SQL] Affected Rows: %d\n", affected)
		}
		if lastID, e := result.LastInsertId(); e == nil {
			log.Printf("[SQL] Last Insert ID: %d\n", lastID)
		}
	}
	log.Printf("[SQL] -------- Result End --------\n")
}

// Execute executes a query that doesn't return rows
func (db *DB) Execute(query string, args ...interface{}) (sql.Result, error) {
	db.logQuery(query, args...)
	start := time.Now()
	var result sql.Result
	var err error
	if db.Tx != nil {
		result, err = db.Tx.Exec(query, args...)
	} else {
		result, err = db.DB.Exec(query, args...)
	}

	duration := time.Since(start)
	db.logResult(result, err, duration)
	return result, err
}

// ExecuteQuery executes a query that returns rows
func (db *DB) ExecuteQuery(query string, args ...interface{}) (*sql.Rows, error) {
	db.logQuery(query, args...)
	start := time.Now()

	var rows *sql.Rows
	var err error
	if db.Tx != nil {
		rows, err = db.Tx.Query(query, args...)
	} else {
		rows, err = db.DB.Query(query, args...)
	}

	if define.Debug {
		duration := time.Since(start)
		log.Printf("[SQL] -------- Query Result --------\n")
		log.Printf("[SQL] Execution Time: %v\n", duration)
		if err != nil {
			log.Printf("[SQL] Error: %v\n", err)
		}
		log.Printf("[SQL] -------- Query End --------\n")
	}

	return rows, err
}

// InsertModel inserts a model into the database
func (db *DB) InsertModel(table string, model interface{}) (sql.Result, error) {
	return db.Insert(table).Model(model).Execute()
}

// InsertModels inserts multiple models into the database
func (db *DB) InsertModels(table string, models interface{}) (sql.Result, error) {
	return db.Insert(table).Models(models).Execute()
}

// UpdateModel updates a model in the database
func (db *DB) UpdateModel(table string, model interface{}, where string, args ...interface{}) (sql.Result, error) {
	return db.Update(table).Model(model).Where(where, args...).Execute()
}

// UpdateModelWithFields updates specific fields of a model in the database
func (db *DB) UpdateModelWithFields(table string, model interface{}, fields []string, where string, args ...interface{}) (sql.Result, error) {
	return db.Update(table).ModelWithFields(model, fields...).Where(where, args...).Execute()
}

// DeleteModel deletes a model from the database
func (db *DB) DeleteModel(table string, where string, args ...interface{}) (sql.Result, error) {
	return db.Delete(table).Where(where, args...).Execute()
}

// QueryModel queries a single model from the database
func (db *DB) QueryModel(table string, model interface{}, where string, args ...interface{}) error {
	return db.Query(table).Where(where, args...).IntoOne(model)
}

// QueryModels queries multiple models from the database
func (db *DB) QueryModels(table string, models interface{}, where string, args ...interface{}) error {
	return db.Query(table).Where(where, args...).Into(models)
}

// QueryPage queries a page of results from the database
func (db *DB) QueryPage(table string, pageNumber, pageSize int, where string, args ...interface{}) (*Page, error) {
	return db.Query(table).Where(where, args...).Page(pageNumber, pageSize)
}

// QueryPageInto queries a page of results from the database and scans them into a slice of structs
func (db *DB) QueryPageInto(table string, pageNumber, pageSize int, dest interface{}, where string, args ...interface{}) (*PageResult, error) {
	return db.Query(table).Where(where, args...).PageInto(pageNumber, pageSize, dest)
}

// QueryPageWithFields queries a page of results with specific fields from the database
func (db *DB) QueryPageWithFields(table string, pageNumber, pageSize int, fields []string, where string, args ...interface{}) (*Page, error) {
	return db.Query(table).Fields(fields...).Where(where, args...).Page(pageNumber, pageSize)
}

// QueryPageIntoWithFields queries a page of results with specific fields from the database and scans them into a slice of structs
func (db *DB) QueryPageIntoWithFields(table string, pageNumber, pageSize int, dest interface{}, fields []string, where string, args ...interface{}) (*PageResult, error) {
	return db.Query(table).Fields(fields...).Where(where, args...).PageInto(pageNumber, pageSize, dest)
}

// BatchInsert performs a batch insert operation
func (db *DB) BatchInsert(table string, fields []string, values [][]interface{}) (sql.Result, error) {
	return db.Insert(table).Fields(fields...).BatchValues(values).Execute()
}

// BatchInsertModels performs a batch insert operation with models
func (db *DB) BatchInsertModels(table string, models interface{}) (sql.Result, error) {
	return db.Insert(table).Models(models).Execute()
}

// BatchUpdate performs a batch update operation
func (db *DB) BatchUpdate(table string, fields []string, values [][]interface{}, where string, args ...interface{}) (sql.Result, error) {
	chain := db.Update(table).Fields(fields...)
	if where != "" {
		chain.Where(where, args...)
	}
	for _, value := range values {
		chain.Values(value...)
	}
	return chain.Execute()
}

// BatchDelete performs a batch delete operation
func (db *DB) BatchDelete(table string, where string, args ...interface{}) (sql.Result, error) {
	return db.Delete(table).Where(where, args...).Execute()
}

// BatchDeleteIn performs a batch delete operation using IN clause
func (db *DB) BatchDeleteIn(table string, field string, values ...interface{}) (sql.Result, error) {
	return db.Delete(table).In(field, values...).Execute()
}

// BatchUpdateModels performs a batch update operation with models
func (db *DB) BatchUpdateModels(table string, models interface{}, where string, args ...interface{}) (sql.Result, error) {
	v := reflect.ValueOf(models)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models must be a slice")
	}

	if v.Len() == 0 {
		return nil, nil
	}

	// Get fields from the first model
	first := v.Index(0)
	if first.Kind() == reflect.Ptr {
		first = first.Elem()
	}
	if first.Kind() != reflect.Struct {
		return nil, fmt.Errorf("models must be a slice of structs")
	}

	t := first.Type()
	fields := make([]string, 0)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}
		fields = append(fields, tag)
	}

	// Get values from all models
	values := make([][]interface{}, v.Len())
	for i := 0; i < v.Len(); i++ {
		model := v.Index(i)
		if model.Kind() == reflect.Ptr {
			model = model.Elem()
		}
		if model.Kind() != reflect.Struct {
			continue
		}

		modelValues := make([]interface{}, 0, len(fields))
		for j := 0; j < t.NumField(); j++ {
			field := t.Field(j)
			tag := field.Tag.Get("gom")
			if tag == "" || tag == "-" {
				continue
			}
			modelValues = append(modelValues, model.Field(j).Interface())
		}
		values[i] = modelValues
	}

	return db.BatchUpdate(table, fields, values, where, args...)
}

// TransactionFunc represents a function that executes within a transaction
type TransactionFunc func(*DB) error

// WithTransaction executes a function within a transaction
func (db *DB) WithTransaction(fn TransactionFunc) error {
	if err := db.Begin(); err != nil {
		return err
	}

	if err := fn(db); err != nil {
		if rbErr := db.Rollback(); rbErr != nil {
			return fmt.Errorf("error rolling back: %v (original error: %v)", rbErr, err)
		}
		return err
	}

	return db.Commit()
}

// InTransaction returns true if the DB is currently in a transaction
func (db *DB) InTransaction() bool {
	return db.Tx != nil
}

// MustBegin starts a new transaction and panics on error
func (db *DB) MustBegin() *DB {
	if err := db.Begin(); err != nil {
		panic(err)
	}
	return db
}

// MustCommit commits the current transaction and panics on error
func (db *DB) MustCommit() {
	if err := db.Commit(); err != nil {
		panic(err)
	}
}

// MustRollback rolls back the current transaction and panics on error
func (db *DB) MustRollback() {
	if err := db.Rollback(); err != nil {
		panic(err)
	}
}

// TransactionContext represents a transaction context
type TransactionContext struct {
	db *DB
}

// Begin starts a new transaction context
func (db *DB) BeginContext() (*TransactionContext, error) {
	if err := db.Begin(); err != nil {
		return nil, err
	}
	return &TransactionContext{db: db}, nil
}

// Commit commits the transaction context
func (tc *TransactionContext) Commit() error {
	return tc.db.Commit()
}

// Rollback rolls back the transaction context
func (tc *TransactionContext) Rollback() error {
	return tc.db.Rollback()
}

// DB returns the underlying DB
func (tc *TransactionContext) DB() *DB {
	return tc.db
}

// MustClose closes the database connection and panics on error
func (db *DB) MustClose() {
	if err := db.Close(); err != nil {
		panic(err)
	}
}

// Ping verifies a connection to the database is still alive
func (db *DB) Ping() error {
	return db.DB.Ping()
}

// SetMaxOpenConns sets the maximum number of open connections to the database
func (db *DB) SetMaxOpenConns(n int) {
	db.DB.SetMaxOpenConns(n)
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection pool
func (db *DB) SetMaxIdleConns(n int) {
	db.DB.SetMaxIdleConns(n)
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.DB.SetConnMaxLifetime(d)
}

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

// Raw executes a raw SQL query and returns the result
