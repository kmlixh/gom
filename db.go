package gom

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"
	"unicode"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/factory/mysql"
	"github.com/kmlixh/gom/v4/factory/postgres"
)

// DBErrorType represents specific types of database errors
type DBErrorType int

const (
	ErrConnection DBErrorType = iota
	ErrQuery
	ErrTransaction
	ErrValidation
	ErrConfiguration
)

// DBError represents a database operation error with enhanced context
type DBError struct {
	Type    DBErrorType
	Op      string
	Err     error
	Details string
	Query   string // Optional, for debugging (only set in debug mode)
}

func (e *DBError) Error() string {
	if e.Query != "" && define.Debug {
		return fmt.Sprintf("%s: %v (%s) [Query: %s]", e.Op, e.Err, e.Details, e.Query)
	}
	if e.Details != "" {
		return fmt.Sprintf("%s: %v (%s)", e.Op, e.Err, e.Details)
	}
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

// newDBError creates a new DBError with the given parameters
func newDBError(errType DBErrorType, op string, err error, details string) *DBError {
	return &DBError{
		Type:    errType,
		Op:      op,
		Err:     err,
		Details: details,
	}
}

// GenerateOptions 代码生成选项
type GenerateOptions struct {
	OutputDir   string // 输出目录
	PackageName string // 包名
	Pattern     string // 表名匹配模式
}

var routineIDCounter int64

// DBMetrics tracks database connection pool statistics
type DBMetrics struct {
	OpenConnections   int64         // Current number of open connections
	InUseConnections  int64         // Current number of connections in use
	IdleConnections   int64         // Current number of idle connections
	WaitCount         int64         // Total number of connections waited for
	WaitDuration      time.Duration // Total time waited for connections
	MaxIdleTimeClosed int64         // Number of connections closed due to max idle time
}

// DB represents the database connection
type DB struct {
	sync.RWMutex
	DB        *sql.DB
	Factory   define.SQLFactory
	RoutineID int64
	options   define.DBOptions
	metrics   *DBMetrics
}

// cloneSelfIfDifferentGoRoutine ensures thread safety by cloning DB instance if needed
func (db *DB) cloneSelfIfDifferentGoRoutine() *DB {
	currentID := atomic.LoadInt64(&routineIDCounter)
	if db.RoutineID == 0 {
		if atomic.CompareAndSwapInt64(&db.RoutineID, 0, currentID) {
			return db
		}
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

// Chain starts a new chain
func (db *DB) Chain() *Chain {
	db = db.cloneSelfIfDifferentGoRoutine()
	return &Chain{
		db:      db,
		factory: db.Factory,
	}
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.DB.Close()
}

// GetMetrics returns the current database metrics
func (db *DB) GetMetrics() DBMetrics {
	stats := db.DB.Stats()
	return DBMetrics{
		OpenConnections:   int64(stats.OpenConnections),
		InUseConnections:  int64(stats.InUse),
		IdleConnections:   int64(stats.Idle),
		WaitCount:         stats.WaitCount,
		WaitDuration:      stats.WaitDuration,
		MaxIdleTimeClosed: stats.MaxIdleClosed,
	}
}

// optimizeConnectionPool configures the database connection pool
func (db *DB) optimizeConnectionPool(opts define.DBOptions) {
	db.DB.SetMaxOpenConns(opts.MaxOpenConns)
	db.DB.SetMaxIdleConns(opts.MaxIdleConns)
	db.DB.SetConnMaxLifetime(opts.ConnMaxLifetime)
	db.DB.SetConnMaxIdleTime(opts.ConnMaxIdleTime)
	db.options = opts

	// Initialize metrics
	db.metrics = &DBMetrics{}

	// Start metrics collection if debug mode is enabled
	if opts.Debug {
		go db.collectMetrics()
	}
}

// collectMetrics periodically updates connection pool metrics
func (db *DB) collectMetrics() {
	ticker := time.NewTicker(time.Second * 5)
	for range ticker.C {
		stats := db.DB.Stats()
		db.metrics = &DBMetrics{
			OpenConnections:   int64(stats.OpenConnections),
			InUseConnections:  int64(stats.InUse),
			IdleConnections:   int64(stats.Idle),
			WaitCount:         stats.WaitCount,
			WaitDuration:      stats.WaitDuration,
			MaxIdleTimeClosed: stats.MaxIdleClosed,
		}
	}
}

// Open creates a new DB connection with options
func Open(driverName, dsn string, opts *define.DBOptions) (*DB, error) {
	// Use default options if none provided
	if opts == nil {
		defaultOpts := define.DefaultDBOptions()
		opts = &defaultOpts
	}

	// Validate options
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %v", err)
	}

	factory, er := define.GetFactory(driverName)
	if er != nil {
		return nil, fmt.Errorf("no SQL factory registered for driver: %s", driverName)
	}

	sqlDB, err := factory.Connect(dsn)
	if err != nil {
		return nil, err
	}

	db := &DB{
		DB:      sqlDB,
		Factory: factory,
		options: *opts,
	}

	// Configure connection pool
	db.optimizeConnectionPool(*opts)

	// Set debug mode
	define.Debug = opts.Debug

	return db, nil
}

// OpenWithDefaults creates a new DB connection with default options
func OpenWithDefaults(driverName, dsn string) (*DB, error) {
	return Open(driverName, dsn, nil)
}

// MustOpen creates a new DB connection with options and panics on error
func MustOpen(driverName, dsn string, opts *define.DBOptions) *DB {
	db, err := Open(driverName, dsn, opts)
	if err != nil {
		panic(err)
	}
	return db
}

// GetOptions returns the current database options
func (db *DB) GetOptions() define.DBOptions {
	return db.options
}

// UpdateOptions updates the database connection pool settings
func (db *DB) UpdateOptions(opts define.DBOptions) error {
	if err := opts.Validate(); err != nil {
		return fmt.Errorf("invalid options: %v", err)
	}

	db.optimizeConnectionPool(opts)
	define.Debug = opts.Debug
	return nil
}

// GetTableInfo 获取表信息
func (db *DB) GetTableInfo(tableName string) (*define.TableInfo, error) {
	return db.Factory.GetTableInfo(db.DB, tableName)
}

// GetTables returns a list of table names in the database
func (db *DB) GetTables(pattern string) ([]string, error) {
	var tables []string

	// For MySQL
	if _, ok := db.Factory.(*mysql.Factory); ok {
		// Convert pattern to SQL LIKE pattern
		if pattern == "*" || pattern == "" {
			pattern = "%"
		} else {
			// Convert glob pattern to SQL LIKE pattern
			pattern = strings.ReplaceAll(pattern, "*", "%")
		}

		// Use INFORMATION_SCHEMA to get table names
		query := `
			SELECT TABLE_NAME 
			FROM INFORMATION_SCHEMA.TABLES 
			WHERE TABLE_SCHEMA = DATABASE() 
			AND TABLE_NAME LIKE ?
		`
		rows, err := db.DB.Query(query, pattern)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				return nil, err
			}
			tables = append(tables, table)
		}
		return tables, rows.Err()
	}

	// For PostgreSQL
	if _, ok := db.Factory.(*postgres.Factory); ok {
		// Convert pattern to SQL LIKE pattern
		if pattern == "*" || pattern == "" {
			pattern = "%"
		} else {
			// Convert glob pattern to SQL LIKE pattern
			pattern = strings.ReplaceAll(pattern, "*", "%")
		}

		query := `
			SELECT schemaname || '.' || tablename
			FROM pg_catalog.pg_tables
			WHERE tablename LIKE $1
			AND schemaname NOT IN ('pg_catalog', 'information_schema')
			ORDER BY schemaname, tablename
		`

		rows, err := db.DB.Query(query, pattern)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				return nil, err
			}
			tables = append(tables, table)
		}
		return tables, rows.Err()
	}

	return nil, fmt.Errorf("unsupported database driver")
}

// GenerateStruct 生成单个表的结构体代码
func (db *DB) GenerateStruct(tableName, outputDir, packageName string) error {
	// 获取表信息
	tableInfo, err := db.GetTableInfo(tableName)
	if err != nil {
		return fmt.Errorf("failed to get table info: %v", err)
	}

	return generateStructFile(tableInfo, outputDir, packageName)
}

// GenerateStructs 批量生成表的结构体代码
func (db *DB) GenerateStructs(opts GenerateOptions) error {
	// 确保输出目录存在
	if err := os.MkdirAll(opts.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// 获取匹配的表
	tables, err := db.GetTables(opts.Pattern)
	if err != nil {
		return fmt.Errorf("failed to get table list: %v", err)
	}

	// 生成每个表的结构体
	for _, tableName := range tables {
		if err := db.GenerateStruct(tableName, opts.OutputDir, opts.PackageName); err != nil {
			return fmt.Errorf("failed to generate struct for table %s: %v", tableName, err)
		}
	}

	// 格式化生成的代码
	if err := formatGeneratedCode(opts.OutputDir); err != nil {
		return fmt.Errorf("failed to format generated code: %v", err)
	}

	return nil
}

// generateStructFile 生成结构体文件
func generateStructFile(tableInfo *define.TableInfo, outputDir, packageName string) error {
	// 获取表名（去掉schema前缀，如果有的话）
	tableName := tableInfo.TableName
	if idx := strings.LastIndex(tableName, "."); idx >= 0 {
		tableName = tableName[idx+1:]
	}

	// 创建输出文件
	filename := filepath.Join(outputDir, snakeCase(tableName)+".go")
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("创建文件失败: %v", err)
	}
	defer file.Close()

	// 使用模板生成代码
	tmpl, err := template.New("struct").Funcs(template.FuncMap{
		"toGoName": toGoName,
		"goType":   goType,
	}).Parse(structTemplate)
	if err != nil {
		return fmt.Errorf("解析模板失败: %v", err)
	}

	data := struct {
		Timestamp   string
		PackageName string
		TableInfo   *define.TableInfo
		StructName  string
	}{
		Timestamp:   time.Now().Format("2006-01-02 15:04:05"),
		PackageName: packageName,
		TableInfo:   tableInfo,
		StructName:  toGoName(tableName),
	}

	if err := tmpl.Execute(file, data); err != nil {
		return fmt.Errorf("生成代码失败: %v", err)
	}

	return nil
}

// formatGeneratedCode 格式化生成的代码
func formatGeneratedCode(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".go" {
			filePath := filepath.Join(dir, entry.Name())
			cmd := exec.Command("go", "fmt", filePath)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("格式化文件 %s 失败: %v", filePath, err)
			}
		}
	}

	return nil
}

// 结构体模板
const structTemplate = `// Code generated by gom at {{.Timestamp}}. DO NOT EDIT.
package {{.PackageName}}

import (
	"time"
)

// {{.StructName}} {{.TableInfo.TableComment}}
type {{.StructName}} struct {
	{{- range .TableInfo.Columns}}
	{{toGoName .Name}} {{goType .Type .IsNullable}} ` + "`" + `gom:"{{.Name}}{{if .IsPrimaryKey}},@{{end}}{{if .IsAutoIncrement}},auto{{end}}{{if not .IsNullable}},notnull{{end}}"` + "`" + ` {{if .Comment}}// {{.Comment}}{{end}}
	{{- end}}
}

// TableName returns the table name
func (m *{{.StructName}}) TableName() string {
	return "{{.TableInfo.TableName}}"
}
`

// 辅助函数
func toGoName(name string) string {
	parts := strings.Split(name, "_")
	for i := range parts {
		parts[i] = strings.Title(parts[i])
	}
	return strings.Join(parts, "")
}

func snakeCase(name string) string {
	return strings.ToLower(name)
}

func goType(dbType string, isNullable bool) string {
	dbType = strings.ToLower(dbType)
	var goType string

	switch {
	case strings.Contains(dbType, "int"):
		if strings.Contains(dbType, "big") {
			goType = "int64"
		} else {
			goType = "int"
		}
	case strings.Contains(dbType, "float"), strings.Contains(dbType, "double"), strings.Contains(dbType, "decimal"):
		goType = "float64"
	case strings.Contains(dbType, "bool"):
		goType = "bool"
	case strings.Contains(dbType, "time"), strings.Contains(dbType, "date"):
		goType = "time.Time"
	default:
		goType = "string"
	}

	if isNullable {
		return "*" + goType
	}
	return goType
}

// GetTableName returns the table name for a model
func (db *DB) GetTableName(model interface{}) (string, error) {
	if model == nil {
		return "", fmt.Errorf("model cannot be nil")
	}

	// 如果是字符串，直接返回
	if tableName, ok := model.(string); ok {
		return tableName, nil
	}

	// 获取模型的类型
	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	// 检查是否为结构体
	if modelType.Kind() != reflect.Struct {
		return "", fmt.Errorf("model must be a struct or struct pointer")
	}

	// 检查是否实现了 TableName 接口
	if tabler, ok := model.(interface{ TableName() string }); ok {
		return tabler.TableName(), nil
	}

	// 使用结构体名称转换为表名
	tableName := modelType.Name()
	if strings.HasSuffix(tableName, "Query") {
		tableName = tableName[:len(tableName)-5]
	}
	return toSnakeCase(tableName), nil
}

// toSnakeCase converts a string to snake case
func toSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if i > 0 && unicode.IsUpper(r) {
			result = append(result, '_')
		}
		result = append(result, unicode.ToLower(r))
	}
	return string(result)
}

func (db DB) GetColumns(table string) ([]define.Column, error) {
	return db.Factory.GetColumns(table, db.DB)
}

func (db DB) GetTableStruct(i any, table string) (*define.TableStruct, error) {
	tableInfo, er := db.Factory.GetTableInfo(db.DB, table)
	if er != nil {
		return nil, er
	}
	maps, er := define.GetFieldToColMap(i, tableInfo)
	if er != nil {
		return nil, er
	}
	return &define.TableStruct{
		TableInfo:     *tableInfo,
		FieldToColMap: maps,
	}, nil
}
func (db DB) GetTableStruct2(i any) (*define.TableStruct, error) {
	tableName, er := db.GetTableName(i)
	if er != nil {
		panic("table name was nil")
	}
	return db.GetTableStruct(i, tableName)
}

// GetDB returns the underlying sql.DB object
func (db *DB) GetDB() *sql.DB {
	return db.DB
}
