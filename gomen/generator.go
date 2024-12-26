package gomen

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/factory/mysql"
	"github.com/kmlixh/gom/v4/factory/postgres"
)

// Options 代码生成选项
type Options struct {
	Driver      string // 数据库驱动类型 (mysql/postgres)
	URL         string // 数据库连接URL
	OutputDir   string // 输出目录
	PackageName string // 包名
	Pattern     string // 表名匹配模式 (PostgreSQL 可用 schema.table* 格式)
	Debug       bool   // 是否开启调试模式
	TagStyle    string // 标签风格 (gom/db)
	GenerateDB  bool   // 是否生成db标签
	Prefix      string // 表名前缀（生成时会去掉）
	Suffix      string // 表名后缀（生成时会去掉）
}

// Generator 代码生成器
type Generator struct {
	options Options
	db      *gom.DB
}

// NewGenerator 创建新的代码生成器实例
func NewGenerator(options Options) (*Generator, error) {
	// 验证必要参数
	if err := validateOptions(&options); err != nil {
		return nil, err
	}

	// 连接数据库
	if options.Driver == "mysql" {
		mysql.RegisterFactory()
	} else if options.Driver == "postgres" {
		postgres.RegisterFactory()
	}
	db, err := gom.Open(options.Driver, options.URL, options.Debug)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	return &Generator{
		options: options,
		db:      db,
	}, nil
}

// Generate 执行代码生成
func (g *Generator) Generate() error {
	// 确保输出目录存在
	if err := os.MkdirAll(g.options.OutputDir, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}

	// 获取匹配的表
	tables, err := g.db.GetTables(g.options.Pattern)
	if err != nil {
		return fmt.Errorf("获取表列表失败: %v", err)
	}

	// 生成每个表的结构体
	for _, tableName := range tables {
		if err := g.generateTableStruct(tableName); err != nil {
			return fmt.Errorf("生成表 %s 的结构体失败: %v", tableName, err)
		}
	}

	// 生成模型注册文件
	if err := g.generateModelRegistry(tables); err != nil {
		return fmt.Errorf("生成模型注册文件失败: %v", err)
	}

	// 格式化生成的代码
	if err := formatGeneratedCode(g.options.OutputDir); err != nil {
		return fmt.Errorf("格式化代码失败: %v", err)
	}

	return nil
}

// generateTableStruct 生成单个表的结构体
func (g *Generator) generateTableStruct(tableName string) error {
	// 获取表信息
	tableInfo, err := g.db.GetTableInfo(tableName)
	if err != nil {
		return fmt.Errorf("获取表信息失败: %v", err)
	}

	// 设置类型检查字段
	tableInfo.HasDecimal = hasDecimalType(tableInfo.Columns)
	tableInfo.HasUUID = hasUUIDType(tableInfo.Columns)
	tableInfo.HasIP = hasIPType(tableInfo.Columns)

	// 处理表名
	structName := tableInfo.TableName
	if idx := strings.LastIndex(structName, "."); idx >= 0 {
		structName = structName[idx+1:]
	}
	if g.options.Prefix != "" {
		structName = strings.TrimPrefix(structName, g.options.Prefix)
	}
	if g.options.Suffix != "" {
		structName = strings.TrimSuffix(structName, g.options.Suffix)
	}
	structName = toGoName(structName)

	// 创建输出文件
	filename := filepath.Join(g.options.OutputDir, snakeCase(structName)+".go")
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("创建文件失败: %v", err)
	}
	defer file.Close()

	// 准备模板数据
	data := struct {
		Timestamp    string
		PackageName  string
		TableInfo    *define.TableInfo
		StructName   string
		GenerateDB   bool
		TagStyle     string
		GenerateJson bool
	}{
		Timestamp:    time.Now().Format("2006-01-02 15:04:05"),
		PackageName:  g.options.PackageName,
		TableInfo:    tableInfo,
		StructName:   structName,
		GenerateDB:   g.options.GenerateDB,
		TagStyle:     g.options.TagStyle,
		GenerateJson: true, // 默认生成 JSON 标签
	}

	// 使用模板生成代码
	tmpl, err := template.New("struct").Funcs(template.FuncMap{
		"toGoName":    toGoName,
		"goType":      goType,
		"buildTags":   g.buildTags,
		"isTimeField": isTimeField,
	}).Parse(structTemplate)
	if err != nil {
		return fmt.Errorf("解析模板失败: %v", err)
	}

	if err := tmpl.Execute(file, data); err != nil {
		return fmt.Errorf("生成代码失败: %v", err)
	}

	return nil
}

// generateModelRegistry 生成模型注册文件
func (g *Generator) generateModelRegistry(tables []string) error {
	filename := filepath.Join(g.options.OutputDir, "models.go")
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("创建模型注册文件失败: %v", err)
	}
	defer file.Close()

	// 准备模型列表
	var models []string
	for _, table := range tables {
		if idx := strings.LastIndex(table, "."); idx >= 0 {
			table = table[idx+1:]
		}
		if g.options.Prefix != "" {
			table = strings.TrimPrefix(table, g.options.Prefix)
		}
		if g.options.Suffix != "" {
			table = strings.TrimSuffix(table, g.options.Suffix)
		}
		models = append(models, toGoName(table))
	}

	data := struct {
		Timestamp   string
		PackageName string
		Models      []string
	}{
		Timestamp:   time.Now().Format("2006-01-02 15:04:05"),
		PackageName: g.options.PackageName,
		Models:      models,
	}

	tmpl, err := template.New("models").Parse(modelsTemplate)
	if err != nil {
		return fmt.Errorf("解析模板失败: %v", err)
	}

	if err := tmpl.Execute(file, data); err != nil {
		return fmt.Errorf("生成模型注册文件失败: %v", err)
	}

	return nil
}

// Close 关闭数据库连接
func (g *Generator) Close() error {
	if g.db != nil {
		return g.db.Close()
	}
	return nil
}

// validateOptions 验证生成选项
func validateOptions(options *Options) error {
	if options.Driver == "" {
		return fmt.Errorf("数据库驱动类型不能为空")
	}
	if options.Driver != "mysql" && options.Driver != "postgres" {
		return fmt.Errorf("不支持的数据库类型: %s", options.Driver)
	}
	if options.URL == "" {
		return fmt.Errorf("数据库连接URL不能为空")
	}
	if options.OutputDir == "" {
		options.OutputDir = "models"
	}
	if options.PackageName == "" {
		options.PackageName = "models"
	}
	if options.Pattern == "" {
		options.Pattern = "*"
	}
	if options.TagStyle == "" {
		options.TagStyle = "gom"
	}
	return nil
}

// buildTags 构建字段标签
func (g *Generator) buildTags(col *define.ColumnInfo) string {
	var tags []string

	// 主标签 (gom)
	mainTag := fmt.Sprintf(`%s:"%s"`, g.options.TagStyle, col.Name)
	if col.IsPrimaryKey {
		mainTag += ",@"
	}
	if col.IsAutoIncrement {
		mainTag += ",auto"
	}
	if !col.IsNullable {
		mainTag += ",notnull"
	}
	tags = append(tags, mainTag)

	// JSON标签 (始终生成)
	tags = append(tags, fmt.Sprintf(`json:"%s"`, col.Name))

	// DB标签 (可选)
	if g.options.GenerateDB {
		tags = append(tags, fmt.Sprintf(`db:"%s"`, col.Name))
	}

	return "`" + strings.Join(tags, " ") + "`"
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
			if err := formatGoFile(filePath); err != nil {
				return fmt.Errorf("格式化文件 %s 失败: %v", filePath, err)
			}
		}
	}

	return nil
}

// formatGoFile 格式化单个Go文件
func formatGoFile(filePath string) error {
	cmd := exec.Command("go", "fmt", filePath)
	return cmd.Run()
}

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
		} else if strings.Contains(dbType, "small") {
			goType = "int16"
		} else if strings.Contains(dbType, "tiny") {
			goType = "int8"
		} else {
			goType = "int"
		}
	case strings.Contains(dbType, "decimal"), strings.Contains(dbType, "numeric"):
		goType = "decimal.Decimal"
	case strings.Contains(dbType, "float"):
		goType = "float32"
	case strings.Contains(dbType, "double"):
		goType = "float64"
	case strings.Contains(dbType, "bool"):
		goType = "bool"
	case strings.Contains(dbType, "time"), strings.Contains(dbType, "date"):
		goType = "time.Time"
	case strings.Contains(dbType, "json"):
		goType = "json.RawMessage"
	case strings.Contains(dbType, "uuid"):
		goType = "uuid.UUID"
	case strings.Contains(dbType, "inet"):
		goType = "net.IP"
	default:
		goType = "string"
	}

	if isNullable {
		return "*" + goType
	}
	return goType
}

func isTimeField(name string) bool {
	name = strings.ToLower(name)
	return name == "created_at" || name == "updated_at" || name == "deleted_at"
}

// hasDecimalType 检查是否包含 Decimal 类型
func hasDecimalType(columns []define.ColumnInfo) bool {
	for _, col := range columns {
		if strings.Contains(strings.ToLower(col.Type), "decimal") ||
			strings.Contains(strings.ToLower(col.Type), "numeric") {
			return true
		}
	}
	return false
}

// hasUUIDType 检查是否包含 UUID 类型
func hasUUIDType(columns []define.ColumnInfo) bool {
	for _, col := range columns {
		if strings.Contains(strings.ToLower(col.Type), "uuid") {
			return true
		}
	}
	return false
}

// hasIPType 检查是否包含 IP 类型
func hasIPType(columns []define.ColumnInfo) bool {
	for _, col := range columns {
		if strings.Contains(strings.ToLower(col.Type), "inet") {
			return true
		}
	}
	return false
}

// 结构体模板
const structTemplate = `// Code generated by gom at {{.Timestamp}}. DO NOT EDIT.
package {{.PackageName}}

import (
	"time"
	{{- if .GenerateJson}}
	"encoding/json"
	{{- end}}
	{{- if .TableInfo.HasDecimal}}
	"github.com/shopspring/decimal"
	{{- end}}
	{{- if .TableInfo.HasUUID}}
	"github.com/google/uuid"
	{{- end}}
	{{- if .TableInfo.HasIP}}
	"net"
	{{- end}}
)

// {{.StructName}} {{.TableInfo.TableComment}}
type {{.StructName}} struct {
	{{- range .TableInfo.Columns}}
	{{toGoName .Name}} {{goType .Type .IsNullable}} {{buildTags .}} {{if .Comment}}// {{.Comment}}{{end}}
	{{- end}}
}

// TableName returns the table name
func (m *{{.StructName}}) TableName() string {
	return "{{.TableInfo.TableName}}"
}

// BeforeCreate handles the before create hook
func (m *{{.StructName}}) BeforeCreate() error {
	now := time.Now()
	{{- range .TableInfo.Columns}}
	{{- if and (isTimeField .Name) (eq .Name "created_at")}}
	m.CreatedAt = now
	{{- end}}
	{{- if and (isTimeField .Name) (eq .Name "updated_at")}}
	m.UpdatedAt = now
	{{- end}}
	{{- end}}
	return nil
}

// BeforeUpdate handles the before update hook
func (m *{{.StructName}}) BeforeUpdate() error {
	{{- range .TableInfo.Columns}}
	{{- if and (isTimeField .Name) (eq .Name "updated_at")}}
	m.UpdatedAt = time.Now()
	{{- end}}
	{{- end}}
	return nil
}
`

// 模型注册模板
const modelsTemplate = `// Code generated by gom at {{.Timestamp}}. DO NOT EDIT.
package {{.PackageName}}

// RegisterModels registers all models
func RegisterModels() []interface{} {
	return []interface{}{
		{{- range .Models}}
		&{{.}}{},
		{{- end}}
	}
}
`
