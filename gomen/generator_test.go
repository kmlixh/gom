package gomen

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func init() {
	// 初始化测试环境
	os.Setenv("TZ", "UTC")
}

// TestGenerator tests the code generator
func TestGenerator(t *testing.T) {
	// 从环境变量获取数据库配置
	pgURL := os.Getenv("TEST_PG_URL")
	if pgURL == "" {
		t.Skip("TEST_PG_URL not set, skipping PostgreSQL tests")
	}

	mysqlURL := os.Getenv("TEST_MYSQL_URL")
	if mysqlURL == "" {
		t.Skip("TEST_MYSQL_URL not set, skipping MySQL tests")
	}

	// 测试 PostgreSQL 连接
	if db, err := sql.Open("pgx", pgURL); err != nil {
		t.Skip("PostgreSQL connection failed:", err)
	} else {
		db.Close()
	}

	// 测试 MySQL 连接
	if db, err := sql.Open("mysql", mysqlURL); err != nil {
		t.Skip("MySQL connection failed:", err)
	} else {
		db.Close()
	}

	// 测试配置
	tests := []struct {
		name    string
		options Options
		wantErr bool
	}{
		{
			name: "PostgreSQL Generator",
			options: Options{
				Driver:      "pgx",
				URL:         pgURL,
				OutputDir:   "testdata/postgres",
				PackageName: "models",
				Pattern:     "public.*",
				Debug:       true,
				TagStyle:    "gom",
				GenerateDB:  true,
			},
			wantErr: false,
		},
		{
			name: "MySQL Generator",
			options: Options{
				Driver:      "mysql",
				URL:         mysqlURL,
				OutputDir:   "testdata/mysql",
				PackageName: "models",
				Pattern:     "*",
				Prefix:      "t_",
				Debug:       true,
				TagStyle:    "gom",
				GenerateDB:  true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建输出目录
			if err := os.MkdirAll(tt.options.OutputDir, 0755); err != nil {
				t.Fatalf("创建输出目录失败: %v", err)
			}
			defer os.RemoveAll(tt.options.OutputDir)

			// 创建生成器实例
			generator, err := NewGenerator(tt.options)
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("创建生成器失败: %v", err)
				}
				return
			}
			defer generator.Close()

			// 执行代码生成
			if err := generator.Generate(); err != nil {
				if !tt.wantErr {
					t.Fatalf("生���代码失败: %v", err)
				}
				return
			}

			// 验证生成的文件
			files, err := os.ReadDir(tt.options.OutputDir)
			if err != nil {
				t.Fatalf("读取输出目录失败: %v", err)
			}

			// 检查是否生成了文件
			if len(files) == 0 {
				t.Error("没有生成任何文件")
			}

			// 检查生成的文件
			for _, file := range files {
				if filepath.Ext(file.Name()) != ".go" {
					t.Errorf("生成了非 Go 文件: %s", file.Name())
				}
			}
		})
	}
}

// ... rest of the test code ...
