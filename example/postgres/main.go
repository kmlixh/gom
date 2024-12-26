package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/example"
	_ "github.com/kmlixh/gom/v4/factory/postgres"
)

// ReturnResult 用于接收 RETURNING 子句的结果
type ReturnResult struct {
	ID        int64     `gom:"id"`
	CreatedAt time.Time `gom:"created_at"`
}

// UserTheme 用于接收 JSONB 查询结果
type UserTheme struct {
	Username string `gom:"username"`
	Theme    string `gom:"theme"`
}

// SearchResult 用于接收全文搜索结果
type SearchResult struct {
	Username string  `gom:"username"`
	Email    string  `gom:"email"`
	Rank     float64 `gom:"rank"`
}

// CategoryTree 用于接收递归查询结果
type CategoryTree struct {
	Path  string `gom:"path"`
	Level int    `gom:"level"`
}

// LockedUser 用于接收锁定的用户记录
type LockedUser struct {
	ID       int64  `gom:"id"`
	Username string `gom:"username"`
}

// 演示PostgreSQL特有的RETURNING功能
func demonstrateReturning(db *gom.DB) error {
	fmt.Println("\nDemonstrating PostgreSQL RETURNING clause...")

	now := time.Now()
	// 插入并返回生成的ID
	query := `
		INSERT INTO users (username, email, age, active, role, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			RETURNING id, created_at
	`

	var results []ReturnResult
	err := db.Chain().RawQuery(query,
		"pg_user",
		"pg@example.com",
		28,
		true,
		"user",
		now,
		now,
	).Into(&results)

	if err != nil {
		return fmt.Errorf("insert with returning failed: %v", err)
	}

	if len(results) > 0 {
		fmt.Printf("Inserted user with ID: %v, Created at: %v\n", results[0].ID, results[0].CreatedAt)
	}
	return nil
}

// 演示PostgreSQL的JSONB操作
func demonstrateJsonbOperations(db *gom.DB) error {
	fmt.Println("\nDemonstrating JSONB operations...")

	// 创建带JSONB列的表
	_, err := db.Chain().RawExecute(`
		CREATE TABLE IF NOT EXISTS user_settings (
			id SERIAL PRIMARY KEY,
			user_id BIGINT REFERENCES users(id),
			preferences JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)

	if err != nil {
		return fmt.Errorf("create table failed: %v", err)
	}

	// 插入JSONB数据
	_, err = db.Chain().RawExecute(`
		INSERT INTO user_settings (user_id, preferences)
		SELECT id, '{"theme": "dark", "notifications": true}'::jsonb
		FROM users
		WHERE username = $1
	`, "pg_user")

	if err != nil {
		return fmt.Errorf("insert jsonb failed: %v", err)
	}

	// 查询JSONB数据
	var results []UserTheme
	err = db.Chain().RawQuery(`
		SELECT u.username, s.preferences->>'theme' as theme
		FROM users u
		JOIN user_settings s ON u.id = s.user_id
		WHERE s.preferences @> '{"notifications": true}'::jsonb
	`).Into(&results)

	if err != nil {
		return fmt.Errorf("query jsonb failed: %v", err)
	}

	fmt.Printf("Found %d users with notification preferences\n", len(results))
	for _, r := range results {
		fmt.Printf("User: %s, Theme: %s\n", r.Username, r.Theme)
	}
	return nil
}

// 演示PostgreSQL的全文搜索
func demonstrateFullTextSearch(db *gom.DB) error {
	fmt.Println("\nDemonstrating full-text search...")

	// 添加全文搜索列
	_, err := db.Chain().RawExecute(`
		ALTER TABLE users ADD COLUMN IF NOT EXISTS search_vector tsvector
		GENERATED ALWAYS AS (
			setweight(to_tsvector('english', coalesce(username,'')), 'A') ||
			setweight(to_tsvector('english', coalesce(email,'')), 'B')
		) STORED
	`)

	if err != nil {
		return fmt.Errorf("add search vector failed: %v", err)
	}

	// 创建全文搜索索引
	_, err = db.Chain().RawExecute(`
		CREATE INDEX IF NOT EXISTS users_search_idx ON users USING GIN (search_vector)
	`)

	if err != nil {
		return fmt.Errorf("create search index failed: %v", err)
	}

	// 执行全文搜索
	var results []SearchResult
	err = db.Chain().RawQuery(`
		SELECT username, email, ts_rank(search_vector, query) as rank
		FROM users, plainto_tsquery('english', $1) query
		WHERE search_vector @@ query
		ORDER BY rank DESC
	`, "user").Into(&results)

	if err != nil {
		return fmt.Errorf("full-text search failed: %v", err)
	}

	fmt.Printf("Found %d matching documents\n", len(results))
	for _, r := range results {
		fmt.Printf("User: %s (%s), Rank: %.2f\n", r.Username, r.Email, r.Rank)
	}
	return nil
}

// 演示PostgreSQL的递归查询
func demonstrateRecursiveQueries(db *gom.DB) error {
	fmt.Println("\nDemonstrating recursive queries...")

	// 创建层级数据表
	_, err := db.Chain().RawExecute(`
		CREATE TABLE IF NOT EXISTS categories (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			parent_id INT REFERENCES categories(id),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)

	if err != nil {
		return fmt.Errorf("create categories table failed: %v", err)
	}

	// 插入示例数据
	_, err = db.Chain().RawExecute(`
		INSERT INTO categories (name, parent_id) VALUES
		('Electronics', NULL),
		('Computers', 1),
		('Laptops', 2),
		('Gaming Laptops', 3)
		ON CONFLICT DO NOTHING
	`)

	if err != nil {
		return fmt.Errorf("insert categories failed: %v", err)
	}

	// 执行递归查询
	var results []CategoryTree
	err = db.Chain().RawQuery(`
		WITH RECURSIVE category_tree AS (
			SELECT id, name, parent_id, 1 as level, name::text as path
			FROM categories
			WHERE parent_id IS NULL
			
			UNION ALL
			
			SELECT c.id, c.name, c.parent_id, ct.level + 1,
				(ct.path || ' > ' || c.name::text)
			FROM categories c
			JOIN category_tree ct ON ct.id = c.parent_id
		)
		SELECT path, level
		FROM category_tree
		ORDER BY path
	`).Into(&results)

	if err != nil {
		return fmt.Errorf("recursive query failed: %v", err)
	}

	fmt.Println("Category hierarchy:")
	for _, r := range results {
		fmt.Printf("%s (Level %d)\n", r.Path, r.Level)
	}
	return nil
}

// 演示事务和并发控制
func demonstrateConcurrencyControl(db *gom.DB) error {
	fmt.Println("\nDemonstrating concurrency control...")

	return db.Chain().Transaction(func(chain *gom.Chain) error {
		// 设置事务隔离级别为可序列化
		chain.SetIsolationLevel(sql.LevelSerializable)

		// 使用SKIP LOCKED进行并发处理
		var results []LockedUser
		err := chain.RawQuery(`
			SELECT id, username
			FROM users
			WHERE active = true
			FOR UPDATE SKIP LOCKED
			LIMIT 5
			`).Into(&results)

		if err != nil {
			return fmt.Errorf("select with skip locked failed: %v", err)
		}

		fmt.Printf("Locked %d rows for processing\n", len(results))

		// 处理锁定的记录
		for _, user := range results {
			_, err = chain.RawExecute(`
				UPDATE users
				SET updated_at = CURRENT_TIMESTAMP
				WHERE id = $1
			`, user.ID)

			if err != nil {
				return fmt.Errorf("update locked row failed: %v", err)
			}
			fmt.Printf("Updated user: %s\n", user.Username)
		}

		return nil
	})
}

// 演示代码生成
func demonstrateCodeGeneration(db *gom.DB) error {
	fmt.Println("\nDemonstrating code generation...")

	// 创建目标目录
	err := os.MkdirAll("./generated", 0755)
	if err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// 生成单个表的结构体（包含 schema）
	fmt.Println("Generating struct for 'public.users' table...")
	err = db.GenerateStruct("public.users", "./generated", "models")
	if err != nil {
		return fmt.Errorf("generate struct for users failed: %v", err)
	}

	// 生成 public schema 下所有表的结构体
	fmt.Println("Generating structs for all tables in public schema...")
	err = db.GenerateStructs(gom.GenerateOptions{
		OutputDir:   "./generated",
		PackageName: "models",
		Pattern:     "public.*",
	})
	if err != nil {
		return fmt.Errorf("generate structs for public schema failed: %v", err)
	}

	fmt.Println("Code generation completed successfully")
	return nil
}

// 演示PostgreSQL的各种数据类型
func demonstrateDataTypes(db *gom.DB) error {
	fmt.Println("\nDemonstrating PostgreSQL data types...")

	// 删除并创建测试表
	_, err := db.Chain().RawExecute("DROP TABLE IF EXISTS test_types")
	if err != nil {
		return fmt.Errorf("drop table failed: %v", err)
	}

	_, err = db.Chain().RawExecute(`
		CREATE TABLE test_types (
			id BIGSERIAL PRIMARY KEY,
			small_int_col SMALLINT,
			int_col INTEGER,
			big_int_col BIGINT,
			decimal_col DECIMAL(10,2),
			real_col REAL,
			double_col DOUBLE PRECISION,
			varchar_col VARCHAR(255),
			text_col TEXT,
			char_col CHAR(10),
			bool_col BOOLEAN,
			date_col DATE,
			time_col TIME,
			timestamp_col TIMESTAMP,
			timestamptz_col TIMESTAMP WITH TIME ZONE,
			interval_col INTERVAL,
			json_col JSON,
			jsonb_col JSONB,
			uuid_col UUID,
			inet_col INET,
			array_int_col INTEGER[],
			array_text_col TEXT[],
			point_col POINT,
			line_col LINE,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("create table failed: %v", err)
	}

	// 生成结构体
	fmt.Println("Generating struct for 'test_types' table...")
	err = db.GenerateStruct("test_types", "./generated", "models")
	if err != nil {
		return fmt.Errorf("generate struct for test_types failed: %v", err)
	}

	// 插入测试数据
	_, err = db.Chain().RawExecute(`
		INSERT INTO test_types (
			small_int_col, int_col, big_int_col, decimal_col,
			real_col, double_col, varchar_col, text_col,
			char_col, bool_col, date_col, time_col,
			timestamp_col, timestamptz_col, interval_col,
			json_col, jsonb_col, uuid_col, inet_col,
			array_int_col, array_text_col, point_col, line_col
		) VALUES (
			100, 1000, 10000, 123.45,
			123.456, 123.4567, 'varchar text', 'long text content',
			'char(10)', true, '2024-01-01', '12:00:00',
			'2024-01-01 12:00:00', '2024-01-01 12:00:00+08',
			'1 year 2 months',
			'{"key": "value"}',
			'{"key": "value"}',
			'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
			'192.168.1.1',
			'{1,2,3}',
			'{"a","b","c"}',
			'(1,1)',
			'{1,2,3}'
		)
	`)
	if err != nil {
		return fmt.Errorf("insert test data failed: %v", err)
	}

	fmt.Println("PostgreSQL data types test completed successfully")
	return nil
}

func main() {
	// 连接��PostgreSQL
	db, err := gom.Open("postgres", "postgres://postgres:yzy123@192.168.110.249:5432/test?sslmode=disable", true)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// 清理并创建表
	fmt.Println("Setting up database...")
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS user_settings CASCADE")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS user_profiles CASCADE")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS users CASCADE")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS user_roles CASCADE")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS categories CASCADE")
	if err != nil {
		log.Fatal(err)
	}

	// 创建基础表
	err = db.Chain().CreateTable(&example.UserRole{})
	if err != nil {
		log.Fatal(err)
	}
	err = db.Chain().CreateTable(&example.User{})
	if err != nil {
		log.Fatal(err)
	}
	err = db.Chain().CreateTable(&example.UserProfile{})
	if err != nil {
		log.Fatal(err)
	}

	// 演示PostgreSQL特有功能
	if err := demonstrateReturning(db); err != nil {
		log.Fatal(err)
	}

	if err := demonstrateJsonbOperations(db); err != nil {
		log.Fatal(err)
	}

	if err := demonstrateFullTextSearch(db); err != nil {
		log.Fatal(err)
	}

	if err := demonstrateRecursiveQueries(db); err != nil {
		log.Fatal(err)
	}

	if err := demonstrateConcurrencyControl(db); err != nil {
		log.Fatal(err)
	}

	// 演示代码生成
	if err := demonstrateCodeGeneration(db); err != nil {
		log.Fatal(err)
	}

	// 演示各种数据类型
	if err := demonstrateDataTypes(db); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nAll PostgreSQL examples completed successfully!")
}
