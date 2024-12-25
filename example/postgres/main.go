package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/example"
	_ "github.com/kmlixh/gom/v4/factory/postgres"
)

// 演示PostgreSQL特有的RETURNING功能
func demonstrateReturning(db *gom.DB) error {
	fmt.Println("\nDemonstrating PostgreSQL RETURNING clause...")

	// 插入并返回生成的ID
	query := `
		INSERT INTO users (username, email, age, active, role, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at
	`

	result, err := db.Chain().RawQuery(query,
		"pg_user",
		"pg@example.com",
		28,
		true,
		"user",
		time.Now(),
		time.Now(),
	)

	if err != nil {
		return fmt.Errorf("insert with returning failed: %v", err)
	}

	if len(result.Data) > 0 {
		fmt.Printf("Inserted user with ID: %v\n", result.Data[0]["id"])
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
	result, err := db.Chain().RawQuery(`
		SELECT u.username, s.preferences->>'theme' as theme
		FROM users u
		JOIN user_settings s ON u.id = s.user_id
		WHERE s.preferences @> '{"notifications": true}'::jsonb
	`)

	if err != nil {
		return fmt.Errorf("query jsonb failed: %v", err)
	}

	fmt.Printf("Found %d users with notification preferences\n", len(result.Data))
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
	result, err := db.Chain().RawQuery(`
		SELECT username, email, ts_rank(search_vector, query) as rank
		FROM users, plainto_tsquery('english', $1) query
		WHERE search_vector @@ query
		ORDER BY rank DESC
	`, "user")

	if err != nil {
		return fmt.Errorf("full-text search failed: %v", err)
	}

	fmt.Printf("Found %d matching documents\n", len(result.Data))
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
	result, err := db.Chain().RawQuery(`
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
	`)

	if err != nil {
		return fmt.Errorf("recursive query failed: %v", err)
	}

	fmt.Println("Category hierarchy:")
	for _, row := range result.Data {
		fmt.Printf("%s (Level %v)\n", row["path"], row["level"])
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
		result, err := chain.RawQuery(`
			SELECT id, username
			FROM users
			WHERE active = true
			FOR UPDATE SKIP LOCKED
			LIMIT 5
		`)

		if err != nil {
			return fmt.Errorf("select with skip locked failed: %v", err)
		}

		fmt.Printf("Locked %d rows for processing\n", len(result.Data))

		// 处理锁定的记录
		for _, row := range result.Data {
			_, err = chain.RawExecute(`
				UPDATE users
				SET updated_at = CURRENT_TIMESTAMP
				WHERE id = $1
			`, row["id"])

			if err != nil {
				return fmt.Errorf("update locked row failed: %v", err)
			}
		}

		return nil
	})
}

// 演示代码生成
func demonstrateCodeGeneration(db *gom.DB) error {
	fmt.Println("\nDemonstrating code generation...")

	// 生成单个表的结构体（包含 schema）
	fmt.Println("Generating struct for 'public.users' table...")
	err := db.GenerateStruct("public.users", "./generated", "models")
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

	// 生成 public schema 下所有以 user 开头的表的结构体
	fmt.Println("Generating structs for all user-related tables in public schema...")
	err = db.GenerateStructs(gom.GenerateOptions{
		OutputDir:   "./generated",
		PackageName: "models",
		Pattern:     "public.user*",
	})
	if err != nil {
		return fmt.Errorf("generate structs for user tables failed: %v", err)
	}

	fmt.Println("Code generation completed successfully")
	return nil
}

func main() {
	// 连接到PostgreSQL
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

	fmt.Println("\nAll PostgreSQL examples completed successfully!")
}
