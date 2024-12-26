package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/example"
	_ "github.com/kmlixh/gom/v4/factory/mysql"
)

// 演示批量插入操作
func demonstrateBatchInsert(db *gom.DB) error {
	fmt.Println("\nDemonstrating batch insert...")
	users := []map[string]interface{}{
		{
			"username":   "batch_user1",
			"email":      "batch1@example.com",
			"age":        25,
			"active":     true,
			"role":       "user",
			"created_at": time.Now(),
			"updated_at": time.Now(),
		},
		{
			"username":   "batch_user2",
			"email":      "batch2@example.com",
			"age":        30,
			"active":     true,
			"role":       "user",
			"created_at": time.Now(),
			"updated_at": time.Now(),
		},
	}

	// 先创建一个角色
	role := &example.UserRole{
		Name:        "user",
		Description: "Normal user role",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	_, err := db.Chain().From(role).Save()
	if err != nil {
		return fmt.Errorf("create role failed: %v", err)
	}

	_, err = db.Chain().From("users").BatchValues(users).Save()
	if err != nil {
		return fmt.Errorf("batch insert failed: %v", err)
	}
	fmt.Println("Batch insert successful")
	return nil
}

// 演示复杂查询
func demonstrateComplexQuery(db *gom.DB) error {
	fmt.Println("\nDemonstrating complex query...")

	// 使用Query模型进行范围查询
	minAge := 25
	maxAge := 35
	isActive := true

	queryModel := &example.UserQuery{
		MinAge:   &minAge,
		MaxAge:   &maxAge,
		IsActive: &isActive,
	}

	var users []example.User
	err := db.Chain().From(queryModel).
		OrderByDesc("age").
		OrderBy("username").
		Page(1, 10).
		List().
		Into(&users)

	if err != nil {
		return fmt.Errorf("complex query failed: %v", err)
	}

	fmt.Printf("Found %d users matching criteria\n", len(users))
	return nil
}

// 演示事务和锁
func demonstrateTransactionAndLocking(db *gom.DB) error {
	fmt.Println("\nDemonstrating transaction with row locking...")

	return db.Chain().Transaction(func(chain *gom.Chain) error {
		// 设置事务隔离级别
		chain.SetIsolationLevel(sql.LevelRepeatableRead)

		// 使用FOR UPDATE进行锁定
		var users []example.User
		err := chain.RawQuery("SELECT * FROM users WHERE age > ? FOR UPDATE", 30).
			Into(&users)
		if err != nil {
			return fmt.Errorf("select for update failed: %v", err)
		}

		// 更新锁定的记录
		for _, user := range users {
			_, err = chain.From(&example.User{}).
				Set("age", user.Age+1).
				Where("id", "=", user.ID).
				Update()

			if err != nil {
				return fmt.Errorf("update failed: %v", err)
			}
		}

		return nil
	})
}

// 演示关联查询
func demonstrateJoinQueries(db *gom.DB) error {
	fmt.Println("\nDemonstrating join queries...")

	// 先创建一些用户配置文件
	for i := 1; i <= 2; i++ {
		profile := &example.UserProfile{
			UserID:    int64(i),
			Avatar:    fmt.Sprintf("/avatars/user%d.jpg", i),
			Bio:       fmt.Sprintf("Test user %d bio", i),
			Location:  "Test Location",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		_, err := db.Chain().From(profile).Save()
		if err != nil {
			return fmt.Errorf("create profile failed: %v", err)
		}
	}

	// 定义一个包含所需字段的结构体
	type UserWithProfile struct {
		ID        int64     `gom:"id"`
		Username  string    `gom:"username"`
		Email     string    `gom:"email"`
		Age       int       `gom:"age"`
		Active    bool      `gom:"active"`
		Role      string    `gom:"role"`
		CreatedAt time.Time `gom:"created_at"`
		UpdatedAt time.Time `gom:"updated_at"`
		Avatar    string    `gom:"avatar"`
		Bio       string    `gom:"bio"`
		Location  string    `gom:"location"`
	}

	query := `
		SELECT u.*, p.avatar, p.bio, p.location 
		FROM users u 
		LEFT JOIN user_profiles p ON u.id = p.user_id 
		WHERE u.age > ?
	`

	var results []UserWithProfile
	err := db.Chain().RawQuery(query, 25).Into(&results)
	if err != nil {
		return fmt.Errorf("join query failed: %v", err)
	}

	fmt.Printf("Found %d records with profile information\n", len(results))
	for _, r := range results {
		fmt.Printf("User: %s (Age: %d), Location: %s, Bio: %s\n",
			r.Username, r.Age, r.Location, r.Bio)
	}
	return nil
}

// 演示自增ID回填
func demonstrateAutoIncrementId(db *gom.DB) error {
	fmt.Println("\nDemonstrating auto-increment ID filling...")

	user := &example.User{
		Username:  "auto_id_test",
		Email:     "auto_id@example.com",
		Age:       25,
		Active:    true,
		Role:      "user",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	fmt.Printf("Before save - User ID: %d\n", user.ID)

	result, err := db.Chain().From(user).Save()
	if err != nil {
		return fmt.Errorf("save user failed: %v", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("get last insert id failed: %v", err)
	}

	fmt.Printf("After save - Result ID: %d, User ID: %d\n", id, user.ID)
	return nil
}

// 演示代码生成
func demonstrateCodeGeneration(db *gom.DB) error {
	fmt.Println("\nDemonstrating code generation...")

	// 创建目标目录
	err := os.MkdirAll("./generated", 0755)
	if err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// 生成单个表的结构体
	fmt.Println("Generating struct for 'users' table...")
	err = db.GenerateStruct("users", "./generated", "models")
	if err != nil {
		return fmt.Errorf("generate struct for users failed: %v", err)
	}

	// 生成所有以 user 开头的表的结构体
	fmt.Println("Generating structs for all tables starting with 'user'...")
	err = db.GenerateStructs(gom.GenerateOptions{
		OutputDir:   "./generated",
		PackageName: "models",
		Pattern:     "user*",
	})
	if err != nil {
		return fmt.Errorf("generate structs failed: %v", err)
	}

	fmt.Println("Code generation completed successfully")
	return nil
}

// 演示各种数据类型
func demonstrateDataTypes(db *gom.DB) error {
	fmt.Println("\nDemonstrating various data types...")

	// 删除并创建测试表
	_, err := db.Chain().RawExecute("DROP TABLE IF EXISTS test_types")
	if err != nil {
		return fmt.Errorf("drop table failed: %v", err)
	}

	_, err = db.Chain().RawExecute(`
		CREATE TABLE test_types (
			id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT 'Primary key',
			tiny_int_col TINYINT COMMENT 'Tiny integer column',
			small_int_col SMALLINT COMMENT 'Small integer column',
			int_col INT COMMENT 'Integer column',
			big_int_col BIGINT COMMENT 'Big integer column',
			float_col FLOAT COMMENT 'Float column',
			double_col DOUBLE COMMENT 'Double column',
			decimal_col DECIMAL(10,2) COMMENT 'Decimal column',
			varchar_col VARCHAR(255) COMMENT 'Varchar column',
			text_col TEXT COMMENT 'Text column',
			date_col DATE COMMENT 'Date column',
			datetime_col DATETIME COMMENT 'Datetime column',
			timestamp_col TIMESTAMP COMMENT 'Timestamp column',
			bool_col BOOLEAN COMMENT 'Boolean column',
			nullable_int INT NULL COMMENT 'Nullable integer column',
			nullable_string VARCHAR(255) NULL COMMENT 'Nullable string column',
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Creation time',
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time'
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Test table for various data types'
	`)
	if err != nil {
		return fmt.Errorf("create table failed: %v", err)
	}

	// 生成 test_types 表的结构体
	fmt.Println("Generating struct for 'test_types' table...")
	err = db.GenerateStruct("test_types", "./generated", "models")
	if err != nil {
		return fmt.Errorf("generate struct for test_types failed: %v", err)
	}

	fmt.Println("Code generation for test_types completed successfully")
	return nil
}

func main() {
	// 连接到MySQL
	db, err := gom.Open("mysql", "root:123456@tcp(192.168.110.249:3306)/test?parseTime=true", true)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// 清理并创建表
	fmt.Println("Setting up database...")
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS user_profiles")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS users")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS user_roles")
	if err != nil {
		log.Fatal(err)
	}

	// 创建表
	fmt.Println("Creating tables...")
	err = db.Chain().CreateTable(&example.UserRole{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created user_roles table")

	err = db.Chain().CreateTable(&example.User{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created users table")

	err = db.Chain().CreateTable(&example.UserProfile{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created user_profiles table")

	// 演示批量插入
	if err := demonstrateBatchInsert(db); err != nil {
		log.Fatal(err)
	}

	// 演示复杂查询
	if err := demonstrateComplexQuery(db); err != nil {
		log.Fatal(err)
	}

	// 演示事务和锁
	if err := demonstrateTransactionAndLocking(db); err != nil {
		log.Fatal(err)
	}

	// 演示关联查询
	if err := demonstrateJoinQueries(db); err != nil {
		log.Fatal(err)
	}

	// 演示自增ID回填
	if err := demonstrateAutoIncrementId(db); err != nil {
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

	fmt.Println("\nAll MySQL examples completed successfully!")
}
