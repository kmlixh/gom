package main

import (
	"database/sql"
	"fmt"
	"log"
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
	result, err := db.Chain().From(queryModel).
		OrderBy("age DESC").
		Page(1, 10).
		List()

	if err != nil {
		return fmt.Errorf("complex query failed: %v", err)
	}

	err = result.Into(&users)
	if err != nil {
		return fmt.Errorf("scanning results failed: %v", err)
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
		result, err := chain.RawQuery("SELECT * FROM users WHERE age > ? FOR UPDATE", 30)
		if err != nil {
			return fmt.Errorf("select for update failed: %v", err)
		}

		err = result.Into(&users)
		if err != nil {
			return fmt.Errorf("scanning results failed: %v", err)
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

	query := `
		SELECT u.*, p.avatar, p.bio, p.location 
		FROM users u 
		LEFT JOIN user_profiles p ON u.id = p.user_id 
		WHERE u.age > ?
	`

	result, err := db.Chain().RawQuery(query, 25)
	if err != nil {
		return fmt.Errorf("join query failed: %v", err)
	}

	fmt.Printf("Found %d records with profile information\n", len(result.Data))
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

	// 生成单个表的结构体
	fmt.Println("Generating struct for 'users' table...")
	err := db.GenerateStruct("users", "./generated", "models")
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

	fmt.Println("\nAll MySQL examples completed successfully!")
}
