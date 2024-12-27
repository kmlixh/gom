package main

import (
	"fmt"
	"log"
	"time"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/example"
	_ "github.com/kmlixh/gom/v4/factory/mysql"
)

func main() {
	// Connect to MySQL
	db, err := gom.Open("mysql", "root:password@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=True&loc=Local", true)
	if err != nil {
		log.Fatal(err)
	}
	defer db.DB.Close()

	// Create tables
	chain := db.Chain()
	err = chain.CreateTable(&example.User{})
	if err != nil {
		log.Fatal(err)
	}

	// Insert a user
	user := &example.User{
		Username:  "john_doe",
		Email:     "john@example.com",
		Age:       25,
		Active:    true,
		Role:      "admin",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	result, err := chain.Table("users").From(user).Save()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Inserted user with ID: %d\n", result.ID)

	// Basic conditions
	var users []example.User
	err = chain.Table("users").
		Eq("active", true).
		Gt("age", 20).
		Like("username", "%john%").
		Into(&users)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d active users over 20 with 'john' in username\n", len(users))

	// OR conditions
	err = chain.Table("users").
		Eq("role", "admin").
		OrEq("role", "manager").
		OrGt("age", 30).
		Into(&users)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d users who are admins, managers, or over 30\n", len(users))

	// Complex conditions using Where2 with timestamp
	adminCondition := define.Eq("role", "admin").
		And(define.Gt("age", 25)).
		And(define.Gt("created_at", time.Now().AddDate(0, -1, 0))) // Created in the last month

	managerCondition := define.Eq("role", "manager").
		And(define.Between("age", 20, 30))

	err = chain.Table("users").
		Where2(adminCondition.Or(managerCondition)).
		Into(&users)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d users matching complex conditions\n", len(users))

	// Using array operations with mixed arrays
	adminRoles := []string{"admin", "superadmin"}
	managerRoles := []string{"manager", "supervisor"}
	extraRoles := []interface{}{"leader", "director"}
	err = chain.Table("users").
		Where2(define.In("role", adminRoles, managerRoles, extraRoles)).
		IsNotNull("email").
		OrderBy("created_at").
		Limit(10).
		Into(&users)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d users with specific roles\n", len(users))

	// Update with multiple conditions and NOT IN using arrays
	restrictedRoles := []string{"admin", "superadmin"}
	moreRestricted := []interface{}{"manager", "supervisor"}
	updateResult, err := chain.Table("users").
		Where2(define.Eq("active", true).
			And(define.NotIn("role", restrictedRoles, moreRestricted))).
		Set("role", "user").
		Set("updated_at", time.Now()).
		Save()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Updated %d users\n", updateResult.Affected)

	// Delete with conditions using numeric arrays
	youngAges := []int{13, 14, 15, 16, 17}
	sqlResult, err := chain.Table("users").
		Where2(define.In("age", youngAges)).
		OrIsNull("email").
		Delete()
	if err != nil {
		log.Fatal(err)
	}
	affected, _ := sqlResult.RowsAffected()
	fmt.Printf("Deleted %d users\n", affected)

	// Transaction example with savepoints
	err = chain.Transaction(func(tx *gom.Chain) error {
		// Create a savepoint
		err := tx.Savepoint("before_updates")
		if err != nil {
			return err
		}

		// Complex update within transaction
		adminUser := define.Eq("role", "admin").
			And(define.Gt("age", 30))

		_, err = tx.Table("users").
			Where2(adminUser).
			Set("active", false).
			Save()
		if err != nil {
			tx.RollbackTo("before_updates")
			return err
		}

		// Create another savepoint
		err = tx.Savepoint("after_admin_update")
		if err != nil {
			return err
		}

		// Another operation in the same transaction
		_, err = tx.Table("users").
			Eq("active", false).
			Set("updated_at", time.Now()).
			Save()
		if err != nil {
			tx.RollbackTo("after_admin_update")
			return err
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// 统计示例
	fmt.Println("\n=== 统计示例 ===")

	// 计算用户总数
	totalUsers, err := chain.Table("users").Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("总用户数: %d\n", totalUsers)

	// 计算活跃用户的平均年龄
	avgAge, err := chain.Table("users").
		Eq("active", true).
		Avg("age")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("活跃用户平均年龄: %.2f\n", avgAge)

	// 计算管理员的年龄总和
	sumAge, err := chain.Table("users").
		Eq("role", "admin").
		Sum("age")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("管理员年龄总和: %.0f\n", sumAge)

	// 复杂条件的统计
	activeAdminCount, err := chain.Table("users").
		Where2(define.Eq("active", true).
			And(define.In("role", []string{"admin", "superadmin"}))).
		Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("活跃管理员数量: %d\n", activeAdminCount)

	// 分页查询示例
	fmt.Println("\n=== 分页查询示例 ===")

	// 使用模型的分页查询
	pageInfo, err := chain.Table("users").
		Eq("active", true).
		OrderBy("created_at").
		Page(1, 10). // 第1页，每页10条
		PageInfo(&example.User{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("总记录数: %d\n", pageInfo.Total)
	fmt.Printf("总页数: %d\n", pageInfo.Pages)
	fmt.Printf("当前页: %d\n", pageInfo.PageNum)
	fmt.Printf("每页大小: %d\n", pageInfo.PageSize)
	fmt.Printf("是否有上一页: %v\n", pageInfo.HasPrev)
	fmt.Printf("是否有下一页: %v\n", pageInfo.HasNext)

	// 类型断言获取用户列表
	if users, ok := pageInfo.List.([]example.User); ok {
		for _, user := range users {
			fmt.Printf("用户ID: %d, 用户名: %s\n", user.ID, user.Username)
		}
	}

	// 不使用模型的分页查询（返回原始数据）
	rawPageInfo, err := chain.Table("users").
		Fields("id", "username", "email").
		Page(2, 5). // 第2页，每页5条
		PageInfo(nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\n原始数据分页查询结果:\n")
	fmt.Printf("总记录数: %d\n", rawPageInfo.Total)
	fmt.Printf("当前页数据:\n")
	for _, item := range rawPageInfo.List.([]map[string]interface{}) {
		fmt.Printf("ID: %v, 用户名: %v\n", item["id"], item["username"])
	}
}
