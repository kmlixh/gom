package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/define"
	_ "github.com/kmlixh/gom/v4/factory/mysql"
)

// User represents a user in the system
type User struct {
	ID        int64     `gom:"id,@,auto"`
	Username  string    `gom:"username,notnull"`
	Password  string    `gom:"password,notnull"`
	Email     string    `gom:"email,notnull"`
	Phone     string    `gom:"phone"`
	Age       int       `gom:"age"`
	Active    bool      `gom:"active,notnull,default"`
	Role      string    `gom:"role,notnull,default"`
	CreatedAt time.Time `gom:"created_at,notnull,default"`
	UpdatedAt time.Time `gom:"updated_at,notnull,default"`
}

// TableName returns the table name for User
func (u *User) TableName() string {
	return "users"
}

// Order represents an order in the system
type Order struct {
	ID          int64     `gom:"id,@,auto"`
	UserID      int64     `gom:"user_id,notnull"`
	OrderNumber string    `gom:"order_number,notnull"`
	Amount      float64   `gom:"amount,notnull"`
	Status      string    `gom:"status,notnull,default"`
	CreatedAt   time.Time `gom:"created_at,notnull,default"`
	UpdatedAt   time.Time `gom:"updated_at,notnull,default"`
}

// TableName returns the table name for Order
func (o *Order) TableName() string {
	return "orders"
}

func main() {
	// 1. 初始化数据库连接
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Minute,
		ConnMaxIdleTime: 30 * time.Second,
		Debug:           true,
	}

	db, err := gom.Open("mysql", "root:123456@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=true", opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 2. 创建表
	err = db.Chain().CreateTable(&User{})
	if err != nil {
		log.Fatal(err)
	}
	err = db.Chain().CreateTable(&Order{})
	if err != nil {
		log.Fatal(err)
	}

	// 3. 演示事务和敏感数据处理
	err = db.Chain().TransactionWithOptions(define.TransactionOptions{
		Timeout:         time.Second * 10,
		IsolationLevel:  define.IsolationLevel(sql.LevelReadCommitted),
		PropagationMode: define.PropagationRequired,
	}, func(tx *gom.Chain) error {
		// 3.1 插入用户数据（带敏感信息处理）
		user := &User{
			Username:  "john_doe",
			Password:  "password123",
			Email:     "john@example.com",
			Phone:     "13800138000",
			Age:       25,
			Active:    true,
			Role:      "user",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		chain := tx.Table("users").
			AddSensitiveField("password", gom.SensitiveOptions{
				Type: gom.SensitiveEncrypted,
				Encryption: &gom.EncryptionConfig{
					Algorithm: gom.AES256,
					KeySource: gom.KeySourceEnv,
					KeySourceConfig: map[string]string{
						"key_name": "ENCRYPTION_KEY",
					},
				},
			}).
			AddSensitiveField("phone", gom.SensitiveOptions{
				Type: gom.SensitivePhone,
			}).
			AddSensitiveField("email", gom.SensitiveOptions{
				Type: gom.SensitiveEmail,
			})

		result := chain.From(user).Save()
		if result.Error != nil {
			return result.Error
		}
		userID := result.ID

		// 3.2 插入订单数据
		order := &Order{
			UserID:      userID,
			OrderNumber: fmt.Sprintf("ORD%d", time.Now().Unix()),
			Amount:      99.99,
			Status:      "pending",
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}

		result = tx.Table("orders").From(order).Save()
		if result.Error != nil {
			return result.Error
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	// 4. 演示批量操作
	// 4.1 批量插入
	var users []map[string]interface{}
	for i := 0; i < 100; i++ {
		users = append(users, map[string]interface{}{
			"username":   fmt.Sprintf("user_%d", i),
			"password":   fmt.Sprintf("pass_%d", i),
			"email":      fmt.Sprintf("user%d@example.com", i),
			"age":        20 + (i % 20),
			"active":     true,
			"role":       "user",
			"created_at": time.Now(),
			"updated_at": time.Now(),
		})
	}

	chain := db.Chain().Table("users").BatchValues(users)
	affected, err := chain.BatchInsert(10)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Batch inserted %d users\n", affected)

	// 4.2 批量更新
	var updates []map[string]interface{}
	for i := 0; i < 10; i++ {
		updates = append(updates, map[string]interface{}{
			"id":     int64(i + 1),
			"active": false,
		})
	}

	chain = db.Chain().Table("users").BatchValues(updates)
	affected, err = chain.BatchUpdate(5)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Batch updated %d users\n", affected)

	// 5. 演示复杂查询
	// 5.1 分页查询
	var queryUsers []User
	pageInfo, err := db.Chain().
		Table("users").
		Where("age", define.OpGe, 25).
		OrderBy("created_at").
		Page(1, 10).
		PageInfo(&queryUsers)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d users, total pages: %d\n", len(queryUsers), pageInfo.Pages)

	// 5.2 聚合查询
	var result *gom.QueryResult
	result = db.Chain().
		Table("users").
		Where("active", define.OpEq, true).
		Fields("AVG(age) as avg_age").
		List()

	if result.Error() != nil {
		log.Fatal(result.Error())
	}

	if !result.Empty() {
		avgAge := result.Data[0]["avg_age"]
		fmt.Printf("Average age of active users: %v\n", avgAge)
	}

	// 5.3 分组统计
	result = db.Chain().
		Table("users").
		Fields("role", "COUNT(*) as count", "AVG(age) as avg_age").
		GroupBy("role").
		Having("COUNT(*) > ?", 1).
		List()

	if result.Error() != nil {
		log.Fatal(result.Error())
	}

	for _, r := range result.Data {
		fmt.Printf("Role: %s, Count: %v, Avg Age: %v\n", r["role"], r["count"], r["avg_age"])
	}

	// 5.4 关联查询
	result = db.Chain().
		Table("users u").
		Fields("u.username", "o.order_number", "o.amount", "o.status").
		Where("u.id", define.OpEq, 1).
		RawQuery(`
			SELECT u.username, o.order_number, o.amount, o.status
			FROM users u
			LEFT JOIN orders o ON u.id = o.user_id
			WHERE u.id = ?
		`, 1)

	if result.Error() != nil {
		log.Fatal(result.Error())
	}

	for _, r := range result.Data {
		fmt.Printf("User: %s, Order: %s, Amount: %v, Status: %s\n",
			r["username"], r["order_number"], r["amount"], r["status"])
	}
}
