package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/define"
	_ "github.com/kmlixh/gom/v4/factory/postgres"
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

// Product represents a product in the system
type Product struct {
	ID          int64     `gom:"id,@,auto"`
	Name        string    `gom:"name,notnull"`
	Description string    `gom:"description"`
	Price       float64   `gom:"price,notnull"`
	Stock       int       `gom:"stock,notnull"`
	Category    string    `gom:"category,notnull"`
	CreatedAt   time.Time `gom:"created_at,notnull,default"`
	UpdatedAt   time.Time `gom:"updated_at,notnull,default"`
}

// TableName returns the table name for Product
func (p *Product) TableName() string {
	return "products"
}

// Order represents an order in the system
type Order struct {
	ID        int64     `gom:"id,@,auto"`
	UserID    int64     `gom:"user_id,notnull"`
	ProductID int64     `gom:"product_id,notnull"`
	Quantity  int       `gom:"quantity,notnull"`
	Status    string    `gom:"status,notnull,default"`
	CreatedAt time.Time `gom:"created_at,notnull,default"`
	UpdatedAt time.Time `gom:"updated_at,notnull,default"`
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

	db, err := gom.Open("postgres", "host=localhost port=5432 user=postgres password=123456 dbname=test sslmode=disable", opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 2. 创建表
	for _, model := range []interface{}{&User{}, &Product{}, &Order{}} {
		err = db.Chain().CreateTable(model)
		if err != nil {
			log.Fatal(err)
		}
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

		// 3.2 创建产品
		product := &Product{
			Name:        "PostgreSQL Guide",
			Description: "Complete guide for PostgreSQL",
			Price:       49.99,
			Stock:       100,
			Category:    "books",
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}

		result = tx.Table("products").From(product).Save()
		if result.Error != nil {
			return result.Error
		}
		productID := result.ID

		// 3.3 创建订单
		order := &Order{
			UserID:    userID,
			ProductID: productID,
			Quantity:  1,
			Status:    "pending",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
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
	// 4.1 批量插入产品
	var products []map[string]interface{}
	for i := 0; i < 50; i++ {
		products = append(products, map[string]interface{}{
			"name":        fmt.Sprintf("Product %d", i),
			"description": fmt.Sprintf("Description for product %d", i),
			"price":       float64(10 + i),
			"stock":       100,
			"category":    fmt.Sprintf("category_%d", i%5),
			"created_at":  time.Now(),
			"updated_at":  time.Now(),
		})
	}

	chain := db.Chain().Table("products").BatchValues(products)
	affected, err := chain.BatchInsert(10)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Batch inserted %d products\n", affected)

	// 5. 演示高级查询
	// 5.1 使用 CTE (Common Table Expression)
	result := db.Chain().RawQuery(`
		WITH ranked_products AS (
			SELECT 
				p.*,
				RANK() OVER (PARTITION BY category ORDER BY price DESC) as price_rank
			FROM products p
		)
		SELECT *
		FROM ranked_products
		WHERE price_rank = 1
	`)

	if result.Error != nil {
		log.Fatal(result.Error)
	}

	fmt.Println("\nMost expensive products in each category:")
	for _, r := range result.Data {
		fmt.Printf("Category: %s, Product: %s, Price: %.2f\n",
			r["category"], r["name"], r["price"])
	}

	// 5.2 复杂聚合查询
	result = db.Chain().RawQuery(`
		SELECT 
			u.role,
			COUNT(DISTINCT u.id) as user_count,
			COUNT(DISTINCT o.id) as order_count,
			COALESCE(SUM(p.price * o.quantity), 0) as total_amount
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		LEFT JOIN products p ON o.product_id = p.id
		GROUP BY u.role
	`)

	if result.Error != nil {
		log.Fatal(result.Error)
	}

	fmt.Println("\nSales statistics by user role:")
	for _, r := range result.Data {
		fmt.Printf("Role: %s, Users: %v, Orders: %v, Total Amount: %.2f\n",
			r["role"], r["user_count"], r["order_count"], r["total_amount"])
	}

	// 5.3 全文搜索
	result = db.Chain().RawQuery(`
		SELECT 
			id, name, description,
			ts_rank(
				to_tsvector('english', name || ' ' || description),
				to_tsquery('english', 'guide | complete')
			) as rank
		FROM products
		WHERE to_tsvector('english', name || ' ' || description) @@ to_tsquery('english', 'guide | complete')
		ORDER BY rank DESC
	`)

	if result.Error != nil {
		log.Fatal(result.Error)
	}

	fmt.Println("\nFull-text search results:")
	for _, r := range result.Data {
		fmt.Printf("Product: %s, Rank: %v\n", r["name"], r["rank"])
	}

	// 5.4 窗口函数
	result = db.Chain().RawQuery(`
		SELECT 
			date_trunc('month', created_at) as month,
			category,
			COUNT(*) as monthly_sales,
			SUM(COUNT(*)) OVER (PARTITION BY category ORDER BY date_trunc('month', created_at)) as cumulative_sales
		FROM orders o
		JOIN products p ON o.product_id = p.id
		GROUP BY date_trunc('month', created_at), category
		ORDER BY month, category
	`)

	if result.Error != nil {
		log.Fatal(result.Error)
	}

	fmt.Println("\nMonthly and cumulative sales by category:")
	for _, r := range result.Data {
		fmt.Printf("Month: %v, Category: %s, Monthly: %v, Cumulative: %v\n",
			r["month"], r["category"], r["monthly_sales"], r["cumulative_sales"])
	}
}
