package main

import (
	"fmt"
	"github.com/kmlixh/gom/v4"
	"log"
	"time"

	_ "github.com/kmlixh/gom/v4/factory/postgres"
)

type User struct {
	ID        int64     `gom:"id"`
	Name      string    `gom:"name"`
	Email     string    `gom:"email"`
	Age       int       `gom:"age"`
	CreatedAt time.Time `gom:"created_at"`
}

func main() {
	// 连接数据库，启用调试模式
	db, err := gom.Open("postgres", "host=192.168.110.249 port=5432 user=postgres password=yzy123 dbname=test sslmode=disable", true)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 插入示例
	insertExample(db)

	// 查询示例
	queryExample(db)

	// 更新示例
	updateExample(db)

	// 删除示例
	deleteExample(db)

	// 事务示例
	transactionExample(db)

	// 批量操作示例
	batchExample(db)

	// 分页查询示例
	paginationExample(db)
}

func insertExample(db *gom.DB) {
	// 单条插入，PostgreSQL返回自增ID
	user := &User{
		Name:      "John Doe",
		Email:     "john@example.com",
		Age:       30,
		CreatedAt: time.Now(),
	}
	result, err := db.Insert("users").
		Model(user).
		Execute()
	if err != nil {
		log.Printf("Insert error: %v\n", err)
		return
	}
	id, _ := result.LastInsertId()
	fmt.Printf("Inserted user with ID: %d\n", id)

	// 使用RETURNING子句
	var insertedID int64
	err = db.Query("users").
		Fields("id").
		Where("email = ?", "john@example.com").
		IntoOne(&insertedID)
	if err != nil {
		log.Printf("Query error: %v\n", err)
		return
	}
}

func queryExample(db *gom.DB) {
	// 使用PostgreSQL特有的ILIKE进行不区分大小写的搜索
	var users []User
	err := db.Query("users").
		Where("email LIKE ?", "%@example.com").
		Into(&users)
	if err != nil {
		log.Printf("Query error: %v\n", err)
		return
	}

	// 使用PostgreSQL的区间类型
	err = db.Query("users").
		Where("age <@ int4range(?, ?)", 20, 30).
		Into(&users)
	if err != nil {
		log.Printf("Query error: %v\n", err)
		return
	}

	// 使用PostgreSQL的JSON操作
	err = db.Query("users").
		Where("data->>'status' = ?", "active").
		Into(&users)
	if err != nil {
		log.Printf("Query error: %v\n", err)
		return
	}
}

func updateExample(db *gom.DB) {
	// 使用RETURNING子句的更新
	result, err := db.Update("users").
		Fields("name", "age").
		Values("John Smith", 31).
		Where("id = ?", 1).
		Execute()
	if err != nil {
		log.Printf("Update error: %v\n", err)
		return
	}
	rows, _ := result.RowsAffected()
	fmt.Printf("Updated %d rows\n", rows)

	// 更新JSON字段
	result, err = db.Update("users").
		Fields("data").
		Values(`{"status": "inactive", "last_login": "2023-01-01"}`).
		Where("id = ?", 1).
		Execute()
	if err != nil {
		log.Printf("Update error: %v\n", err)
		return
	}
}

func deleteExample(db *gom.DB) {
	// 使用USING子句的删除
	result, err := db.Delete("users").
		Where("age < ?", 20).
		Execute()
	if err != nil {
		log.Printf("Delete error: %v\n", err)
		return
	}
	rows, _ := result.RowsAffected()
	fmt.Printf("Deleted %d rows\n", rows)

	// 使用CASCADE的删除
	result, err = db.Delete("users").
		Where("id = ?", 1).
		Execute()
	if err != nil {
		log.Printf("Delete error: %v\n", err)
		return
	}
}

func transactionExample(db *gom.DB) {
	err := db.WithTransaction(func(tx *gom.DB) error {
		// 在事务中使用SAVEPOINT
		user := &User{
			Name:      "Transaction User",
			Email:     "tx@example.com",
			Age:       25,
			CreatedAt: time.Now(),
		}
		_, err := tx.Insert("users").Model(user).Execute()
		if err != nil {
			return err
		}

		// 在事务中执行更新
		_, err = tx.Update("users").
			Fields("age").
			Values(26).
			Where("email = ?", "tx@example.com").
			Execute()
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		log.Printf("Transaction error: %v\n", err)
		return
	}
	fmt.Println("Transaction completed successfully")
}

func batchExample(db *gom.DB) {
	// 使用COPY进行批量插入
	users := []User{
		{Name: "User1", Email: "user1@example.com", Age: 21},
		{Name: "User2", Email: "user2@example.com", Age: 22},
		{Name: "User3", Email: "user3@example.com", Age: 23},
	}
	result, err := db.Insert("users").Models(users).Execute()
	if err != nil {
		log.Printf("Batch insert error: %v\n", err)
		return
	}
	rows, _ := result.RowsAffected()
	fmt.Printf("Batch inserted %d rows\n", rows)

	// 批量更新
	result, err = db.Update("users").
		Fields("age").
		Values(25).
		Where("age < ?", 23).
		Execute()
	if err != nil {
		log.Printf("Batch update error: %v\n", err)
		return
	}
}

func paginationExample(db *gom.DB) {
	// 使用OFFSET FETCH进行分页
	var users []User
	pageResult, err := db.Query("users").
		OrderBy("id").
		PageInto(1, 10, &users)
	if err != nil {
		log.Printf("Pagination error: %v\n", err)
		return
	}

	fmt.Printf("Page %d of %d, Total: %d\n",
		pageResult.PageNumber,
		pageResult.TotalPages,
		pageResult.Total)

	// 使用窗口函数的分页
	err = db.Query("users").
		Fields("*", "ROW_NUMBER() OVER(ORDER BY id) AS row_num").
		Where("age > $1", 20).
		OrderBy("created_at DESC").
		Into(&users)
	if err != nil {
		log.Printf("Query error: %v\n", err)
		return
	}
}
