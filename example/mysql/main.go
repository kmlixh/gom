package main

import (
	"fmt"
	"github.com/kmlixh/gom/v4"
	"log"
	"time"

	"github.com/kmlixh/gom/v4/define"
	_ "github.com/kmlixh/gom/v4/factory/mysql"
)

type User struct {
	ID        int64     `gom:"id"`
	Name      string    `gom:"name"`
	Email     string    `gom:"email"`
	Age       int       `gom:"age"`
	CreatedAt time.Time `gom:"created_at"`
}

func main() {
	// 连接数据库
	db, err := gom.Open("mysql", "root:123456@tcp(192.168.110.249:3306)/test?charset=utf8mb4&parseTime=True", true)
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
	// 单条插入
	user := &User{
		Name:      "John Doe",
		Email:     "john@example.com",
		Age:       30,
		CreatedAt: time.Now(),
	}
	result, err := db.Insert("users").Model(user).Execute()
	if err != nil {
		log.Printf("Insert error: %v\n", err)
		return
	}
	id, _ := result.LastInsertId()
	fmt.Printf("Inserted user with ID: %d\n", id)

	// 指定字段插入
	_, err = db.Insert("users").
		Fields("name", "email").
		Values("Jane Doe", "jane@example.com").
		Execute()
	if err != nil {
		log.Printf("Insert error: %v\n", err)
		return
	}
}

func queryExample(db *gom.DB) {
	// 单条查询
	var user User
	err := db.Query("users").
		Where("id = ?", 1).
		IntoOne(&user)
	if err != nil {
		log.Printf("Query error: %v\n", err)
		return
	}
	fmt.Printf("Found user: %+v\n", user)

	// 条件查询
	var users []User
	err = db.Query("users").
		Fields("id", "name", "email").
		Where("age > ?", 25).
		OrderBy("id DESC").
		Limit(10).
		Into(&users)
	if err != nil {
		log.Printf("Query error: %v\n", err)
		return
	}

	// 聚合查询
	count, err := db.Query("users").
		Where("age > ?", 25).
		Count()
	if err != nil {
		log.Printf("Count error: %v\n", err)
		return
	}
	fmt.Printf("Found %d users\n", count)

	// 基本链式条件查询
	err = db.Query("users").
		Eq("age", 30).
		Like("name", "John").
		OrderBy("id DESC").
		Into(&users)
	if err != nil {
		log.Printf("Query error: %v\n", err)
		return
	}

	// 复杂AND/OR条件组合
	err = db.Query("users").
		Group(define.TypeAnd,
			&define.Condition{Field: "age", Operator: define.Gt, Value: 20},
			&define.Condition{Field: "age", Operator: define.Lt, Value: 30},
		).
		Group(define.TypeOr,
			&define.Condition{Field: "name", Operator: define.Like, Value: "John"},
			&define.Condition{Field: "name", Operator: define.Like, Value: "Jane"},
		).
		OrderBy("id DESC").
		Into(&users)
	if err != nil {
		log.Printf("Complex query error: %v\n", err)
		return
	}

	// 嵌套条件查询
	subCond1 := &define.Condition{
		Type: define.TypeAnd,
		SubConds: []*define.Condition{
			{Field: "age", Operator: define.Gte, Value: 20},
			{Field: "age", Operator: define.Lte, Value: 30},
		},
	}
	subCond2 := &define.Condition{
		Type: define.TypeOr,
		SubConds: []*define.Condition{
			{Field: "email", Operator: define.Like, Value: "@gmail.com"},
			{Field: "email", Operator: define.Like, Value: "@yahoo.com"},
		},
	}
	err = db.Query("users").
		Group(define.TypeAnd, subCond1, subCond2).
		OrderBy("created_at DESC").
		Into(&users)
	if err != nil {
		log.Printf("Nested query error: %v\n", err)
		return
	}

	// 范围条件查询
	err = db.Query("users").
		Between("age", 20, 30).
		In("status", []interface{}{"active", "pending"}).
		NotIn("role", []interface{}{"admin", "guest"}).
		OrderBy("id DESC").
		Into(&users)
	if err != nil {
		log.Printf("Range query error: %v\n", err)
		return
	}

	// 组合多个条件组
	cond1 := define.NewAndCondition(
		&define.Condition{Field: "age", Operator: define.Gt, Value: 18},
		&define.Condition{Field: "status", Operator: define.Eq, Value: "active"},
	)
	cond2 := define.NewOrCondition(
		&define.Condition{Field: "role", Operator: define.Eq, Value: "user"},
		&define.Condition{Field: "role", Operator: define.Eq, Value: "admin"},
	)
	err = db.Query("users").
		Group(define.TypeAnd, cond1, cond2).
		OrderBy("created_at DESC").
		Into(&users)
	if err != nil {
		log.Printf("Combined group query error: %v\n", err)
		return
	}
}

func updateExample(db *gom.DB) {
	// 单条更新
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

	// 使用模型更新
	user := &User{
		ID:    1,
		Name:  "John Smith",
		Email: "john.smith@example.com",
		Age:   32,
	}
	_, err = db.Update("users").
		Model(user).
		Where("id = ?", user.ID).
		Execute()
	if err != nil {
		log.Printf("Update error: %v\n", err)
		return
	}
}

func deleteExample(db *gom.DB) {
	// 单条删除
	result, err := db.Delete("users").
		Where("id = ?", 1).
		Execute()
	if err != nil {
		log.Printf("Delete error: %v\n", err)
		return
	}
	rows, _ := result.RowsAffected()
	fmt.Printf("Deleted %d rows\n", rows)

	// 条件删除
	result, err = db.Delete("users").
		Where("age < ?", 20).
		Execute()
	if err != nil {
		log.Printf("Delete error: %v\n", err)
		return
	}
}

func transactionExample(db *gom.DB) {
	err := db.WithTransaction(func(tx *gom.DB) error {
		// 在事务中执行插入
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
	// 批量插入
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
	// 分页查询
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

	// 带条件的分页查询
	pageResult, err = db.Query("users").
		Where("age > ?", 20).
		OrderBy("created_at DESC").
		PageInto(2, 10, &users)
	if err != nil {
		log.Printf("Pagination error: %v\n", err)
		return
	}
}
