package main

import (
	"fmt"
	"log"

	"github.com/kmlixh/gom"
	// Import MySQL factory for automatic registration
	_ "github.com/kmlixh/gom/factory/mysql"
)

type User struct {
	ID      int64  `gom:"id"`
	Name    string `gom:"name"`
	Email   string `gom:"email"`
	Age     int    `gom:"age"`
	Created string `gom:"created"`
}

func main() {
	// Connect to MySQL database and create ORM instance in one line
	orm, err := gom.Connect("mysql", "root:123456@tcp(192.168.110.249:3306)/test?parseTime=true")
	if err != nil {
		log.Fatal(err)
	}
	defer orm.Close()

	// Create table
	createTable := `
		CREATE TABLE IF NOT EXISTS users (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL,
			age INT NOT NULL,
			created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`
	_, err = orm.Execute(createTable)
	if err != nil {
		log.Fatal(err)
	}

	// Begin transaction
	err = orm.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Insert some test data
	result, err := orm.StartQuery("users").
		Select("name", "email", "age").
		Where("name = ?", "John").
		First()
	if err != nil {
		log.Fatal(err)
	}

	if result.Empty() {
		fmt.Println("No user found, inserting new user...")
		// Insert a new user
		_, err = orm.Execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", "John", "john@example.com", 30)
		if err != nil {
			orm.Rollback()
			log.Fatal(err)
		}
	}

	// Query with chain operations
	result, err = orm.StartQuery("users").
		Select("name", "email", "age").
		Where("age >= ? AND age <= ?", 25, 35).
		OrderByDesc("created").
		Limit(10).
		Offset(0).
		List()
	if err != nil {
		orm.Rollback()
		log.Fatal(err)
	}

	// Print results
	var users []User
	err = result.Into(&users)
	if err != nil {
		orm.Rollback()
		log.Fatal(err)
	}

	fmt.Println("Users found:", result.Size())
	for _, user := range users {
		fmt.Printf("ID: %d, Name: %s, Email: %s, Age: %d\n", user.ID, user.Name, user.Email, user.Age)
	}

	// Count users
	count, err := orm.StartQuery("users").
		Where("age >= ?", 25).
		Count()
	if err != nil {
		orm.Rollback()
		log.Fatal(err)
	}
	fmt.Printf("Total users age >= 25: %d\n", count)

	// Check if any users exist
	exists, err := orm.StartQuery("users").
		Where("age > ?", 40).
		Exists()
	if err != nil {
		orm.Rollback()
		log.Fatal(err)
	}
	fmt.Printf("Users with age > 40 exist: %v\n", exists)

	// Commit transaction
	err = orm.Commit()
	if err != nil {
		log.Fatal(err)
	}
}
