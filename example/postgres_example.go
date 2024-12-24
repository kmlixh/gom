package main

import (
	"fmt"
	"log"

	"github.com/kmlixh/gom"
	// Import PostgreSQL factory for automatic registration
	_ "github.com/kmlixh/gom/factory/postgres"
)

func main_postgres() {
	// Connect to PostgreSQL database and create ORM instance in one line
	// Example DSN: "postgres://username:password@localhost:5432/dbname?sslmode=disable"
	orm, err := gom.Connect("postgres", "postgres://postgres:postgres@localhost:5432/test?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer orm.Close()

	// Create table
	createTable := `
		CREATE TABLE IF NOT EXISTS users (
			id BIGSERIAL PRIMARY KEY,
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
		Where("name = $1", "John").
		First()
	if err != nil {
		log.Fatal(err)
	}

	if result.Empty() {
		fmt.Println("No user found, inserting new user...")
		// Insert a new user
		_, err = orm.Execute("INSERT INTO users (name, email, age) VALUES ($1, $2, $3)", "John", "john@example.com", 30)
		if err != nil {
			orm.Rollback()
			log.Fatal(err)
		}
	}

	// Query with chain operations
	result, err = orm.StartQuery("users").
		Select("name", "email", "age").
		Where("age >= $1 AND age <= $2", 25, 35).
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
		Where("age >= $1", 25).
		Count()
	if err != nil {
		orm.Rollback()
		log.Fatal(err)
	}
	fmt.Printf("Total users age >= 25: %d\n", count)

	// Check if any users exist
	exists, err := orm.StartQuery("users").
		Where("age > $1", 40).
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
