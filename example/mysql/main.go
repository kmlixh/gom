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
	newUser := &example.User{
		Username:  "john_doe",
		Email:     "john@example.com",
		Age:       25,
		Active:    true,
		Role:      "admin",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	result := chain.Table("users").From(newUser).Save()
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	fmt.Printf("Inserted user with ID: %d\n", result.ID)

	// Basic conditions
	var queryUsers []example.User
	err = chain.Table("users").
		Eq("active", true).
		Gt("age", 20).
		Into(&queryUsers)
	if err != nil {
		log.Fatal(err)
	}

	// Complex conditions
	restrictedRoles := []string{"admin", "superuser"}
	moreRestricted := []string{"owner"}
	result = chain.Table("users").
		Where2(define.Eq("active", true).
			And(define.NotIn("role", restrictedRoles, moreRestricted))).
		Set("role", "user").
		Set("updated_at", time.Now()).
		Save()
	if result.Error != nil {
		log.Fatal(result.Error)
	}

	// OR conditions
	youngAges := []int{18, 19, 20}
	result = chain.Table("users").
		Where2(define.In("age", youngAges)).
		OrIsNull("email").
		Delete()
	if result.Error != nil {
		log.Fatal(result.Error)
	}

	// Transaction example
	err = chain.Transaction(func(tx *gom.Chain) error {
		// Update admin user
		adminUser := define.Eq("role", "admin").And(define.Eq("active", true))
		result := tx.Table("users").Where2(adminUser).Set("active", false).Save()
		if result.Error != nil {
			return result.Error
		}

		// Update inactive users
		var inactiveUsers []example.User
		err := tx.Table("users").
			Eq("active", false).
			Into(&inactiveUsers)
		if err != nil {
			return err
		}

		// Update all inactive users at once
		result = tx.Table("users").Eq("active", false).Set("updated_at", time.Now()).Save()
		if result.Error != nil {
			return result.Error
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Batch operations
	batchUsers := []interface{}{
		&example.User{
			Username:  "user1",
			Email:     "user1@example.com",
			Age:       30,
			Active:    true,
			Role:      "user",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		&example.User{
			Username:  "user2",
			Email:     "user2@example.com",
			Age:       35,
			Active:    true,
			Role:      "user",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
	}

	result = chain.Table("users").Save(batchUsers...)
	if result.Error != nil {
		log.Fatal(result.Error)
	}

	// Update multiple records
	result = chain.Table("users").Eq("username", "john_doe").Update()
	if result.Error != nil {
		log.Fatal(result.Error)
	}

	// Raw SQL
	var rawUsers []example.User
	qr := chain.RawQuery("SELECT * FROM users WHERE age > ?", 30)
	if qr.Error() != nil {
		log.Fatal(qr.Error())
	}
	err = qr.Into(&rawUsers)
	if err != nil {
		log.Fatal(err)
	}

	// Update with raw SQL
	result = chain.RawExecute("UPDATE users SET active = ? WHERE username = ?", false, "updated_john")
	if result.Error != nil {
		log.Fatal(result.Error)
	}

	// Cleanup
	_, err = db.DB.Exec("DROP TABLE IF EXISTS users")
	if err != nil {
		log.Fatal(err)
	}
}
