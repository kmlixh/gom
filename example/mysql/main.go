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
}
