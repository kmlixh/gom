package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/example"
	_ "github.com/kmlixh/gom/v4/factory/mysql"
)

func main() {
	// Connect to MySQL
	db, err := gom.Open("mysql", "root:123456@tcp(localhost:3306)/test?parseTime=true", true)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Example 1: Using different isolation levels
	fmt.Println("\nExample 1: Transaction with SERIALIZABLE isolation level")
	chain := db.Chain()
	chain.SetIsolationLevel(sql.LevelSerializable)

	err = chain.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Create a user in serializable transaction
	user := &example.User{
		Username:  "alice",
		Email:     "alice@example.com",
		Age:       25,
		Active:    true,
		Role:      "user",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err = chain.From(user).Save()
	if err != nil {
		chain.Rollback()
		log.Fatal(err)
	}

	err = chain.Commit()
	if err != nil {
		log.Fatal(err)
	}

	// Example 2: Using savepoints
	fmt.Println("\nExample 2: Transaction with savepoints")
	chain = db.Chain()
	err = chain.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Create first user
	user1 := &example.User{
		Username:  "bob",
		Email:     "bob@example.com",
		Age:       30,
		Active:    true,
		Role:      "user",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err = chain.From(user1).Save()
	if err != nil {
		chain.Rollback()
		log.Fatal(err)
	}

	// Create savepoint after first user
	err = chain.Savepoint("after_user1")
	if err != nil {
		chain.Rollback()
		log.Fatal(err)
	}

	// Create second user
	user2 := &example.User{
		Username:  "charlie",
		Email:     "charlie@example.com",
		Age:       35,
		Active:    true,
		Role:      "user",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err = chain.From(user2).Save()
	if err != nil {
		chain.Rollback()
		log.Fatal(err)
	}

	// Rollback to savepoint (this will undo user2 creation but keep user1)
	err = chain.RollbackTo("after_user1")
	if err != nil {
		chain.Rollback()
		log.Fatal(err)
	}

	// Create alternative second user
	user2Alt := &example.User{
		Username:  "david",
		Email:     "david@example.com",
		Age:       40,
		Active:    true,
		Role:      "user",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err = chain.From(user2Alt).Save()
	if err != nil {
		chain.Rollback()
		log.Fatal(err)
	}

	// Commit the transaction
	err = chain.Commit()
	if err != nil {
		log.Fatal(err)
	}

	// Example 3: Transaction status checking
	fmt.Println("\nExample 3: Transaction status checking")
	chain = db.Chain()
	fmt.Printf("Is in transaction before Begin(): %v\n", chain.IsInTransaction())

	err = chain.Begin()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Is in transaction after Begin(): %v\n", chain.IsInTransaction())

	err = chain.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Is in transaction after Commit(): %v\n", chain.IsInTransaction())

	fmt.Println("\nAll examples completed successfully!")
}
