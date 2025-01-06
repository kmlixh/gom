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

	// Create table
	err = db.Chain().CreateTable(&example.User{})
	if err != nil {
		log.Fatal(err)
	}

	// Insert data
	newUser := &example.User{
		Username:  "john_doe",
		Email:     "john@example.com",
		Age:       25,
		Active:    true,
		Role:      "admin",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	result := db.Chain().Table("users").From(newUser).Save()
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	fmt.Printf("Inserted user with ID: %d\n", result.ID)

	// Query data
	var queryUsers []example.User
	qr := db.Chain().Table("users").Where2(define.Eq("age", 25)).List(&queryUsers)
	if qr.Error() != nil {
		log.Fatal(qr.Error())
	}
	for _, u := range queryUsers {
		fmt.Printf("Found user: %+v\n", u)
	}
}
