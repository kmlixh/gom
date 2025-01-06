package main

import (
	"fmt"
	"log"
	"time"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/define"
)

// User represents a user in the system
type User struct {
	Id        int64     `gom:"id,@,auto"`
	Name      string    `gom:"name,notnull"`
	Email     string    `gom:"email,notnull"`
	Age       int       `gom:"age,notnull"`
	CreatedAt time.Time `gom:"created_at,notnull,default"`
	UpdatedAt time.Time `gom:"updated_at,notnull,default"`
}

// TableName returns the table name for User
func (u *User) TableName() string {
	return "users"
}

func main() {
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

	// Create table
	err = db.Chain().CreateTable(&User{})
	if err != nil {
		log.Fatal(err)
	}

	// Insert data
	user := &User{
		Name:  "John",
		Email: "john@example.com",
		Age:   25,
	}
	result := db.Chain().Table("users").From(user).Save()
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	fmt.Printf("Inserted user with ID: %d\n", result.ID)

	// Query data
	var users []User
	qr := db.Chain().Table("users").Where2(define.Eq("age", 25)).List(&users)
	if qr.Error() != nil {
		log.Fatal(qr.Error())
	}
	for _, u := range users {
		fmt.Printf("Found user: %+v\n", u)
	}
}
