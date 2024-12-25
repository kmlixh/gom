package main

import (
	"fmt"
	"log"
	"time"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/example"
	_ "github.com/kmlixh/gom/v4/factory/postgres"
)

func main() {
	// Connect to PostgreSQL
	db, err := gom.Open("postgres", "postgres://postgres:yzy123@192.168.110.249:5432/test?sslmode=disable", true)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Drop existing tables if they exist
	fmt.Println("Dropping existing tables...")
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS user_profiles")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS users")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS user_roles")
	if err != nil {
		log.Fatal(err)
	}

	// Create tables using Chain().CreateTable()
	fmt.Println("Creating tables...")
	err = db.Chain().CreateTable(&example.UserRole{})
	if err != nil {
		log.Fatalf("Failed to create user_roles table: %v", err)
	}
	fmt.Println("Created user_roles table")

	err = db.Chain().CreateTable(&example.User{})
	if err != nil {
		log.Fatalf("Failed to create users table: %v", err)
	}
	fmt.Println("Created users table")

	err = db.Chain().CreateTable(&example.UserProfile{})
	if err != nil {
		log.Fatalf("Failed to create user_profiles table: %v", err)
	}
	fmt.Println("Created user_profiles table")

	// Test 1: Insert a role using Chain().From().Save()
	fmt.Println("\nTest 1: Inserting role...")
	role := &example.UserRole{
		Name:        "admin",
		Description: "Administrator role",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	result, err := db.Chain().From(role).Save()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created admin role")

	// Test 2: Insert a user with Chain().From().Save()
	fmt.Println("\nTest 2: Inserting user...")
	user := &example.User{
		Username:  "john_doe",
		Email:     "john@example.com",
		Age:       30,
		Active:    true,
		Role:      "admin",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	result, err = db.Chain().From(user).Save()
	if err != nil {
		log.Fatal(err)
	}
	userID, err := result.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Inserted user with ID: %d\n", userID)

	// Test 3: Insert user profile with Chain().From().Save()
	fmt.Println("\nTest 3: Inserting user profile...")
	profile := &example.UserProfile{
		UserID:    userID,
		Avatar:    "/avatars/john.jpg",
		Bio:       "Software Engineer",
		Location:  "San Francisco",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	_, err = db.Chain().From(profile).Save()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created user profile")

	// Test 4: Query user with Chain().From().Where()
	fmt.Println("\nTest 4: Querying user by username...")
	var users []example.User
	queryResult, err := db.Chain().From(&example.User{}).Where("username", "=", "john_doe").List()
	if err != nil {
		log.Fatal(err)
	}
	err = queryResult.Into(&users)
	if err != nil {
		log.Fatal(err)
	}
	if len(users) > 0 {
		fmt.Printf("Found user: %s (ID: %d, Email: %s)\n", users[0].Username, users[0].ID, users[0].Email)
	} else {
		fmt.Println("User not found")
	}

	// Test 5: Update user with Chain().From().Set().Where()
	fmt.Println("\nTest 5: Updating user age...")
	_, err = db.Chain().From(&example.User{}).Set("age", 31).Where("id", "=", userID).Save()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Updated user age")

	// Test 6: Batch insert users
	fmt.Println("\nTest 6: Batch inserting users...")
	batchUsers := []example.User{
		{
			Username:  "user1",
			Email:     "user1@example.com",
			Age:       25,
			Active:    true,
			Role:      "user",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		{
			Username:  "user2",
			Email:     "user2@example.com",
			Age:       28,
			Active:    true,
			Role:      "user",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
	}

	// Convert slice of users to slice of maps
	var batchMaps []map[string]interface{}
	for _, u := range batchUsers {
		batchMaps = append(batchMaps, map[string]interface{}{
			"username":   u.Username,
			"email":      u.Email,
			"age":        u.Age,
			"active":     u.Active,
			"role":       u.Role,
			"created_at": u.CreatedAt,
			"updated_at": u.UpdatedAt,
		})
	}

	_, err = db.Chain().From(&example.User{}).BatchValues(batchMaps).Save()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Batch inserted users")

	// Test 7: Pagination with Chain().From().Page()
	fmt.Println("\nTest 7: Testing pagination...")
	var pagedUsers []example.User
	queryResult, err = db.Chain().From(&example.User{}).OrderBy("id ASC").Limit(2).Offset(0).List()
	if err != nil {
		log.Fatal(err)
	}
	err = queryResult.Into(&pagedUsers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Page 1 (size 2) users count: %d\n", len(pagedUsers))

	// Test 8: Complex query using UserQuery struct
	fmt.Println("\nTest 8: Complex query using UserQuery...")
	minAge := 25
	isActive := true
	queryModel := &example.UserQuery{
		MinAge:   &minAge,
		IsActive: &isActive,
	}
	var queryUsers []example.User
	queryResult, err = db.Chain().From(queryModel).List()
	if err != nil {
		log.Fatal(err)
	}
	err = queryResult.Into(&queryUsers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d active users with age >= 25\n", len(queryUsers))

	// Test 9: Raw query execution
	fmt.Println("\nTest 9: Raw query execution...")
	var rawUsers []example.User
	rawResult, err := db.Chain().RawQuery(`
		SELECT u.*, p.avatar, p.bio, p.location 
		FROM users u 
		LEFT JOIN user_profiles p ON u.id = p.user_id 
		WHERE u.id = $1`, userID)
	if err != nil {
		log.Fatal(err)
	}
	err = rawResult.Into(&rawUsers)
	if err != nil {
		log.Fatal(err)
	}
	if len(rawUsers) > 0 {
		fmt.Printf("Found user with raw query: %s (ID: %d, Email: %s)\n", rawUsers[0].Username, rawUsers[0].ID, rawUsers[0].Email)
	} else {
		fmt.Println("User not found with raw query")
	}

	// Test 10: Delete operation
	fmt.Println("\nTest 10: Delete operation...")
	_, err = db.Chain().From(&example.User{}).Where("username", "=", "user2").Delete()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Deleted user2")

	// Test 11: Custom table model
	fmt.Println("\nTest 11: Testing custom table model...")
	// Drop existing custom_users table
	_, err = db.Chain().RawExecute("DROP TABLE IF EXISTS custom_users CASCADE")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Chain().CreateTable(&example.CustomUser{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created custom_users table")

	customUser := &example.CustomUser{
		Username:  "custom_user",
		Email:     "custom@example.com",
		Age:       35,
		Active:    true,
		Role:      "admin",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	result, err = db.Chain().From(customUser).Save()
	if err != nil {
		log.Fatal(err)
	}
	customUserID, err := result.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Inserted custom user with ID: %d\n", customUserID)

	var customUsers []example.CustomUser
	queryResult, err = db.Chain().From(&example.CustomUser{}).List()
	if err != nil {
		log.Fatal(err)
	}
	err = queryResult.Into(&customUsers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d custom users\n", len(customUsers))

	fmt.Println("\nAll tests completed successfully!")
}
