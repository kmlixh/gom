package gom

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
)

func setupFromTestDB(t *testing.T) *DB {
	config := testutils.DefaultMySQLConfig()
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		Debug:           true,
	}
	db, err := Open(config.Driver, config.DSN(), opts)
	if err != nil {
		t.Fatalf("Failed to create DB instance: %v", err)
		return nil
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS fromtestuser (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			age BIGINT,
			email VARCHAR(255),
			created_at DATETIME,
			updated_at DATETIME,
			is_active BOOLEAN DEFAULT true,
			score DOUBLE DEFAULT 0.0
		)
	`
	_, err = db.DB.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
		return nil
	}

	_, err = db.DB.Exec("TRUNCATE TABLE fromtestuser")
	if err != nil {
		t.Fatalf("Failed to truncate test table: %v", err)
		return nil
	}

	return db
}

type FromTestUser struct {
	ID        int64     `gom:"id,@,auto"`
	Name      string    `gom:"name"`
	Age       int64     `gom:"age"`
	Email     string    `gom:"email"`
	CreatedAt time.Time `gom:"created_at"`
	UpdatedAt time.Time `gom:"updated_at,default"`
	IsActive  bool      `gom:"is_active,default"`
	Score     float64   `gom:"score,default"`
}

func (u *FromTestUser) TableName() string {
	return "fromtestuser"
}

func TestFromBasicOperations(t *testing.T) {
	db := setupFromTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database error")
		return
	}
	defer db.Close()

	// Test data
	now := time.Now().Round(time.Second)
	user := &FromTestUser{
		Name:      "Test User",
		Age:       25,
		Email:     "test@example.com",
		CreatedAt: now,
	}

	// Test From with Insert
	chain := db.Chain().From(user)
	result := chain.Save()
	if result.Error != nil {
		t.Errorf("Failed to save user: %v", result.Error)
		return
	}
	if result.ID == 0 {
		t.Error("Expected non-zero ID after save")
		return
	}
	user.ID = result.ID

	// Verify the insert
	fetchedUser := &FromTestUser{}
	err := db.Chain().Where("id", define.OpEq, user.ID).From(fetchedUser).List2(fetchedUser)
	if err != nil {
		t.Errorf("Failed to fetch user: %v", err)
		return
	}
	if user.Name != fetchedUser.Name {
		t.Errorf("Expected name %s, got %s", user.Name, fetchedUser.Name)
	}
	if user.Age != fetchedUser.Age {
		t.Errorf("Expected age %d, got %d", user.Age, fetchedUser.Age)
	}
	if user.Email != fetchedUser.Email {
		t.Errorf("Expected email %s, got %s", user.Email, fetchedUser.Email)
	}
	if user.CreatedAt.Unix() != fetchedUser.CreatedAt.Unix() {
		t.Errorf("Expected created_at %v, got %v", user.CreatedAt, fetchedUser.CreatedAt)
	}
	if !fetchedUser.IsActive {
		t.Error("Expected IsActive to be true by default")
	}

	// Test From with Update
	user.Name = "Updated Name"
	user.Age = 26
	result = db.Chain().From(user).Where("id", define.OpEq, user.ID).Update(nil)
	if result.Error != nil {
		t.Errorf("Failed to update user: %v", result.Error)
		return
	}
	if result.Affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.Affected)
	}

	// Verify the update
	fetchedUser = &FromTestUser{}
	err = db.Chain().Where("id", define.OpEq, user.ID).From(fetchedUser).List2(fetchedUser)
	if err != nil {
		t.Errorf("Failed to fetch updated user: %v", err)
		return
	}
	if fetchedUser.Name != "Updated Name" {
		t.Errorf("Expected updated name %s, got %s", "Updated Name", fetchedUser.Name)
	}
	if fetchedUser.Age != 26 {
		t.Errorf("Expected updated age %d, got %d", 26, fetchedUser.Age)
	}

	// Test From with Delete
	result = db.Chain().From(user).Where("id", define.OpEq, user.ID).Delete()
	if result.Error != nil {
		t.Errorf("Failed to delete user: %v", result.Error)
		return
	}
	if result.Affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.Affected)
	}

	// Verify the delete
	fetchedUser = &FromTestUser{}
	err = db.Chain().Where("id", define.OpEq, user.ID).From(fetchedUser).List2(fetchedUser)
	if err != sql.ErrNoRows {
		t.Errorf("Expected sql.ErrNoRows, got %v", err)
	}
}

func TestFromWithDefaults(t *testing.T) {
	db := setupFromTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database error")
		return
	}
	defer db.Close()

	// Test data with only required fields
	user := &FromTestUser{
		Name:      "Default Test",
		Age:       30,
		Email:     "default@example.com",
		CreatedAt: time.Now(),
	}

	// Insert with defaults
	result := db.Chain().From(user).Save()
	if result.Error != nil {
		t.Errorf("Failed to save user: %v", result.Error)
		return
	}
	if result.ID == 0 {
		t.Error("Expected non-zero ID after save")
		return
	}
	user.ID = result.ID

	// Verify defaults were applied
	fetchedUser := &FromTestUser{}
	err := db.Chain().Where("id", define.OpEq, user.ID).From(fetchedUser).List2(fetchedUser)
	if err != nil {
		t.Errorf("Failed to fetch user: %v", err)
		return
	}
	if !fetchedUser.IsActive {
		t.Error("Expected IsActive to be true by default")
	}
	if fetchedUser.Score != 0.0 {
		t.Errorf("Expected Score to be 0.0 by default, got %f", fetchedUser.Score)
	}
	if fetchedUser.UpdatedAt.IsZero() {
		t.Error("Expected UpdatedAt to have default timestamp")
	}
}

func TestFromWithBatchOperations(t *testing.T) {
	db := setupFromTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database error")
		return
	}
	defer db.Close()

	// Create test data
	now := time.Now().Round(time.Second)
	users := []*FromTestUser{
		{
			Name:      "Batch User 1",
			Age:       25,
			Email:     "batch1@example.com",
			CreatedAt: now,
		},
		{
			Name:      "Batch User 2",
			Age:       30,
			Email:     "batch2@example.com",
			CreatedAt: now,
		},
	}

	// Test batch insert
	for _, user := range users {
		result := db.Chain().From(user).Save()
		if result.Error != nil {
			t.Errorf("Failed to save user: %v", result.Error)
			return
		}
		if result.ID == 0 {
			t.Error("Expected non-zero ID after save")
			return
		}
		user.ID = result.ID
	}

	// Test batch update
	for _, user := range users {
		user.Name = "Updated " + user.Name
		result := db.Chain().From(user).Where("id", define.OpEq, user.ID).Update(nil)
		if result.Error != nil {
			t.Errorf("Failed to update user: %v", result.Error)
			return
		}
		if result.Affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.Affected)
		}
	}

	// Verify updates
	var fetchedUsers []*FromTestUser
	err := db.Chain().OrderBy("id").From(&FromTestUser{}).List2(&fetchedUsers)
	if err != nil {
		t.Errorf("Failed to fetch users: %v", err)
		return
	}
	if len(fetchedUsers) != 2 {
		t.Errorf("Expected 2 users, got %d", len(fetchedUsers))
		return
	}
	if fetchedUsers[0].Name != "Updated Batch User 1" {
		t.Errorf("Expected name %s, got %s", "Updated Batch User 1", fetchedUsers[0].Name)
	}
	if fetchedUsers[1].Name != "Updated Batch User 2" {
		t.Errorf("Expected name %s, got %s", "Updated Batch User 2", fetchedUsers[1].Name)
	}

	// Test batch delete
	for _, user := range users {
		result := db.Chain().From(user).Where("id", define.OpEq, user.ID).Delete()
		if result.Error != nil {
			t.Errorf("Failed to delete user: %v", result.Error)
			return
		}
		if result.Affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.Affected)
		}
	}

	// Verify all deleted
	err = db.Chain().From(&FromTestUser{}).List2(&fetchedUsers)
	if err != nil {
		t.Errorf("Failed to fetch users: %v", err)
		return
	}
	if len(fetchedUsers) != 0 {
		t.Errorf("Expected 0 users after deletion, got %d", len(fetchedUsers))
	}
}

func TestFromWithComplexQueries(t *testing.T) {
	db := setupFromTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database error")
		return
	}
	defer db.Close()

	// Create test data
	now := time.Now().Round(time.Second)
	users := []*FromTestUser{
		{
			Name:      "Complex User 1",
			Age:       25,
			Email:     "complex1@example.com",
			CreatedAt: now,
			Score:     85.5,
		},
		{
			Name:      "Complex User 2",
			Age:       30,
			Email:     "complex2@example.com",
			CreatedAt: now,
			Score:     92.0,
		},
		{
			Name:      "Complex User 3",
			Age:       35,
			Email:     "complex3@example.com",
			CreatedAt: now,
			Score:     78.5,
		},
	}

	// Insert test data
	for _, user := range users {
		result := db.Chain().From(user).Save()
		if result.Error != nil {
			t.Errorf("Failed to save user: %v", result.Error)
			return
		}
		if result.ID == 0 {
			t.Error("Expected non-zero ID after save")
			return
		}
		user.ID = result.ID
	}

	// Test complex query with multiple conditions
	var fetchedUsers []*FromTestUser
	err := db.Chain().
		From(&FromTestUser{}).
		Where("age", define.OpGe, 25).
		And("age", define.OpLe, 30).
		And("score", define.OpGt, 80.0).
		OrderBy("score").
		List2(&fetchedUsers)

	if err != nil {
		t.Errorf("Failed to fetch users with complex query: %v", err)
		return
	}
	if len(fetchedUsers) != 2 {
		t.Errorf("Expected 2 users, got %d", len(fetchedUsers))
		return
	}
	if fetchedUsers[0].Name != "Complex User 1" {
		t.Errorf("Expected first user %s, got %s", "Complex User 1", fetchedUsers[0].Name)
	}
	if fetchedUsers[1].Name != "Complex User 2" {
		t.Errorf("Expected second user %s, got %s", "Complex User 2", fetchedUsers[1].Name)
	}

	// Test with OR conditions
	err = db.Chain().
		From(&FromTestUser{}).
		Where("age", define.OpEq, 25).
		Or("score", define.OpGt, 90.0).
		OrderBy("age").
		List2(&fetchedUsers)

	if err != nil {
		t.Errorf("Failed to fetch users with OR conditions: %v", err)
		return
	}
	if len(fetchedUsers) != 2 {
		t.Errorf("Expected 2 users, got %d", len(fetchedUsers))
		return
	}
	if fetchedUsers[0].Name != "Complex User 1" {
		t.Errorf("Expected first user %s, got %s", "Complex User 1", fetchedUsers[0].Name)
	}
	if fetchedUsers[1].Name != "Complex User 2" {
		t.Errorf("Expected second user %s, got %s", "Complex User 2", fetchedUsers[1].Name)
	}

	// Test with pagination
	var pagedUsers []*FromTestUser
	err = db.Chain().
		From(&FromTestUser{}).
		OrderBy("age").
		Limit(2).
		List2(&pagedUsers)

	if err != nil {
		t.Errorf("Failed to fetch users with pagination: %v", err)
		return
	}
	if len(pagedUsers) != 2 {
		t.Errorf("Expected 2 users, got %d", len(pagedUsers))
		return
	}
	if pagedUsers[0].Name != "Complex User 1" {
		t.Errorf("Expected first user %s, got %s", "Complex User 1", pagedUsers[0].Name)
	}
	if pagedUsers[1].Name != "Complex User 2" {
		t.Errorf("Expected second user %s, got %s", "Complex User 2", pagedUsers[1].Name)
	}
}
