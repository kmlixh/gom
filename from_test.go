package gom

import (
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
)

func setupFromTestDB(t *testing.T) *DB {
	config := testutils.DefaultMySQLConfig()
	config.User = "root"

	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		Debug:           true,
	}
	db, err := Open(config.Driver, config.DSN(), opts)
	if err != nil {
		t.Skipf("Skipping test due to database connection error: %v", err)
		return nil
	}

	// Test database connection
	if err := db.DB.Ping(); err != nil {
		t.Skipf("Failed to ping database: %v", err)
		return nil
	}

	// Drop table if exists to ensure clean state
	_, err = db.DB.Exec("DROP TABLE IF EXISTS fromtestuser")
	if err != nil {
		t.Skipf("Failed to drop test table: %v", err)
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
			is_active TINYINT(1) DEFAULT 1,
			score DOUBLE DEFAULT 0.0
		)
	`
	_, err = db.DB.Exec(createTableSQL)
	if err != nil {
		t.Errorf("Failed to create test table: %v", err)
		db.Close()
		return nil
	}

	// Clear test data
	_, err = db.DB.Exec("TRUNCATE TABLE fromtestuser")
	if err != nil {
		t.Errorf("Failed to truncate test table: %v", err)
		db.Close()
		return nil
	}

	// Verify table creation
	var tableName string
	err = db.DB.QueryRow("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", "test", "fromtestuser").Scan(&tableName)
	if err != nil {
		t.Skipf("Failed to verify table creation: %v", err)
		return nil
	}
	if tableName != "fromtestuser" {
		t.Skip("Table 'fromtestuser' was not created")
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
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	user := FromTestUser{
		Name:      "Test User",
		Age:       25,
		Email:     "test@example.com",
		IsActive:  true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Test Save
	result := db.Chain().Table("fromtestuser").Save(&user)
	assert.NoError(t, result.Error)
	assert.NotZero(t, user.ID)

	// Test List
	var users []FromTestUser
	result = db.Chain().Table("fromtestuser").Where("id", define.OpEq, user.ID).List(&users)
	assert.NoError(t, result.Error)
	assert.Len(t, users, 1)
	fetchedUser := users[0]
	assert.Equal(t, user.Name, fetchedUser.Name)
	assert.Equal(t, user.Age, fetchedUser.Age)
	assert.Equal(t, user.Email, fetchedUser.Email)
	assert.True(t, user.CreatedAt.Equal(fetchedUser.CreatedAt) || user.CreatedAt.Sub(fetchedUser.CreatedAt) < time.Second)
	assert.True(t, user.IsActive == fetchedUser.IsActive)

	// Test Update
	user.Name = "Updated User"
	result = db.Chain().Table("fromtestuser").Where("id", define.OpEq, user.ID).Update(&user)
	assert.NoError(t, result.Error)

	// Verify update
	var updatedUsers []FromTestUser
	result = db.Chain().Table("fromtestuser").Where("id", define.OpEq, user.ID).List(&updatedUsers)
	assert.NoError(t, result.Error)
	assert.Len(t, updatedUsers, 1)
	updatedUser := updatedUsers[0]
	assert.Equal(t, "Updated User", updatedUser.Name)

	// Test Delete
	result = db.Chain().Table("fromtestuser").Delete(&user)
	assert.NoError(t, result.Error)

	// Verify deletion
	count, err := db.Chain().Table("fromtestuser").Where("id", define.OpEq, user.ID).Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestFromWithDefaults(t *testing.T) {
	db := setupFromTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	user := FromTestUser{
		Name:     "Default Test",
		Age:      30,
		Email:    "default@example.com",
		IsActive: true,
	}

	// Test Save
	result := db.Chain().Table("fromtestuser").Save(&user)
	assert.NoError(t, result.Error)
	assert.NotZero(t, user.ID)

	// Test List
	var users []FromTestUser
	result = db.Chain().Table("fromtestuser").Where("id", define.OpEq, user.ID).List(&users)
	assert.NoError(t, result.Error)
	assert.Len(t, users, 1)
	fetchedUser := users[0]
	assert.Equal(t, user.Name, fetchedUser.Name)
	assert.Equal(t, user.Age, fetchedUser.Age)
	assert.Equal(t, user.Email, fetchedUser.Email)
	assert.True(t, user.CreatedAt.Equal(fetchedUser.CreatedAt) || user.CreatedAt.Sub(fetchedUser.CreatedAt) < time.Second)
	assert.True(t, user.IsActive == fetchedUser.IsActive)
}

func TestFromWithBatchOperations(t *testing.T) {
	db := setupFromTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test users
	users := []FromTestUser{
		{
			Name:      "Batch User 1",
			Age:       25,
			Email:     "batch1@example.com",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			IsActive:  true,
		},
		{
			Name:      "Batch User 2",
			Age:       30,
			Email:     "batch2@example.com",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			IsActive:  true,
		},
	}

	// Test batch save
	for i := range users {
		result := db.Chain().Table("fromtestuser").Save(&users[i])
		assert.NoError(t, result.Error)
		assert.NotZero(t, users[i].ID)
	}

	// Test batch update
	for i := range users {
		users[i].Name = "Updated " + users[i].Name
		result := db.Chain().Table("fromtestuser").Update(&users[i])
		assert.NoError(t, result.Error)
	}

	// Test batch list
	var fetchedUsers []FromTestUser
	result := db.Chain().Table("fromtestuser").OrderBy("id").List(&fetchedUsers)
	assert.NoError(t, result.Error)
	assert.Len(t, fetchedUsers, len(users))
	for i := range users {
		assert.Equal(t, users[i].Name, fetchedUsers[i].Name)
	}

	// Test batch delete
	for i := range users {
		result := db.Chain().Table("fromtestuser").Delete(&users[i])
		assert.NoError(t, result.Error)
	}
}

func TestFromWithComplexQueries(t *testing.T) {
	db := setupFromTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test users with different scores
	users := []FromTestUser{
		{
			Name:      "Complex User 1",
			Age:       25,
			Email:     "complex1@example.com",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			IsActive:  true,
			Score:     85.5,
		},
		{
			Name:      "Complex User 2",
			Age:       30,
			Email:     "complex2@example.com",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			IsActive:  true,
			Score:     92.0,
		},
		{
			Name:      "Complex User 3",
			Age:       35,
			Email:     "complex3@example.com",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			IsActive:  true,
			Score:     78.5,
		},
	}

	// Save test users
	for i := range users {
		result := db.Chain().Table("fromtestuser").Save(&users[i])
		assert.NoError(t, result.Error)
		assert.NotZero(t, users[i].ID)
	}

	// Test complex query
	var fetchedUsers []FromTestUser
	result := db.Chain().Table("fromtestuser").
		Where("age", define.OpGe, 25).
		Where("age", define.OpLe, 30).
		Where("score", define.OpGt, 80).
		OrderBy("score").
		List(&fetchedUsers)
	assert.NoError(t, result.Error)
	assert.Len(t, fetchedUsers, 2)
	assert.Equal(t, users[0].Name, fetchedUsers[0].Name) // Complex User 1
	assert.Equal(t, users[1].Name, fetchedUsers[1].Name) // Complex User 2
}
