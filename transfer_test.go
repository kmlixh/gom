package gom

import (
	"database/sql"
	"math"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

type TestUser struct {
	ID        int64     `gom:"id,@" sql:"id,pk,auto_increment"`
	Name      string    `gom:"name" sql:"name"`
	Age       int       `gom:"age" sql:"age"`
	Email     string    `gom:"email" sql:"email"`
	CreatedAt time.Time `gom:"created_at" sql:"created_at"`
	UpdatedAt time.Time `gom:"updated_at" sql:"updated_at"`
	IsActive  bool      `gom:"is_active" sql:"is_active"`
	Score     float64   `gom:"score" sql:"score"`
}

func (t *TestUser) TableName() string {
	return "test_user"
}

func createTestDB(t *testing.T) *DB {
	// Initialize database options
	opts := &define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		Debug:           true,
	}

	// Get MySQL config from testutils
	config := testutils.DefaultMySQLConfig()
	db, err := Open(config.Driver, config.DSN(), opts)
	if err != nil {
		t.Skipf("Skipping test: could not connect to database: %v", err)
		return nil
	}

	// Test the connection
	err = db.DB.Ping()
	if err != nil {
		t.Skipf("Skipping test: could not ping database: %v", err)
		db.Close()
		return nil
	}

	// Create test table
	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS test_user (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			age INT,
			email VARCHAR(255),
			created_at DATETIME,
			updated_at DATETIME,
			is_active TINYINT(1) DEFAULT 1,
			score DOUBLE DEFAULT 0.0
		)
	`)
	if err != nil {
		t.Errorf("Failed to create test table: %v", err)
		db.Close()
		return nil
	}

	// Clear test data
	_, err = db.DB.Exec("TRUNCATE TABLE test_user")
	if err != nil {
		t.Errorf("Failed to truncate test table: %v", err)
		db.Close()
		return nil
	}

	return db
}

func TestTransferBasicOperations(t *testing.T) {
	db := createTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Clean up any existing data
	_, err := db.DB.Exec("DELETE FROM test_user")
	assert.NoError(t, err)

	now := time.Now().UTC()
	user := &TestUser{
		Name:      "Test User",
		Age:       25,
		Email:     "test@example.com",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  true,
		Score:     85.5,
	}

	// Test Save
	result := db.Chain().Table("test_user").Save(user)
	assert.NoError(t, result.Error)
	assert.NotZero(t, user.ID)

	// Test List single
	var users []TestUser
	result = db.Chain().Table("test_user").Where("id", define.OpEq, user.ID).List(&users)
	assert.NoError(t, result.Error)
	assert.Len(t, users, 1)
	fetchedUser := users[0]
	assert.Equal(t, user.Name, fetchedUser.Name)
	assert.Equal(t, user.Age, fetchedUser.Age)
	assert.Equal(t, user.Email, fetchedUser.Email)
	timeDiff := math.Abs(float64(user.CreatedAt.Unix() - fetchedUser.CreatedAt.Unix()))
	assert.LessOrEqual(t, timeDiff, float64(1), "CreatedAt timestamps should be within 1 second")
	assert.Equal(t, user.IsActive, fetchedUser.IsActive)
	assert.Equal(t, user.Score, fetchedUser.Score)

	// Test List multiple
	users = nil
	result = db.Chain().Table("test_user").OrderBy("id").List(&users)
	assert.NoError(t, result.Error)
	assert.Len(t, users, 1)
	assert.Equal(t, user.Name, users[0].Name)

	// Test Update
	updatedUser := *user
	updatedUser.Name = "Updated User 1"
	result = db.Chain().Table("test_user").Where("id", define.OpEq, user.ID).Values(map[string]interface{}{
		"name": updatedUser.Name,
	}).Save()
	assert.NoError(t, result.Error)

	users = nil
	result = db.Chain().Table("test_user").Where("id", define.OpEq, user.ID).List(&users)
	assert.NoError(t, result.Error)
	assert.Len(t, users, 1)
	assert.Equal(t, "Updated User 1", users[0].Name)

	// Test Delete
	result = db.Chain().Table("test_user").Where("id", define.OpEq, user.ID).Delete()
	assert.NoError(t, result.Error)

	// Verify deletion
	var checkUser TestUser
	result = db.Chain().Table("test_user").Where("id", define.OpEq, user.ID).First(&checkUser)
	assert.Error(t, result.Error)
	assert.Equal(t, sql.ErrNoRows, result.Error)
}

func BenchmarkTransferOperations(b *testing.B) {
	gdb, err := Open("mysql", "root:root@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=true", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer gdb.Close()

	// Prepare test data
	now := time.Now()
	user := &TestUser{
		Name:      "Benchmark User",
		Age:       25,
		Email:     "bench@test.com",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  true,
		Score:     85.5,
	}

	result := gdb.Chain().Save(user)
	if result.Error != nil {
		b.Fatal(result.Error)
	}

	b.Run("ListSingle", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var u TestUser
			result := gdb.Chain().Where("id", define.OpEq, user.ID).List(&u)
			if result.Error != nil {
				b.Fatal(result.Error)
			}
		}
	})

	b.Run("ListMultiple", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var users []TestUser
			result := gdb.Chain().List(&users)
			if result.Error != nil {
				b.Fatal(result.Error)
			}
		}
	})

	b.Run("Update", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			u := *user
			u.Name = "Updated " + string(rune(i))
			result := gdb.Chain().Update(&u)
			if result.Error != nil {
				b.Fatal(result.Error)
			}
		}
	})
}

func TestTransferEdgeCases(t *testing.T) {
	db := createTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Clean up any existing data
	_, err := db.DB.Exec("DELETE FROM test_user")
	assert.NoError(t, err)

	// Test nil pointer
	var nilPtr *TestUser
	result := db.Chain().Table("test_user").Save(nilPtr)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "nil pointer")

	// Test non-pointer
	var user TestUser
	result = db.Chain().Table("test_user").Save(user)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "non-pointer")

	// Test empty result
	result = db.Chain().Table("test_user").Where("id", define.OpEq, -1).First(&user)
	assert.Equal(t, sql.ErrNoRows, result.Error)

	// Test invalid field type
	_, err = db.DB.Exec(`
		INSERT INTO test_user (name, age, email, created_at, updated_at, is_active, score) 
		VALUES ('Invalid', 'not a number', 'test@test.com', NOW(), NOW(), true, 1.0)
	`)
	assert.Error(t, err) // Should error on invalid integer value

	var users []TestUser
	result = db.Chain().Table("test_user").List(&users)
	assert.NoError(t, result.Error)
	assert.Empty(t, users) // No records should exist

	// Test invalid table name
	result = db.Chain().Table("non_existent_table").List(&users)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "table") // Error should mention table issue
}

func TestTransferConcurrency(t *testing.T) {
	db := createTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test data
	user := &TestUser{
		Name:      "Concurrent User",
		Age:       25,
		Email:     "concurrent@test.com",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		IsActive:  true,
		Score:     85.5,
	}

	result := db.Chain().Table("test_user").Save(user)
	assert.NoError(t, result.Error)

	// Test concurrent access
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			var users []TestUser
			result := db.Chain().Table("test_user").Where("id", define.OpEq, user.ID).List(&users)
			assert.NoError(t, result.Error)
			assert.Len(t, users, 1)
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
