package gom

import (
	"database/sql"
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
			is_active BOOLEAN DEFAULT true,
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

	// Create test user
	now := time.Now().Round(time.Second)
	user := &TestUser{
		Name:      "User 1",
		Age:       25,
		Email:     "user1@test.com",
		CreatedAt: now,
		UpdatedAt: now,
		IsActive:  true,
		Score:     85.5,
	}

	// Test Save2
	err := db.Chain().Table("test_user").Save2(user)
	assert.NoError(t, err)
	assert.NotZero(t, user.ID)

	// Test List2 single
	var fetchedUser TestUser
	err = db.Chain().Table("test_user").Where("id", define.OpEq, user.ID).List2(&fetchedUser)
	assert.NoError(t, err)
	assert.Equal(t, user.Name, fetchedUser.Name)
	assert.Equal(t, user.Age, fetchedUser.Age)
	assert.Equal(t, user.Email, fetchedUser.Email)
	assert.Equal(t, user.CreatedAt.Unix(), fetchedUser.CreatedAt.Unix())
	assert.Equal(t, user.IsActive, fetchedUser.IsActive)
	assert.Equal(t, user.Score, fetchedUser.Score)

	// Test List2 multiple
	var users []TestUser
	err = db.Chain().Table("test_user").OrderBy("id").List2(&users)
	assert.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, user.Name, users[0].Name)

	// Test Update2
	user.Name = "Updated User 1"
	err = db.Chain().Table("test_user").Update2(user)
	assert.NoError(t, err)

	var updatedUser TestUser
	err = db.Chain().Table("test_user").Where("id", define.OpEq, user.ID).List2(&updatedUser)
	assert.NoError(t, err)
	assert.Equal(t, "Updated User 1", updatedUser.Name)

	// Test Delete2
	err = db.Chain().Table("test_user").Delete2(user)
	assert.NoError(t, err)

	var deletedUser TestUser
	err = db.Chain().Table("test_user").Where("id", define.OpEq, user.ID).List2(&deletedUser)
	assert.Equal(t, sql.ErrNoRows, err)
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

	err = gdb.Chain().Save2(user)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("List2Single", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var u TestUser
			err := gdb.Chain().Where("id", define.OpEq, user.ID).List2(&u)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("List2Multiple", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var users []TestUser
			err := gdb.Chain().List2(&users)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Update2", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			u := *user
			u.Name = "Updated " + string(rune(i))
			err := gdb.Chain().Update2(&u)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestTransferEdgeCases(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	// Test nil pointer
	err := db.Chain().List2(nil)
	assert.Error(t, err)

	// Test non-pointer
	var user TestUser
	err = db.Chain().List2(user)
	assert.Error(t, err)

	// Test empty result
	err = db.Chain().Where("id", define.OpEq, -1).List2(&user)
	assert.Equal(t, sql.ErrNoRows, err)

	// Test invalid field type
	_, err = db.DB.Exec(`
		INSERT INTO test_user (name, age, email) 
		VALUES ('Invalid', 'not a number', 'test@test.com')
	`)
	assert.Error(t, err) // Should error on invalid integer value

	var users []TestUser
	err = db.Chain().List2(&users)
	assert.NoError(t, err) // Should succeed in listing existing records
	assert.Empty(t, users) // No records should exist
}

func TestTransferConcurrency(t *testing.T) {
	db := createTestDB(t)
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

	err := db.Chain().Save2(user)
	assert.NoError(t, err)

	// Test concurrent access
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			var u TestUser
			err := db.Chain().Where("id", define.OpEq, user.ID).List2(&u)
			assert.NoError(t, err)
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
