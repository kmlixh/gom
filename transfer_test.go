package gom

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v4/define"
	"github.com/stretchr/testify/assert"
)

type TestUser struct {
	ID        int64     `gom:"id,@"`
	Name      string    `gom:"name"`
	Age       int       `gom:"age"`
	Email     string    `gom:"email"`
	CreatedAt time.Time `gom:"created_at"`
	UpdatedAt time.Time `gom:"updated_at"`
	IsActive  bool      `gom:"is_active"`
	Score     float64   `gom:"score"`
}

func createTestDB(t *testing.T) *DB {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=true")
	assert.NoError(t, err)

	// Create test table
	_, err = db.Exec(`
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
	assert.NoError(t, err)

	// Clear test data
	_, err = db.Exec("TRUNCATE TABLE test_user")
	assert.NoError(t, err)

	return &DB{DB: db}
}

func TestTransferBasicOperations(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	// Test data
	now := time.Now().Round(time.Second)
	users := []TestUser{
		{
			Name:      "User 1",
			Age:       25,
			Email:     "user1@test.com",
			CreatedAt: now,
			UpdatedAt: now,
			IsActive:  true,
			Score:     85.5,
		},
		{
			Name:      "User 2",
			Age:       30,
			Email:     "user2@test.com",
			CreatedAt: now,
			UpdatedAt: now,
			IsActive:  false,
			Score:     92.0,
		},
	}

	// Test Save2
	for _, user := range users {
		err := db.Chain().Save2(&user)
		assert.NoError(t, err)
		assert.NotZero(t, user.ID)
	}

	// Test List2 single
	var user TestUser
	err := db.Chain().Where("id", define.OpEq, users[0].ID).List2(&user)
	assert.NoError(t, err)
	assert.Equal(t, users[0].Name, user.Name)
	assert.Equal(t, users[0].Age, user.Age)
	assert.Equal(t, users[0].Email, user.Email)
	assert.Equal(t, users[0].CreatedAt.Unix(), user.CreatedAt.Unix())
	assert.Equal(t, users[0].IsActive, user.IsActive)
	assert.Equal(t, users[0].Score, user.Score)

	// Test List2 multiple
	var fetchedUsers []TestUser
	err = db.Chain().OrderBy("id").List2(&fetchedUsers)
	assert.NoError(t, err)
	assert.Len(t, fetchedUsers, 2)
	assert.Equal(t, users[0].Name, fetchedUsers[0].Name)
	assert.Equal(t, users[1].Name, fetchedUsers[1].Name)

	// Test Update2
	user.Name = "Updated User 1"
	err = db.Chain().Update2(&user)
	assert.NoError(t, err)

	var updatedUser TestUser
	err = db.Chain().Where("id", define.OpEq, user.ID).List2(&updatedUser)
	assert.NoError(t, err)
	assert.Equal(t, "Updated User 1", updatedUser.Name)

	// Test Delete2
	err = db.Chain().Delete2(&user)
	assert.NoError(t, err)

	var deletedUser TestUser
	err = db.Chain().Where("id", define.OpEq, user.ID).List2(&deletedUser)
	assert.Equal(t, sql.ErrNoRows, err)
}

func BenchmarkTransferOperations(b *testing.B) {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=true")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	gdb := &DB{DB: db}

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
	assert.NoError(t, err)

	var users []TestUser
	err = db.Chain().List2(&users)
	assert.NoError(t, err) // Should not error, invalid values should be zero
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
