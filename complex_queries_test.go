package gom

import (
	"strconv"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/factory/mysql"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

// TestCategory 测试分类模型
type TestCategory struct {
	ID        int64     `sql:"id,pk,auto_increment" gom:"id"`
	TestID    int64     `sql:"test_id" gom:"test_id"`
	Category  string    `sql:"category" gom:"category"`
	CreatedAt time.Time `sql:"created_at" gom:"created_at"`
}

func (t *TestCategory) TableName() string {
	return "test_categories"
}

func createTestCategoryTable(db *DB) error {
	var sql string
	if _, ok := db.Factory.(*mysql.Factory); ok {
		sql = `
			CREATE TABLE IF NOT EXISTS test_categories (
				id BIGINT AUTO_INCREMENT PRIMARY KEY,
				test_id BIGINT NOT NULL,
				category VARCHAR(255) NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
			)
		`
	} else {
		sql = `
			CREATE TABLE IF NOT EXISTS test_categories (
				id BIGSERIAL PRIMARY KEY,
				test_id BIGINT NOT NULL,
				category VARCHAR(255) NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
			)
		`
	}
	_, err := db.DB.Exec(sql)
	return err
}

// TestRawQueryResult 原始查询结果
type TestRawQueryResult struct {
	Count  int64   `gom:"count"`
	AvgAge float64 `gom:"avg_age"`
}

func setupTestTables(t *testing.T, db *DB) error {
	// Create test tables
	err := db.Chain().CreateTable(&TestModel{})
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
		return err
	}

	err = db.Chain().CreateTable(&TestCategory{})
	if err != nil {
		t.Fatalf("Failed to create test category table: %v", err)
		return err
	}

	return nil
}

func TestComplexQueries(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer db.Close()

		runComplexQueriesTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runComplexQueriesTest(t, db)
	})
}

func runComplexQueriesTest(t *testing.T, db *DB) {
	// Create test tables
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	err = createTestCategoryTable(db)
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "test_categories")

	// Insert test data
	values := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "test" + strconv.Itoa(i%5),
			"age":        20 + (i % 10),
			"created_at": time.Now(),
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// Insert test categories
	categories := make([]map[string]interface{}, 0)
	for i := 0; i < 50; i++ {
		categories = append(categories, map[string]interface{}{
			"test_id":    i + 1,
			"category":   "category" + strconv.Itoa(i%3),
			"created_at": time.Now(),
		})
	}
	affected, err = db.Chain().Table("test_categories").BatchValues(categories).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(50), affected)

	// Run complex queries
	t.Run("Join_Query", func(t *testing.T) {
		query := `
			SELECT t.name, t.age, tc.category
			FROM tests t
			INNER JOIN test_categories tc ON t.id = tc.test_id
			ORDER BY t.id LIMIT 10
		`
		result := db.Chain().RawQuery(query)
		assert.NoError(t, result.Error)
		assert.NotEmpty(t, result.Data)

		// Verify the result structure
		firstRow := result.Data[0]
		assert.Contains(t, firstRow, "name")
		assert.Contains(t, firstRow, "age")
		assert.Contains(t, firstRow, "category")
	})
}

func TestAdvancedRawQueries(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupMySQLTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer db.Close()

		runAdvancedRawQueriesTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupPostgreSQLTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer db.Close()

		runAdvancedRawQueriesTest(t, db)
	})
}

func runAdvancedRawQueriesTest(t *testing.T, db *DB) {
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Insert test data
	values := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "test" + strconv.Itoa(i%5),
			"age":        20 + (i % 10),
			"created_at": time.Now(),
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	t.Run("Raw_Query", func(t *testing.T) {
		var query string
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = "SELECT COUNT(*) as count, AVG(CAST(age AS DECIMAL(10,2))) as avg_age FROM tests"
		} else {
			query = "SELECT COUNT(*) as count, AVG(age)::numeric as avg_age FROM tests"
		}
		result := db.Chain().RawQuery(query)
		assert.NoError(t, result.Error)

		// Handle count which could be int64 or []uint8
		var count int64
		switch v := result.Data[0]["count"].(type) {
		case int64:
			count = v
		case []uint8:
			count, err = strconv.ParseInt(string(v), 10, 64)
			assert.NoError(t, err)
		default:
			t.Fatalf("Unexpected type for count: %T", v)
		}
		assert.True(t, count > 0)

		// Handle avg_age which could be float64, []uint8, or string
		var avgAge float64
		switch v := result.Data[0]["avg_age"].(type) {
		case float64:
			avgAge = v
		case []uint8:
			avgAge, err = strconv.ParseFloat(string(v), 64)
			assert.NoError(t, err)
		case string:
			avgAge, err = strconv.ParseFloat(v, 64)
			assert.NoError(t, err)
		default:
			t.Fatalf("Unexpected type for avg_age: %T", v)
		}
		assert.True(t, avgAge > 0)
	})

	t.Run("Raw_Exec", func(t *testing.T) {
		var query string
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = "UPDATE tests SET age = age + 1 WHERE age < ?"
		} else {
			query = "UPDATE tests SET age = age + 1 WHERE age < $1"
		}
		result := db.Chain().RawExecute(query, 25)
		assert.NoError(t, result.Error)
		assert.True(t, result.Affected > 0)
	})
}

func TestJoinQueries(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping MySQL test due to database connection error")
			return
		}
		defer db.Close()

		runJoinQueriesTest(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer func() {
			_ = testutils.CleanupTestDB(db.DB, "tests")
			db.Close()
		}()
		runJoinQueriesTest(t, db)
	})
}

func runJoinQueriesTest(t *testing.T, db *DB) {
	// Create test tables
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Create another test table for joining
	var createTableSQL string
	if _, ok := db.Factory.(*mysql.Factory); ok {
		createTableSQL = `
			CREATE TABLE test_details (
				id INT PRIMARY KEY AUTO_INCREMENT,
				test_id INT,
				detail VARCHAR(255),
				created_at TIMESTAMP
			)
		`
	} else {
		createTableSQL = `
			CREATE TABLE test_details (
				id SERIAL PRIMARY KEY,
				test_id INT,
				detail VARCHAR(255),
				created_at TIMESTAMP
			)
		`
	}
	result := db.Chain().RawExecute(createTableSQL)
	assert.NoError(t, result.Error)
	defer testutils.CleanupTestDB(db.DB, "test_details")

	// Insert test data
	values := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]interface{}{
			"name":       "test",
			"age":        20 + (i % 10),
			"created_at": time.Now(),
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	// Insert details
	detailValues := make([]map[string]interface{}, 0)
	for i := 1; i <= 100; i++ {
		detailValues = append(detailValues, map[string]interface{}{
			"test_id":    i,
			"detail":     "detail for test " + string(rune('A'+i%26)),
			"created_at": time.Now(),
		})
	}
	affected, err = db.Chain().Table("test_details").BatchValues(detailValues).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	t.Run("Inner Join", func(t *testing.T) {
		var query string
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = `
				SELECT t.name, t.age, td.detail
				FROM tests t
				INNER JOIN test_details td ON t.id = td.test_id
				WHERE t.age > ?
				ORDER BY t.id ASC
			`
		} else {
			query = `
				SELECT t.name, t.age, td.detail
				FROM tests t
				INNER JOIN test_details td ON t.id = td.test_id
				WHERE t.age > $1
				ORDER BY t.id ASC
			`
		}
		result := db.Chain().RawQuery(query, 25)
		assert.NoError(t, result.Error)
		assert.True(t, len(result.Data) > 0)
	})

	t.Run("Left Join", func(t *testing.T) {
		var query string
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = `
				SELECT t.name, t.age, td.detail
				FROM tests t
				LEFT JOIN test_details td ON t.id = td.test_id
				WHERE t.age > ?
				ORDER BY t.id ASC
			`
		} else {
			query = `
				SELECT t.name, t.age, td.detail
				FROM tests t
				LEFT JOIN test_details td ON t.id = td.test_id
				WHERE t.age > $1
				ORDER BY t.id ASC
			`
		}
		result := db.Chain().RawQuery(query, 25)
		assert.NoError(t, result.Error)
		assert.True(t, len(result.Data) > 0)
	})
}
