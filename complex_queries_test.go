package gom

import (
	"strconv"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/kmlixh/gom/v4/factory/mysql"
	"github.com/kmlixh/gom/v4/testutils"
	"github.com/stretchr/testify/assert"
)

// TestCategory 测试分类模型
type TestCategory struct {
	ID        int64     `gom:"id,pk,auto_increment"`
	TestID    int64     `gom:"test_id"`
	Category  string    `gom:"category"`
	CreatedAt time.Time `gom:"created_at"`
}

// TestRawQueryResult 原始查询结果
type TestRawQueryResult struct {
	Count  int     `gom:"count"`
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
	// Create test table
	err := db.Chain().CreateTable(&TestModel{})
	assert.NoError(t, err)
	defer testutils.CleanupTestDB(db.DB, "tests")

	// Create additional test table for joins
	type TestCategory struct {
		ID        int64     `sql:"id,pk,auto_increment"`
		TestID    int64     `sql:"test_id"`
		Category  string    `sql:"category"`
		CreatedAt time.Time `sql:"created_at"`
	}
	err = db.Chain().CreateTable(&TestCategory{})
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

	// Insert category data
	categoryValues := make([]map[string]interface{}, 0)
	for i := 0; i < 5; i++ {
		categoryValues = append(categoryValues, map[string]interface{}{
			"test_id":    int64(i + 1),
			"category":   "category" + strconv.Itoa(i),
			"created_at": time.Now(),
		})
	}
	affected, err = db.Chain().Table("test_categories").BatchValues(categoryValues).BatchInsert(5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), affected)

	// Test complex queries
	t.Run("Aggregation", func(t *testing.T) {
		// Count with condition
		var query string
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = "SELECT COUNT(*) as count FROM tests WHERE age > ?"
		} else {
			query = "SELECT COUNT(*) as count FROM tests WHERE age > $1"
		}
		result := db.Chain().Table("tests").
			Where("age", define.OpGt, 25).
			RawQuery(query, 25)
		assert.NoError(t, result.Error)
		assert.Equal(t, int64(40), result.Data[0]["count"])

		// Multiple aggregations
		result = db.Chain().RawQuery(`
			SELECT 
				COUNT(*) as total_count,
				AVG(CAST(age AS DECIMAL(10,2))) as avg_age,
				MIN(age) as min_age,
				MAX(age) as max_age
			FROM tests
		`)
		assert.NoError(t, result.Error)
		data := result.Data[0]
		assert.Equal(t, int64(100), data["total_count"])
		assert.InDelta(t, 24.5, data["avg_age"], 0.1)
		assert.Equal(t, int64(20), data["min_age"])
		assert.Equal(t, int64(29), data["max_age"])

		// Conditional aggregation
		result = db.Chain().RawQuery(`
			SELECT 
				name,
				COUNT(*) as count,
				AVG(CAST(age AS DECIMAL(10,2))) as avg_age
			FROM tests
			GROUP BY name
			HAVING count > 10
			ORDER BY count DESC
		`)
		assert.NoError(t, result.Error)
		assert.Equal(t, 5, len(result.Data))
	})

	t.Run("Complex Joins", func(t *testing.T) {
		// Inner join with conditions
		var query string
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = `
				SELECT t.name, tc.category, COUNT(*) as count
				FROM tests t
				INNER JOIN test_categories tc ON t.id = tc.test_id
				WHERE t.age > ?
				GROUP BY t.name, tc.category
				ORDER BY count DESC
			`
		} else {
			query = `
				SELECT t.name, tc.category, COUNT(*) as count
				FROM tests t
				INNER JOIN test_categories tc ON t.id = tc.test_id
				WHERE t.age > $1
				GROUP BY t.name, tc.category
				ORDER BY count DESC
			`
		}
		result := db.Chain().RawQuery(query, 20)
		assert.NoError(t, result.Error)
		assert.Greater(t, len(result.Data), 0)

		// Left join with null check
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = `
				SELECT t.name, tc.category
				FROM tests t
				LEFT JOIN test_categories tc ON t.id = tc.test_id
				WHERE tc.category IS NULL
			`
		} else {
			query = `
				SELECT t.name, tc.category
				FROM tests t
				LEFT JOIN test_categories tc ON t.id = tc.test_id
				WHERE tc.category IS NULL
			`
		}
		result = db.Chain().RawQuery(query)
		assert.NoError(t, result.Error)
	})

	t.Run("Window Functions", func(t *testing.T) {
		// Row number
		var query string
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = `
				SELECT name, age,
					ROW_NUMBER() OVER (PARTITION BY name ORDER BY age) as row_num
				FROM tests
				LIMIT 10
			`
		} else {
			query = `
				SELECT name, age,
					ROW_NUMBER() OVER (PARTITION BY name ORDER BY age) as row_num
				FROM tests
				LIMIT 10
			`
		}
		result := db.Chain().RawQuery(query)
		assert.NoError(t, result.Error)
		assert.Equal(t, 10, len(result.Data))

		// Running total
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = `
				SELECT name, age,
					SUM(age) OVER (PARTITION BY name ORDER BY age) as running_total
				FROM tests
				LIMIT 10
			`
		} else {
			query = `
				SELECT name, age,
					SUM(age) OVER (PARTITION BY name ORDER BY age) as running_total
				FROM tests
				LIMIT 10
			`
		}
		result = db.Chain().RawQuery(query)
		assert.NoError(t, result.Error)
		assert.Equal(t, 10, len(result.Data))
	})

	t.Run("Error Handling", func(t *testing.T) {
		// Invalid SQL syntax
		result := db.Chain().RawQuery("SELECT * FROMM tests")
		assert.Error(t, result.Error)

		// Invalid column name
		result = db.Chain().RawQuery("SELECT invalid_column FROM tests")
		assert.Error(t, result.Error)

		// Invalid table name
		result = db.Chain().RawQuery("SELECT * FROM invalid_table")
		assert.Error(t, result.Error)

		// Invalid group by
		result = db.Chain().RawQuery(`
			SELECT name, COUNT(*)
			FROM tests
			GROUP BY invalid_column
		`)
		assert.Error(t, result.Error)
	})

	t.Run("Complex Subqueries", func(t *testing.T) {
		// Subquery in SELECT
		var query string
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = `
				SELECT t1.name,
					(SELECT COUNT(*) FROM tests t2 WHERE t2.age > t1.age) as higher_age_count
				FROM tests t1
				LIMIT 5
			`
		} else {
			query = `
				SELECT t1.name,
					(SELECT COUNT(*) FROM tests t2 WHERE t2.age > t1.age) as higher_age_count
				FROM tests t1
				LIMIT 5
			`
		}
		result := db.Chain().RawQuery(query)
		assert.NoError(t, result.Error)
		assert.Equal(t, 5, len(result.Data))

		// Subquery in WHERE
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = `
				SELECT name, age
				FROM tests
				WHERE age > (SELECT AVG(age) FROM tests)
				LIMIT 5
			`
		} else {
			query = `
				SELECT name, age
				FROM tests
				WHERE age > (SELECT AVG(age) FROM tests)
				LIMIT 5
			`
		}
		result = db.Chain().RawQuery(query)
		assert.NoError(t, result.Error)
		assert.Greater(t, len(result.Data), 0)
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

		err := setupTestTables(t, db)
		if err != nil {
			t.Fatalf("Failed to setup test tables: %v", err)
			return
		}

		t.Run("Raw_Query", func(t *testing.T) {
			query := `
				SELECT COUNT(*) as count, AVG(age) as avg_age
				FROM tests
			`
			var result TestRawQueryResult
			err := db.Chain().Raw(query).Into(&result)
			assert.NoError(t, err)
			assert.True(t, result.AvgAge >= 0)
		})

		t.Run("Raw_Exec", func(t *testing.T) {
			query := `
				UPDATE tests
				SET age = age + 1
				WHERE age < ?
			`
			result := db.Chain().Raw(query, 25).Exec()
			assert.NoError(t, result.Error)
		})
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := setupPostgreSQLTestDB(t)
		if db == nil {
			t.Skip("Skipping PostgreSQL test due to database connection error")
			return
		}
		defer db.Close()

		err := setupTestTables(t, db)
		if err != nil {
			t.Fatalf("Failed to setup test tables: %v", err)
			return
		}

		t.Run("Raw_Query", func(t *testing.T) {
			query := `
				SELECT COUNT(*) as count, AVG(age) as avg_age
				FROM tests
			`
			var result TestRawQueryResult
			err := db.Chain().Raw(query).Into(&result)
			assert.NoError(t, err)
			assert.True(t, result.AvgAge >= 0)
		})

		t.Run("Raw_Exec", func(t *testing.T) {
			query := `
				UPDATE tests
				SET age = age + 1
				WHERE age < ?
			`
			result := db.Chain().Raw(query, 25).Exec()
			assert.NoError(t, result.Error)
		})
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
			"name":       "test",
			"age":        20 + (i % 10),
			"created_at": time.Now(),
		})
	}
	affected, err := db.Chain().Table("tests").BatchValues(values).BatchInsert(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), affected)

	t.Run("Raw Query", func(t *testing.T) {
		result := db.Chain().RawQuery(`
			SELECT COUNT(*) as count, AVG(age) as avg_age 
			FROM tests
		`)
		assert.NoError(t, result.Error)
		assert.Equal(t, int64(100), result.Data[0]["count"])

		// Handle different types returned by MySQL and PostgreSQL
		avgAge := result.Data[0]["avg_age"]
		switch v := avgAge.(type) {
		case float64:
			assert.InDelta(t, 24.5, v, 0.1)
		case string:
			f, err := strconv.ParseFloat(v, 64)
			assert.NoError(t, err)
			assert.InDelta(t, 24.5, f, 0.1)
		default:
			t.Errorf("unexpected type for avg_age: %T", avgAge)
		}
	})

	t.Run("Raw Exec", func(t *testing.T) {
		var query string
		if _, ok := db.Factory.(*mysql.Factory); ok {
			query = `
				UPDATE tests 
				SET age = age + 1 
				WHERE age < ?
			`
		} else {
			query = `
				UPDATE tests 
				SET age = age + 1 
				WHERE age < $1
			`
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
