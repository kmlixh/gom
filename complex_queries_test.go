package gom

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Employee represents an employee in the database
type Employee struct {
	Id           int64     `gom:"id,primary_key,auto_increment"`
	Name         string    `gom:"name"`
	Age          int       `gom:"age"`
	DepartmentId int64     `gom:"department_id"`
	CreatedAt    time.Time `gom:"created_at"`
}

func (e *Employee) TableName() string {
	return "employees"
}

// Department represents a department in the database
type Department struct {
	Id   int64  `gom:"id,primary_key,auto_increment"`
	Name string `gom:"name"`
}

// EmployeeWithDept represents a joined result of employee and department
type EmployeeWithDept struct {
	Name         string `gom:"name"`
	Age          int    `gom:"age"`
	DepartmentId int64  `gom:"department_id"`
	DeptName     string `gom:"dept_name"`
}

// CountResult represents a count query result
type CountResult struct {
	Count int64 `gom:"count"`
}

func getDb() *DB {
	return setupTestDB(nil)
}

func TestComplexQueries(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test tables
	err := db.Chain().CreateTable(&Employee{})
	assert.NoError(t, err)
	err = db.Chain().CreateTable(&Department{})
	assert.NoError(t, err)

	// Clean up existing data
	err = db.Chain().Table("employees").Delete().Error
	assert.NoError(t, err)
	err = db.Chain().Table("departments").Delete().Error
	assert.NoError(t, err)

	// Insert test departments
	departments := []Department{
		{Name: "Engineering"},
		{Name: "Marketing"},
		{Name: "Sales"},
		{Name: "HR"},
	}

	// Insert departments and store their IDs
	var departmentIds []int64
	for _, dept := range departments {
		result := db.Chain().Table("departments").From(&dept).Save()
		assert.NoError(t, err)
		t.Logf("Inserted department: %+v with ID: %d", dept, result.ID)
		departmentIds = append(departmentIds, result.ID)
	}

	// Insert test employees using the actual department IDs
	employees := []Employee{
		{Name: "Alice", Age: 25, DepartmentId: departmentIds[0], CreatedAt: time.Now()},
		{Name: "Bob", Age: 30, DepartmentId: departmentIds[0], CreatedAt: time.Now()},
		{Name: "Charlie", Age: 35, DepartmentId: departmentIds[0], CreatedAt: time.Now()},
		{Name: "David", Age: 40, DepartmentId: departmentIds[1], CreatedAt: time.Now()},
		{Name: "Eve", Age: 45, DepartmentId: departmentIds[1], CreatedAt: time.Now()},
		{Name: "Frank", Age: 50, DepartmentId: departmentIds[2], CreatedAt: time.Now()},
		{Name: "Grace", Age: 55, DepartmentId: departmentIds[2], CreatedAt: time.Now()},
		{Name: "Henry", Age: 28, DepartmentId: departmentIds[3], CreatedAt: time.Now()},
	}

	for _, emp := range employees {
		result := db.Chain().Table("employees").From(&emp).Save()
		assert.NoError(t, result.Error)
		t.Logf("Inserted employee: %+v with ID: %d", emp, result.ID)
	}

	// Verify data insertion
	var deptCount int64
	deptCount, err = db.Chain().Table("departments").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), deptCount, "Expected 4 departments to be inserted")

	var empCount int64
	empCount, err = db.Chain().Table("employees").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(8), empCount, "Expected 8 employees to be inserted")

	// Test count query
	count, err := db.Chain().Table("employees").Count()
	assert.NoError(t, err)
	t.Logf("Total employees: %d", count)
	assert.Equal(t, int64(8), count)

	// Test complex join query with conditions
	type EmployeeWithDept struct {
		Name         string `gom:"name"`
		Age          int    `gom:"age"`
		DepartmentId int64  `gom:"department_id"`
		DeptName     string `gom:"dept_name"`
	}

	var results []EmployeeWithDept
	query := `
		SELECT e.name, e.age, e.department_id, d.name AS dept_name 
		FROM employees AS e 
		INNER JOIN departments AS d ON e.department_id = d.id 
		WHERE e.age > 30 
		ORDER BY e.age ASC`

	err = db.Chain().RawQuery(query).Into(&results)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(results))

	if len(results) >= 5 {
		assert.Equal(t, "Charlie", results[0].Name)
		assert.Equal(t, 35, results[0].Age)
		assert.Equal(t, "Engineering", results[0].DeptName)

		assert.Equal(t, "David", results[1].Name)
		assert.Equal(t, 40, results[1].Age)
		assert.Equal(t, "Marketing", results[1].DeptName)

		assert.Equal(t, "Eve", results[2].Name)
		assert.Equal(t, 45, results[2].Age)
		assert.Equal(t, "Marketing", results[2].DeptName)

		assert.Equal(t, "Frank", results[3].Name)
		assert.Equal(t, 50, results[3].Age)
		assert.Equal(t, "Sales", results[3].DeptName)

		assert.Equal(t, "Grace", results[4].Name)
		assert.Equal(t, 55, results[4].Age)
		assert.Equal(t, "Sales", results[4].DeptName)
	}

	// Test aggregation query
	type DeptStats struct {
		DeptName string  `gom:"dept_name"`
		EmpCount int64   `gom:"emp_count"`
		AvgAge   float64 `gom:"avg_age"`
	}

	var deptStats []DeptStats
	query = `
		SELECT d.name AS dept_name, COUNT(*) AS emp_count, AVG(e.age) AS avg_age 
		FROM departments AS d 
		INNER JOIN employees AS e ON e.department_id = d.id 
		GROUP BY d.name 
		ORDER BY d.name ASC`

	err = db.Chain().RawQuery(query).Into(&deptStats)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(deptStats))

	if len(deptStats) >= 4 {
		assert.Equal(t, "Engineering", deptStats[0].DeptName)
		assert.Equal(t, int64(3), deptStats[0].EmpCount)
		assert.InDelta(t, 30.0, deptStats[0].AvgAge, 0.1)

		assert.Equal(t, "HR", deptStats[1].DeptName)
		assert.Equal(t, int64(1), deptStats[1].EmpCount)
		assert.InDelta(t, 28.0, deptStats[1].AvgAge, 0.1)

		assert.Equal(t, "Marketing", deptStats[2].DeptName)
		assert.Equal(t, int64(2), deptStats[2].EmpCount)
		assert.InDelta(t, 42.5, deptStats[2].AvgAge, 0.1)

		assert.Equal(t, "Sales", deptStats[3].DeptName)
		assert.Equal(t, int64(2), deptStats[3].EmpCount)
		assert.InDelta(t, 52.5, deptStats[3].AvgAge, 0.1)
	}
}

// TestAdvancedRawQueries tests more complex raw queries with struct mapping
func TestAdvancedRawQueries(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test tables
	err := db.Chain().CreateTable(&Employee{})
	assert.NoError(t, err)
	err = db.Chain().CreateTable(&Department{})
	assert.NoError(t, err)

	// Clean up existing data
	err = db.Chain().Table("employees").Delete().Error
	assert.NoError(t, err)
	err = db.Chain().Table("departments").Delete().Error
	assert.NoError(t, err)

	// Insert test departments
	departments := []Department{
		{Name: "Engineering"},
		{Name: "Marketing"},
		{Name: "Sales"},
		{Name: "HR"},
	}

	// Insert departments and store their IDs
	var departmentIds []int64
	for _, dept := range departments {
		result := db.Chain().Table("departments").From(&dept).Save()
		assert.NoError(t, err)
		t.Logf("Inserted department: %+v with ID: %d", dept, result.ID)
		departmentIds = append(departmentIds, result.ID)
	}

	// Insert test employees using the actual department IDs
	employees := []Employee{
		{Name: "Alice", Age: 25, DepartmentId: departmentIds[0], CreatedAt: time.Now()},
		{Name: "Bob", Age: 30, DepartmentId: departmentIds[0], CreatedAt: time.Now()},
		{Name: "Charlie", Age: 35, DepartmentId: departmentIds[0], CreatedAt: time.Now()},
		{Name: "David", Age: 40, DepartmentId: departmentIds[1], CreatedAt: time.Now()},
		{Name: "Eve", Age: 45, DepartmentId: departmentIds[1], CreatedAt: time.Now()},
		{Name: "Frank", Age: 50, DepartmentId: departmentIds[2], CreatedAt: time.Now()},
		{Name: "Grace", Age: 55, DepartmentId: departmentIds[2], CreatedAt: time.Now()},
		{Name: "Henry", Age: 28, DepartmentId: departmentIds[3], CreatedAt: time.Now()},
	}

	for _, emp := range employees {
		result := db.Chain().Table("employees").From(&emp).Save()
		assert.NoError(t, result.Error)
		t.Logf("Inserted employee: %+v with ID: %d", emp, result.ID)
	}

	// Test complex aggregation with HAVING clause using RawQuery
	type DeptAgeStats struct {
		DeptName   string  `gom:"dept_name"`
		EmpCount   int64   `gom:"emp_count"`
		AverageAge float64 `gom:"avg_age"`
		MinAge     int     `gom:"min_age"`
		MaxAge     int     `gom:"max_age"`
	}

	var ageStats []DeptAgeStats
	query := `
		SELECT 
			d.name AS dept_name,
			COUNT(*) AS emp_count,
			AVG(e.age) AS avg_age,
			MIN(e.age) AS min_age,
			MAX(e.age) AS max_age
		FROM departments AS d
		INNER JOIN employees AS e ON e.department_id = d.id
		GROUP BY d.name
		HAVING COUNT(*) > 1
		ORDER BY avg_age DESC`

	err = db.Chain().RawQuery(query).Into(&ageStats)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(ageStats))

	if len(ageStats) >= 3 {
		// Sales department
		assert.Equal(t, "Sales", ageStats[0].DeptName)
		assert.Equal(t, int64(2), ageStats[0].EmpCount)
		assert.InDelta(t, 52.5, ageStats[0].AverageAge, 0.1)
		assert.Equal(t, 50, ageStats[0].MinAge)
		assert.Equal(t, 55, ageStats[0].MaxAge)

		// Marketing department
		assert.Equal(t, "Marketing", ageStats[1].DeptName)
		assert.Equal(t, int64(2), ageStats[1].EmpCount)
		assert.InDelta(t, 42.5, ageStats[1].AverageAge, 0.1)
		assert.Equal(t, 40, ageStats[1].MinAge)
		assert.Equal(t, 45, ageStats[1].MaxAge)

		// Engineering department
		assert.Equal(t, "Engineering", ageStats[2].DeptName)
		assert.Equal(t, int64(3), ageStats[2].EmpCount)
		assert.InDelta(t, 30.0, ageStats[2].AverageAge, 0.1)
		assert.Equal(t, 25, ageStats[2].MinAge)
		assert.Equal(t, 35, ageStats[2].MaxAge)
	}

	// Test subquery with EXISTS clause
	type EmployeeInfo struct {
		Name    string `gom:"name"`
		Age     int    `gom:"age"`
		HasTeam bool   `gom:"has_team"`
	}

	var empInfo []EmployeeInfo
	query = `
		SELECT 
			e.name,
			e.age,
			EXISTS (
				SELECT 1 
				FROM employees e2 
				WHERE e2.department_id = e.department_id 
				AND e2.id != e.id
			) AS has_team
		FROM employees e
		ORDER BY e.name`

	err = db.Chain().RawQuery(query).Into(&empInfo)
	assert.NoError(t, err)
	assert.Equal(t, 8, len(empInfo))

	if len(empInfo) >= 8 {
		assert.Equal(t, "Alice", empInfo[0].Name)
		assert.True(t, empInfo[0].HasTeam)

		assert.Equal(t, "Henry", empInfo[7].Name)
		assert.False(t, empInfo[7].HasTeam)
	}
}

func TestJoinQueries(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Skip("Skipping test due to database connection error")
		return
	}
	defer db.Close()

	// Create test tables
	err := db.Chain().CreateTable(&Employee{})
	assert.NoError(t, err)

	err = db.Chain().CreateTable(&Department{})
	assert.NoError(t, err)

	// Clean up existing data
	err = db.Chain().Table("employees").Delete().Error
	assert.NoError(t, err)

	err = db.Chain().Table("departments").Delete().Error
	assert.NoError(t, err)

	// Insert departments
	departments := []Department{
		{Name: "Engineering"},
		{Name: "Marketing"},
		{Name: "Sales"},
	}

	var deptIds []int64
	for _, dept := range departments {
		result := db.Chain().Table("departments").From(&dept).Save()
		assert.NoError(t, result.Error)
		if result.Error == nil {
			lastId, err := result.LastInsertId()
			assert.NoError(t, err)
			deptIds = append(deptIds, lastId)
		}
	}

	// Verify department IDs
	assert.Equal(t, 3, len(deptIds), "Expected 3 department IDs")

	// Insert employees
	employees := []Employee{
		{Name: "Alice", Age: 25, DepartmentId: deptIds[0], CreatedAt: time.Now()},
		{Name: "Bob", Age: 30, DepartmentId: deptIds[0], CreatedAt: time.Now()},
		{Name: "Charlie", Age: 35, DepartmentId: deptIds[1], CreatedAt: time.Now()},
		{Name: "David", Age: 40, DepartmentId: deptIds[1], CreatedAt: time.Now()},
		{Name: "Eve", Age: 45, DepartmentId: deptIds[2], CreatedAt: time.Now()},
	}

	for _, emp := range employees {
		result := db.Chain().Table("employees").From(&emp).Save()
		assert.NoError(t, result.Error)
	}

	// Verify data insertion
	var deptCount int64
	deptCount, err = db.Chain().Table("departments").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), deptCount, "Expected 3 departments to be inserted")

	var empCount int64
	empCount, err = db.Chain().Table("employees").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), empCount, "Expected 5 employees to be inserted")

	// Test JOIN query
	var results []EmployeeWithDept
	query := `
		SELECT e.name, e.age, e.department_id, d.name AS dept_name 
			FROM employees e 
			INNER JOIN departments d ON e.department_id = d.id 
			WHERE e.age > 30 
			ORDER BY e.age ASC`

	err = db.Chain().RawQuery(query).Into(&results)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results), "Expected 3 results from JOIN query")

	if len(results) >= 3 {
		assert.Equal(t, "Charlie", results[0].Name)
		assert.Equal(t, 35, results[0].Age)
		assert.Equal(t, "Marketing", results[0].DeptName)

		assert.Equal(t, "David", results[1].Name)
		assert.Equal(t, 40, results[1].Age)
		assert.Equal(t, "Marketing", results[1].DeptName)

		assert.Equal(t, "Eve", results[2].Name)
		assert.Equal(t, 45, results[2].Age)
		assert.Equal(t, "Sales", results[2].DeptName)
	}

	// Test JOIN query with aggregation
	type DeptStats struct {
		DeptName string  `gom:"dept_name"`
		EmpCount int64   `gom:"emp_count"`
		AvgAge   float64 `gom:"avg_age"`
	}

	var deptStats []DeptStats
	query = `
		SELECT d.name AS dept_name, COUNT(*) AS emp_count, AVG(e.age) AS avg_age 
			FROM departments d 
			INNER JOIN employees e ON e.department_id = d.id 
			GROUP BY d.name 
			ORDER BY d.name ASC`

	err = db.Chain().RawQuery(query).Into(&deptStats)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(deptStats), "Expected 3 department stats")

	if len(deptStats) >= 3 {
		assert.Equal(t, "Engineering", deptStats[0].DeptName)
		assert.Equal(t, int64(2), deptStats[0].EmpCount)
		assert.InDelta(t, 27.5, deptStats[0].AvgAge, 0.1)

		assert.Equal(t, "Marketing", deptStats[1].DeptName)
		assert.Equal(t, int64(2), deptStats[1].EmpCount)
		assert.InDelta(t, 37.5, deptStats[1].AvgAge, 0.1)

		assert.Equal(t, "Sales", deptStats[2].DeptName)
		assert.Equal(t, int64(1), deptStats[2].EmpCount)
		assert.InDelta(t, 45.0, deptStats[2].AvgAge, 0.1)
	}
}
