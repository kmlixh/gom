package define

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

type Order struct {
	Field string
	Type  OrderType
}

// TestFactoryImpl implements SQLFactory interface for testing
type TestFactoryImpl struct {
	dbType string
}

func (m *TestFactoryImpl) Connect(dsn string) (*sql.DB, error) {
	return nil, nil
}

func (m *TestFactoryImpl) GetType() string {
	return m.dbType
}

func (m *TestFactoryImpl) BuildSelect(table string, fields []string, conditions []*Condition, orders []*Order, limit, offset int) (string, []interface{}) {
	var args []interface{}
	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(fields, ", "), table)

	if len(conditions) > 0 {
		whereClause, whereArgs := m.buildWhereClause(conditions)
		sql += " WHERE " + whereClause
		args = append(args, whereArgs...)
	}

	if len(orders) > 0 {
		var orderClauses []string
		for _, order := range orders {
			orderClauses = append(orderClauses, fmt.Sprintf("%s %s", order.Field, order.Type))
		}
		sql += " ORDER BY " + strings.Join(orderClauses, ", ")
	}

	if limit > 0 {
		sql += " LIMIT ?"
		args = append(args, limit)
	}

	if offset >= 0 {
		sql += " OFFSET ?"
		args = append(args, offset)
	}

	return sql, args
}

func (m *TestFactoryImpl) buildWhereClause(conditions []*Condition) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", nil
	}

	var clauses []string
	var args []interface{}

	for i, cond := range conditions {
		if cond.SubConds != nil && len(cond.SubConds) > 0 {
			subClause, subArgs := m.buildWhereClause(cond.SubConds)
			if subClause != "" {
				if i > 0 {
					if cond.JoinType == JoinOr {
						clauses = append(clauses, "OR")
					} else {
						clauses = append(clauses, "AND")
					}
				}
				clauses = append(clauses, "("+subClause+")")
				args = append(args, subArgs...)
			}
			continue
		}

		if cond.Field != "" {
			clause, arg := m.buildConditionClause(cond)
			if i > 0 {
				if cond.JoinType == JoinOr {
					clauses = append(clauses, "OR")
				} else {
					clauses = append(clauses, "AND")
				}
			}
			clauses = append(clauses, clause)
			if arg != nil {
				args = append(args, arg...)
			}
		}
	}

	return strings.Join(clauses, " "), args
}

func (m *TestFactoryImpl) buildConditionClause(cond *Condition) (string, []interface{}) {
	switch cond.Op {
	case OpEq:
		return fmt.Sprintf("%s = ?", cond.Field), []interface{}{cond.Value}
	case OpGt:
		return fmt.Sprintf("%s > ?", cond.Field), []interface{}{cond.Value}
	case OpIn:
		if values, ok := cond.Value.([]string); ok {
			placeholders := make([]string, len(values))
			args := make([]interface{}, len(values))
			for i, v := range values {
				placeholders[i] = "?"
				args[i] = v
			}
			return fmt.Sprintf("%s IN (%s)", cond.Field, strings.Join(placeholders, ", ")), args
		}
	}
	return "", nil
}

func (m *TestFactoryImpl) BuildUpdate(table string, fields map[string]interface{}, fieldOrder []string, conditions []*Condition) (string, []interface{}) {
	var args []interface{}
	var setClauses []string

	for _, field := range fieldOrder {
		value := fields[field]
		if str, ok := value.(string); ok && strings.Contains(str, " ") {
			// Handle expressions like "amount * quantity"
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", field, str))
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", field))
			args = append(args, value)
		}
	}

	sql := fmt.Sprintf("UPDATE %s SET %s", table, strings.Join(setClauses, ", "))

	if len(conditions) > 0 {
		whereClause, whereArgs := m.buildWhereClause(conditions)
		sql += " WHERE " + whereClause
		args = append(args, whereArgs...)
	}

	return sql, args
}

func (m *TestFactoryImpl) BuildInsert(table string, fields map[string]interface{}, fieldOrder []string) (string, []interface{}) {
	return "INSERT INTO test_table (field) VALUES (?)", []interface{}{1}
}

func (m *TestFactoryImpl) BuildBatchInsert(table string, values []map[string]interface{}, fieldOrder []string) (string, []interface{}) {
	if len(values) == 0 {
		return "", nil
	}

	var args []interface{}
	var placeholders []string

	// Build field list
	fields := strings.Join(fieldOrder, ", ")

	// Build placeholders and collect args
	for _, value := range values {
		var rowPlaceholders []string
		for _, field := range fieldOrder {
			rowPlaceholders = append(rowPlaceholders, "?")
			args = append(args, value[field])
		}
		placeholders = append(placeholders, "("+strings.Join(rowPlaceholders, ", ")+")")
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, fields, strings.Join(placeholders, ", "))
	return sql, args
}

func (m *TestFactoryImpl) BuildDelete(table string, conditions []*Condition) (string, []interface{}) {
	return "DELETE FROM test_table WHERE field = ?", []interface{}{1}
}

func (m *TestFactoryImpl) BuildCreateTable(table string, modelType reflect.Type) string {
	return "CREATE TABLE test_table (id INT PRIMARY KEY)"
}

func (m *TestFactoryImpl) GetTableInfo(db *sql.DB, tableName string) (*TableInfo, error) {
	return &TableInfo{
		TableName:    "test_table",
		TableComment: "Test table",
		PrimaryKeys:  []string{"id"},
		Columns: []ColumnInfo{
			{
				Name:            "id",
				Type:            "INT",
				Length:          11,
				IsNullable:      false,
				IsPrimaryKey:    true,
				IsAutoIncrement: true,
				Comment:         "Primary key",
			},
		},
	}, nil
}

func (m *TestFactoryImpl) GetTables(db *sql.DB, pattern string) ([]string, error) {
	return []string{"test_table"}, nil
}

func (m *TestFactoryImpl) BuildOrderBy(orders []Order) (string, []interface{}) {
	if len(orders) == 0 {
		return "", nil
	}
	var orderStrings []string
	for _, order := range orders {
		orderStrings = append(orderStrings, fmt.Sprintf("%s %v", order.Field, order.Type))
	}
	return fmt.Sprintf("ORDER BY %s", strings.Join(orderStrings, ", ")), nil
}

func TestSQLFactoryComplexQueries(t *testing.T) {
	t.Run("Complex_Select_NestedConditions", func(t *testing.T) {
		factory := &TestFactoryImpl{}
		fields := []string{"id", "name", "age", "role", "department"}
		conditions := []*Condition{
			{
				JoinType: JoinAnd,
				SubConds: []*Condition{
					{Field: "age", Op: OpGt, Value: 30},
					{Field: "role", Op: OpEq, Value: "manager", JoinType: JoinAnd},
				},
			},
			{
				JoinType: JoinAnd,
				SubConds: []*Condition{
					{Field: "status", Op: OpEq, Value: "active"},
					{Field: "department", Op: OpIn, Value: []string{"IT", "HR"}, JoinType: JoinOr},
				},
			},
		}
		orders := []*Order{{Field: "name", Type: OrderAsc}}
		limit := 10
		offset := 0

		sql, args := factory.BuildSelect("employees", fields, conditions, orders, limit, offset)
		expectedSQL := "SELECT id, name, age, role, department FROM employees WHERE (age > ? AND role = ?) AND (status = ? OR department IN (?, ?)) ORDER BY name ASC LIMIT ? OFFSET ?"
		assert.Equal(t, expectedSQL, sql)
		assert.Equal(t, []interface{}{30, "manager", "active", "IT", "HR", 10, 0}, args)
	})

	t.Run("Complex_Update_WithCalculations", func(t *testing.T) {
		factory := &TestFactoryImpl{}
		fields := map[string]interface{}{
			"total_amount": "amount * quantity",
			"status":       "completed",
		}
		fieldOrder := []string{"total_amount", "status"}
		conditions := []*Condition{
			{Field: "order_id", Op: OpEq, Value: 123},
			{Field: "status", Op: OpEq, Value: "pending"},
		}

		sql, args := factory.BuildUpdate("orders", fields, fieldOrder, conditions)
		expectedSQL := "UPDATE orders SET total_amount = amount * quantity, status = ? WHERE order_id = ? AND status = ?"
		assert.Equal(t, expectedSQL, sql)
		assert.Equal(t, []interface{}{"completed", 123, "pending"}, args)
	})

	t.Run("Complex_BatchInsert_WithTypes", func(t *testing.T) {
		factory := &TestFactoryImpl{}
		fieldOrder := []string{"name", "price", "category", "is_active"}
		values := []map[string]interface{}{
			{
				"name":      "Product 1",
				"price":     99.99,
				"category":  "Electronics",
				"is_active": true,
			},
			{
				"name":      "Product 2",
				"price":     149.99,
				"category":  "Accessories",
				"is_active": true,
			},
		}

		sql, args := factory.BuildBatchInsert("products", values, fieldOrder)
		expectedSQL := "INSERT INTO products (name, price, category, is_active) VALUES (?, ?, ?, ?), (?, ?, ?, ?)"
		expectedArgs := []interface{}{
			"Product 1", 99.99, "Electronics", true,
			"Product 2", 149.99, "Accessories", true,
		}
		assert.Equal(t, expectedSQL, sql)
		assert.Equal(t, expectedArgs, args)
	})
}
