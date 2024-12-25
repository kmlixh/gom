package gom

import (
	"database/sql"
	"fmt"
	"github.com/kmlixh/gom/v4/define"
	"reflect"
	"strings"
)

// Chain represents the base chain structure
type Chain struct {
	db        *DB
	tableName string
	factory   define.SQLFactory
	conds     []*define.Condition
}

// UpdateChain represents a chain of update operations
type UpdateChain struct {
	*Chain
	fieldList []string
	valueList []interface{}
}

// InsertChain represents a chain of insert operations
type InsertChain struct {
	*Chain
	fieldList []string
	valueList []interface{}
	batchMode bool
}

// DeleteChain represents a chain of delete operations
type DeleteChain struct {
	*Chain
}

// QueryChain represents a chain of query operations
type QueryChain struct {
	*Chain
	fieldList   []string
	orderByExpr string
	limitCount  int
	offsetCount int
}

// Fields sets the fields to select
func (qc *QueryChain) Fields(fields ...string) *QueryChain {
	qc.fieldList = fields
	return qc
}

// OrderBy adds an ORDER BY clause
func (qc *QueryChain) OrderBy(expr string) *QueryChain {
	qc.orderByExpr = expr
	return qc
}

// OrderByDesc adds a descending ORDER BY clause
func (qc *QueryChain) OrderByDesc(field string) *QueryChain {
	qc.orderByExpr = field + " DESC"
	return qc
}

// Limit sets the LIMIT clause
func (qc *QueryChain) Limit(limit int) *QueryChain {
	qc.limitCount = limit
	return qc
}

// Offset sets the OFFSET clause
func (qc *QueryChain) Offset(offset int) *QueryChain {
	qc.offsetCount = offset
	return qc
}

// List executes the query and returns all results
func (qc *QueryChain) List() (*QueryResult, error) {
	if len(qc.fieldList) == 0 {
		qc.fieldList = []string{"*"}
	}

	where, args := qc.buildWhereClause()
	query := qc.factory.GenerateSelectSQL(qc.tableName, qc.fieldList, where, qc.orderByExpr, qc.limitCount, qc.offsetCount)
	rows, err := qc.db.ExecuteQuery(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	return &QueryResult{
		Data:    result,
		Columns: columns,
	}, nil
}

// First returns the first result
func (qc *QueryChain) First() (*QueryResult, error) {
	qc.limitCount = 1
	return qc.List()
}

// Last returns the last result
func (qc *QueryChain) Last() (*QueryResult, error) {
	if qc.orderByExpr == "" {
		if len(qc.fieldList) > 0 && qc.fieldList[0] != "*" {
			qc.orderByExpr = qc.fieldList[0] + " DESC"
		}
	} else {
		if !strings.Contains(strings.ToUpper(qc.orderByExpr), "DESC") {
			qc.orderByExpr += " DESC"
		}
	}
	qc.limitCount = 1
	return qc.List()
}

// One returns exactly one result, returns error if not found or found multiple
func (qc *QueryChain) One() (*QueryResult, error) {
	qc.limitCount = 2 // Get 2 to check if there are multiple results
	result, err := qc.List()
	if err != nil {
		return nil, err
	}

	if result.Empty() {
		return nil, fmt.Errorf("no record found")
	}

	if result.Size() > 1 {
		return nil, fmt.Errorf("multiple records found")
	}

	return &QueryResult{
		Data:    result.Data[:1],
		Columns: result.Columns,
	}, nil
}

// Count returns the count of results
func (qc *QueryChain) Count() (int64, error) {
	where, args := qc.buildWhereClause()
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", qc.tableName)
	if where != "" {
		countQuery += " WHERE " + where
	}

	rows, err := qc.db.ExecuteQuery(countQuery, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

// Exists returns true if any results exist
func (qc *QueryChain) Exists() (bool, error) {
	count, err := qc.Count()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Into scans the result into a struct
func (qc *QueryChain) Into(dest interface{}) error {
	result, err := qc.List()
	if err != nil {
		return err
	}
	return result.Into(dest)
}

// IntoOne scans exactly one result into a struct
func (qc *QueryChain) IntoOne(dest interface{}) error {
	result, err := qc.One()
	if err != nil {
		return err
	}
	return result.Into(dest)
}

// IntoFirst scans the first result into a struct
func (qc *QueryChain) IntoFirst(dest interface{}) error {
	result, err := qc.First()
	if err != nil {
		return err
	}
	return result.Into(dest)
}

// IntoLast scans the last result into a struct
func (qc *QueryChain) IntoLast(dest interface{}) error {
	result, err := qc.Last()
	if err != nil {
		return err
	}
	return result.Into(dest)
}

// Page represents a page of results
type Page struct {
	Data       []map[string]interface{} `json:"data"`
	Total      int64                    `json:"total"`
	PageSize   int                      `json:"page_size"`
	PageNumber int                      `json:"page_number"`
	TotalPages int                      `json:"total_pages"`
}

// PageResult represents a page of results with typed data
type PageResult struct {
	Data       interface{} `json:"data"`
	Total      int64       `json:"total"`
	PageSize   int         `json:"page_size"`
	PageNumber int         `json:"page_number"`
	TotalPages int         `json:"total_pages"`
}

// Page returns a page of results
func (qc *QueryChain) Page(pageNumber, pageSize int) (*Page, error) {
	// Get total count
	total, err := qc.Count()
	if err != nil {
		return nil, err
	}

	// Calculate total pages
	totalPages := int(total) / pageSize
	if int(total)%pageSize > 0 {
		totalPages++
	}

	// Get page data
	qc.Offset((pageNumber - 1) * pageSize).Limit(pageSize)
	result, err := qc.List()
	if err != nil {
		return nil, err
	}

	return &Page{
		Data:       result.Data,
		Total:      total,
		PageSize:   pageSize,
		PageNumber: pageNumber,
		TotalPages: totalPages,
	}, nil
}

// PageInto returns a page of results and scans them into a slice of structs
func (qc *QueryChain) PageInto(pageNumber, pageSize int, dest interface{}) (*PageResult, error) {
	// Get total count
	total, err := qc.Count()
	if err != nil {
		return nil, err
	}

	// Calculate total pages
	totalPages := int(total) / pageSize
	if int(total)%pageSize > 0 {
		totalPages++
	}

	// Get page data
	qc.Offset((pageNumber - 1) * pageSize).Limit(pageSize)
	if err := qc.Into(dest); err != nil {
		return nil, err
	}

	return &PageResult{
		Data:       dest,
		Total:      total,
		PageSize:   pageSize,
		PageNumber: pageNumber,
		TotalPages: totalPages,
	}, nil
}

// buildWhereClause builds the WHERE clause from conditions
func (c *Chain) buildWhereClause() (string, []interface{}) {
	if len(c.conds) == 0 {
		return "", nil
	}

	// Create a top-level AND condition containing all conditions
	topCond := define.NewAndCondition(c.conds...)
	return c.factory.BuildCondition(topCond)
}

// Where adds a raw WHERE condition
func (c *Chain) Where(where string, args ...interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Raw:  where,
		Args: args,
	})
	return c
}

// Where adds a raw WHERE condition for UpdateChain
func (uc *UpdateChain) Where(where string, args ...interface{}) *UpdateChain {
	uc.Chain.Where(where, args...)
	return uc
}

// Where adds a raw WHERE condition for InsertChain
func (ic *InsertChain) Where(where string, args ...interface{}) *InsertChain {
	ic.Chain.Where(where, args...)
	return ic
}

// Where adds a raw WHERE condition for DeleteChain
func (dc *DeleteChain) Where(where string, args ...interface{}) *DeleteChain {
	dc.Chain.Where(where, args...)
	return dc
}

// Where adds a WHERE condition for QueryChain
func (qc *QueryChain) Where(where string, args ...interface{}) *QueryChain {
	qc.Chain.Where(where, args...)
	return qc
}

// And adds an AND condition
func (c *Chain) And(cond string, args ...interface{}) *Chain {
	if len(c.conds) == 0 {
		return c.Where(cond, args...)
	}
	c.conds = append(c.conds, &define.Condition{
		Type: define.TypeAnd,
		SubConds: []*define.Condition{{
			Raw:  cond,
			Args: args,
		}},
	})
	return c
}

// Or adds an OR condition
func (c *Chain) Or(cond string, args ...interface{}) *Chain {
	if len(c.conds) == 0 {
		return c.Where(cond, args...)
	}
	c.conds = append(c.conds, &define.Condition{
		Type: define.TypeOr,
		SubConds: []*define.Condition{{
			Raw:  cond,
			Args: args,
		}},
	})
	return c
}

// Eq adds an equals condition
func (c *Chain) Eq(field string, value interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.Eq,
		Value:    value,
	})
	return c
}

// Ne adds a not equals condition
func (c *Chain) Ne(field string, value interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.Ne,
		Value:    value,
	})
	return c
}

// Gt adds a greater than condition
func (c *Chain) Gt(field string, value interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.Gt,
		Value:    value,
	})
	return c
}

// Gte adds a greater than or equals condition
func (c *Chain) Gte(field string, value interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.Gte,
		Value:    value,
	})
	return c
}

// Lt adds a less than condition
func (c *Chain) Lt(field string, value interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.Lt,
		Value:    value,
	})
	return c
}

// Lte adds a less than or equals condition
func (c *Chain) Lte(field string, value interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.Lte,
		Value:    value,
	})
	return c
}

// Like adds a LIKE condition
func (c *Chain) Like(field string, value string) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.Like,
		Value:    value,
	})
	return c
}

// LikeLeft adds a left LIKE condition
func (c *Chain) LikeLeft(field string, value string) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.LikeLeft,
		Value:    value,
	})
	return c
}

// LikeRight adds a right LIKE condition
func (c *Chain) LikeRight(field string, value string) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.LikeRight,
		Value:    value,
	})
	return c
}

// In adds an IN condition
func (c *Chain) In(field string, values ...interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.In,
		Values:   values,
	})
	return c
}

// NotIn adds a NOT IN condition
func (c *Chain) NotIn(field string, values ...interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.NotIn,
		Values:   values,
	})
	return c
}

// IsNull adds an IS NULL condition
func (c *Chain) IsNull(field string) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.IsNull,
	})
	return c
}

// IsNotNull adds an IS NOT NULL condition
func (c *Chain) IsNotNull(field string) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.IsNotNull,
	})
	return c
}

// Between adds a BETWEEN condition
func (c *Chain) Between(field string, start, end interface{}) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.Between,
		Values:   []interface{}{start, end},
	})
	return c
}

// Fields sets the fields for update
func (uc *UpdateChain) Fields(fields ...string) *UpdateChain {
	uc.fieldList = fields
	return uc
}

// Values sets the values for update
func (uc *UpdateChain) Values(values ...interface{}) *UpdateChain {
	uc.valueList = values
	return uc
}

// Execute executes the update operation
func (uc *UpdateChain) Execute() (sql.Result, error) {
	if len(uc.fieldList) == 0 {
		return nil, fmt.Errorf("no fields specified for update")
	}
	if len(uc.fieldList) != len(uc.valueList) {
		return nil, fmt.Errorf("number of fields (%d) does not match number of values (%d)", len(uc.fieldList), len(uc.valueList))
	}

	where, args := uc.buildWhereClause()
	query := uc.factory.GenerateUpdateSQL(uc.tableName, uc.fieldList, where)
	args = append(uc.valueList, args...)
	return uc.db.Execute(query, args...)
}

// Fields sets the fields for insert
func (ic *InsertChain) Fields(fields ...string) *InsertChain {
	ic.fieldList = fields
	return ic
}

// Values sets the values for insert
func (ic *InsertChain) Values(values ...interface{}) *InsertChain {
	ic.valueList = values
	ic.batchMode = false
	return ic
}

// BatchValues sets multiple rows of values for batch insert
func (ic *InsertChain) BatchValues(values [][]interface{}) *InsertChain {
	ic.batchMode = true
	flatValues := make([]interface{}, 0, len(values)*len(ic.fieldList))
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}
	ic.valueList = flatValues
	return ic
}

// Execute executes the insert operation
func (ic *InsertChain) Execute() (sql.Result, error) {
	if len(ic.fieldList) == 0 {
		return nil, fmt.Errorf("no fields specified for insert")
	}

	if ic.batchMode {
		if len(ic.valueList)%len(ic.fieldList) != 0 {
			return nil, fmt.Errorf("batch values count is not a multiple of field count")
		}
		query := ic.factory.GenerateBatchInsertSQL(ic.tableName, ic.fieldList, len(ic.valueList)/len(ic.fieldList))
		return ic.db.Execute(query, ic.valueList...)
	}

	if len(ic.fieldList) != len(ic.valueList) {
		return nil, fmt.Errorf("number of fields (%d) does not match number of values (%d)", len(ic.fieldList), len(ic.valueList))
	}
	query := ic.factory.GenerateInsertSQL(ic.tableName, ic.fieldList)
	return ic.db.Execute(query, ic.valueList...)
}

// Execute executes the delete operation
func (dc *DeleteChain) Execute() (sql.Result, error) {
	where, args := dc.buildWhereClause()
	query := dc.factory.GenerateDeleteSQL(dc.tableName, where)
	return dc.db.Execute(query, args...)
}

// Model sets the fields and values from a struct
func (ic *InsertChain) Model(model interface{}) *InsertChain {
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ic
	}

	t := v.Type()
	fields := make([]string, 0)
	values := make([]interface{}, 0)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}

		fields = append(fields, tag)
		values = append(values, v.Field(i).Interface())
	}

	return ic.Fields(fields...).Values(values...)
}

// Models sets multiple models for batch insert
func (ic *InsertChain) Models(models interface{}) *InsertChain {
	v := reflect.ValueOf(models)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice {
		return ic
	}

	if v.Len() == 0 {
		return ic
	}

	// Get fields from the first model
	first := v.Index(0)
	if first.Kind() == reflect.Ptr {
		first = first.Elem()
	}
	if first.Kind() != reflect.Struct {
		return ic
	}

	t := first.Type()
	fields := make([]string, 0)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}
		fields = append(fields, tag)
	}

	// Get values from all models
	values := make([][]interface{}, v.Len())
	for i := 0; i < v.Len(); i++ {
		model := v.Index(i)
		if model.Kind() == reflect.Ptr {
			model = model.Elem()
		}
		if model.Kind() != reflect.Struct {
			continue
		}

		modelValues := make([]interface{}, 0, len(fields))
		for j := 0; j < t.NumField(); j++ {
			field := t.Field(j)
			tag := field.Tag.Get("gom")
			if tag == "" || tag == "-" {
				continue
			}
			modelValues = append(modelValues, model.Field(j).Interface())
		}
		values[i] = modelValues
	}

	return ic.Fields(fields...).BatchValues(values)
}

// Model sets the fields and values from a struct
func (uc *UpdateChain) Model(model interface{}, excludeFields ...string) *UpdateChain {
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return uc
	}

	// Create a map of excluded fields for faster lookup
	excludeMap := make(map[string]bool)
	for _, field := range excludeFields {
		excludeMap[field] = true
	}

	t := v.Type()
	fields := make([]string, 0)
	values := make([]interface{}, 0)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}

		// Skip excluded fields
		if excludeMap[tag] {
			continue
		}

		fields = append(fields, tag)
		values = append(values, v.Field(i).Interface())
	}

	return uc.Fields(fields...).Values(values...)
}

// ModelWithFields sets the fields and values from a struct, only including specified fields
func (uc *UpdateChain) ModelWithFields(model interface{}, includeFields ...string) *UpdateChain {
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return uc
	}

	// Create a map of included fields for faster lookup
	includeMap := make(map[string]bool)
	for _, field := range includeFields {
		includeMap[field] = true
	}

	t := v.Type()
	fields := make([]string, 0)
	values := make([]interface{}, 0)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}

		// Only include specified fields
		if !includeMap[tag] {
			continue
		}

		fields = append(fields, tag)
		values = append(values, v.Field(i).Interface())
	}

	return uc.Fields(fields...).Values(values...)
}

// In adds an IN condition for DeleteChain
func (dc *DeleteChain) In(field string, values ...interface{}) *DeleteChain {
	dc.Chain.In(field, values...)
	return dc
}

// In adds an IN condition for UpdateChain
func (uc *UpdateChain) In(field string, values ...interface{}) *UpdateChain {
	uc.Chain.In(field, values...)
	return uc
}

// In adds an IN condition for QueryChain
func (qc *QueryChain) In(field string, values ...interface{}) *QueryChain {
	qc.Chain.In(field, values...)
	return qc
}

// In adds an IN condition for InsertChain
func (ic *InsertChain) In(field string, values ...interface{}) *InsertChain {
	ic.Chain.In(field, values...)
	return ic
}

// And adds an AND condition for QueryChain
func (qc *QueryChain) And(cond string, args ...interface{}) *QueryChain {
	qc.Chain.And(cond, args...)
	return qc
}

// And adds an AND condition for UpdateChain
func (uc *UpdateChain) And(cond string, args ...interface{}) *UpdateChain {
	uc.Chain.And(cond, args...)
	return uc
}

// And adds an AND condition for DeleteChain
func (dc *DeleteChain) And(cond string, args ...interface{}) *DeleteChain {
	dc.Chain.And(cond, args...)
	return dc
}

// And adds an AND condition for InsertChain
func (ic *InsertChain) And(cond string, args ...interface{}) *InsertChain {
	ic.Chain.And(cond, args...)
	return ic
}

// Group adds a condition group
func (c *Chain) Group(condType define.ConditionType, conditions ...*define.Condition) *Chain {
	if condType == define.TypeAnd {
		c.conds = append(c.conds, define.NewAndCondition(conditions...))
	} else {
		c.conds = append(c.conds, define.NewOrCondition(conditions...))
	}
	return c
}

// Group adds a condition group for QueryChain
func (qc *QueryChain) Group(condType define.ConditionType, conditions ...*define.Condition) *QueryChain {
	qc.Chain.Group(condType, conditions...)
	return qc
}

// Group adds a condition group for UpdateChain
func (uc *UpdateChain) Group(condType define.ConditionType, conditions ...*define.Condition) *UpdateChain {
	uc.Chain.Group(condType, conditions...)
	return uc
}

// Group adds a condition group for DeleteChain
func (dc *DeleteChain) Group(condType define.ConditionType, conditions ...*define.Condition) *DeleteChain {
	dc.Chain.Group(condType, conditions...)
	return dc
}

// Eq adds an equals condition for QueryChain
func (qc *QueryChain) Eq(field string, value interface{}) *QueryChain {
	qc.Chain.Eq(field, value)
	return qc
}

// Ne adds a not equals condition for QueryChain
func (qc *QueryChain) Ne(field string, value interface{}) *QueryChain {
	qc.Chain.Ne(field, value)
	return qc
}

// Gt adds a greater than condition for QueryChain
func (qc *QueryChain) Gt(field string, value interface{}) *QueryChain {
	qc.Chain.Gt(field, value)
	return qc
}

// Lt adds a less than condition for QueryChain
func (qc *QueryChain) Lt(field string, value interface{}) *QueryChain {
	qc.Chain.Lt(field, value)
	return qc
}

// Gte adds a greater than or equals condition for QueryChain
func (qc *QueryChain) Gte(field string, value interface{}) *QueryChain {
	qc.Chain.Gte(field, value)
	return qc
}

// Lte adds a less than or equals condition for QueryChain
func (qc *QueryChain) Lte(field string, value interface{}) *QueryChain {
	qc.Chain.Lte(field, value)
	return qc
}

// Like adds a LIKE condition for QueryChain
func (qc *QueryChain) Like(field string, value string) *QueryChain {
	qc.Chain.Like(field, value)
	return qc
}

// NotLike adds a NOT LIKE condition for QueryChain
func (qc *QueryChain) NotLike(field string, value string) *QueryChain {
	qc.Chain.NotLike(field, value)
	return qc
}

// LikeLeft adds a left LIKE condition for QueryChain
func (qc *QueryChain) LikeLeft(field string, value string) *QueryChain {
	qc.Chain.LikeLeft(field, value)
	return qc
}

// LikeRight adds a right LIKE condition for QueryChain
func (qc *QueryChain) LikeRight(field string, value string) *QueryChain {
	qc.Chain.LikeRight(field, value)
	return qc
}

// Between adds a BETWEEN condition for QueryChain
func (qc *QueryChain) Between(field string, start, end interface{}) *QueryChain {
	qc.Chain.Between(field, start, end)
	return qc
}

// IsNull adds an IS NULL condition for QueryChain
func (qc *QueryChain) IsNull(field string) *QueryChain {
	qc.Chain.IsNull(field)
	return qc
}

// IsNotNull adds an IS NOT NULL condition for QueryChain
func (qc *QueryChain) IsNotNull(field string) *QueryChain {
	qc.Chain.IsNotNull(field)
	return qc
}

// NotLike adds a NOT LIKE condition
func (c *Chain) NotLike(field string, value string) *Chain {
	c.conds = append(c.conds, &define.Condition{
		Field:    field,
		Operator: define.NotLike,
		Value:    value,
	})
	return c
}

// NotIn adds a NOT IN condition for QueryChain
func (qc *QueryChain) NotIn(field string, values ...interface{}) *QueryChain {
	qc.Chain.NotIn(field, values...)
	return qc
}

// Between adds a BETWEEN condition for QueryChain
