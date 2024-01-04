package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type DefaultStruct struct {
}

type ITableName interface {
	TableName() string
}

type Column struct {
	ColumnName  string
	Primary     bool
	PrimaryAuto bool //If Primary Key Auto Generate Or2 Not
	ColumnType  string
}
type FieldInfo struct {
	FieldName string
	FieldType reflect.Type
}

type SqlProto struct {
	PreparedSql string
	Data        []interface{}
}

type SqlFunc func(model ...TableModel) []SqlProto
type SqlFactory interface {
	GetColumns(tableName string, db *sql.DB) ([]Column, error)
	GetSqlFunc(sqlType SqlType) SqlFunc
	ConditionToSql(preTag bool, condition Condition) (string, []interface{})
}

type OrderType int

type SqlType int

const (
	_ SqlType = iota
	Query
	Insert
	Update
	Delete
)

const (
	_ OrderType = iota
	Asc
	Desc
)

type OrderBy interface {
	Name() string
	Type() OrderType
}

type OrderByImpl struct {
	name      string
	orderType OrderType
}
type CommonSqlResult struct {
	lastInsertId int64
	rowsAffected int64
	error
}
type RawMetaInfo struct {
	reflect.Type
	TableName string
	IsSlice   bool
	IsPtr     bool
	IsStruct  bool
	RawData   reflect.Value
}

func (c CommonSqlResult) LastInsertId() (int64, error) {
	if c.lastInsertId == 0 {
		return 0, errors.New("")
	}
	return c.lastInsertId, c.error
}

func (c CommonSqlResult) RowsAffected() (int64, error) {
	return c.rowsAffected, c.error
}

func MakeOrderBy(name string, orderType OrderType) OrderBy {
	return OrderByImpl{name, orderType}
}
func (o OrderByImpl) Name() string {
	return o.name
}
func (o OrderByImpl) Type() OrderType {
	return o.orderType
}

type PageInfo interface {
	Page() (int64, int64)
}

type PageImpl struct {
	index int64
	size  int64
}

func MakePage(page int64, size int64) PageInfo {
	if page <= 0 {
		page = 1
	}
	index := (page - 1) * size
	return PageImpl{index, size}
}

func (p PageImpl) Page() (int64, int64) {
	return p.index, p.size
}

type CountResult struct {
	Count int64
	Error error
}

type IRowScanner interface {
	Scan(rows *sql.Rows) (interface{}, error)
}
type DefaultScanner struct {
	RawMetaInfo
	columnMap map[string]FieldInfo
}

func getRowScanner(v interface{}, colFieldNameMap map[string]string) (IRowScanner, error) {
	r := GetRawTableInfo(v)
	if r.IsStruct {
		resultMap := make(map[string]FieldInfo)

		for col, fieldName := range colFieldNameMap {
			f, ok := r.Type.FieldByName(fieldName)
			if !ok {
				return nil, errors.New(fmt.Sprintf("field [%s] not exist or can not be access", fieldName))
			}
			resultMap[col] = FieldInfo{
				FieldName: f.Name,
				FieldType: f.Type,
			}
		}
		return DefaultScanner{r, resultMap}, nil
	} else {
		//说明对象是简单类型，直接取类型即可
		return DefaultScanner{
			r,
			nil,
		}, nil
	}
}

func getDefaultScanner(v interface{}, columns ...string) (IRowScanner, error) {
	r := GetRawTableInfo(v)
	if r.IsStruct {
		colMap, cols := getDefaultsColumnFieldMap(r.Type)
		if len(columns) > 0 {
			_, cc, right := ArrayIntersect2(cols, columns)
			if len(right) > 0 {
				return nil, errors.New(fmt.Sprintf("columns [%s] not compatible", fmt.Sprint(right)))
			}
			cols = cc
		}
		for _, col := range cols {
			_, ok := colMap[col]
			if !ok {
				return nil, errors.New(fmt.Sprintf("column %s not compatible", col))
			}
		}
		return DefaultScanner{r, colMap}, nil
	} else {
		//说明对象是简单类型，直接取类型即可
		return DefaultScanner{
			r,
			nil,
		}, nil
	}
}
func (d DefaultScanner) getScanners(columns ...string) ([]any, error) {
	scanners := make([]any, 0)
	if d.IsStruct {
		for _, col := range columns {
			f, ok := d.columnMap[col]
			if !ok {
				return nil, errors.New(fmt.Sprintf("column [%s] is not compatible", col))
			}
			scanners = append(scanners, GetIScannerOfColumn(reflect.New(f.FieldType).Elem().Interface()))
		}
	} else {
		scanners = append(scanners, GetIScannerOfColumn(reflect.New(d.Type).Elem().Interface()))
	}
	return scanners, nil
}

func (d DefaultScanner) Scan(rows *sql.Rows) (interface{}, error) {
	columns, es := rows.Columns()
	if es != nil {
		return nil, es
	}
	if d.columnMap == nil && len(columns) > 1 {
		return nil, errors.New("columns were too many")
	}
	results := d.RawMetaInfo.RawData
	if d.IsSlice {
		for rows.Next() {
			scanners, er := d.getScanners(columns...)
			if er != nil {
				return nil, er
			}
			if len(columns) != len(scanners) {
				return nil, errors.New("columns of query not compatible with input")
			}
			err := rows.Scan(scanners...)
			if err != nil {
				panic(err)
			}
			var val reflect.Value
			if d.IsStruct {
				val = ScannerResultToStruct(d.RawMetaInfo.Type, scanners, columns)
			} else {
				vv, er := (scanners[0].(IScanner)).Value()
				if er != nil {
					panic(er)
				}
				val = reflect.ValueOf(vv)
			}
			results.Set(reflect.Append(results, val))
		}
	} else {
		if rows.Next() {
			scanners, er := d.getScanners(columns...)
			if er != nil {
				return nil, er
			}
			er = rows.Scan(scanners...)
			if er != nil {
				panic(er)
			}
			var val reflect.Value
			if d.IsStruct {
				val = ScannerResultToStruct(d.RawMetaInfo.Type, scanners, columns)
			} else {
				vv, er := (scanners[0].(IScanner)).Value()
				if er != nil {
					panic(er)
				}
				val = reflect.ValueOf(vv)
			}
			results.Set(val)
		}
	}
	return results.Interface(), nil

}

type TableModel interface {
	Table() string
	Columns() []string
	ColumnDataMap() map[string]interface{}
	Condition() Condition
	OrderBys() []OrderBy
	Page() PageInfo
	Clone() TableModel
}

type DefaultModel struct {
	table         string
	columns       []string
	columnDataMap map[string]interface{}
	condition     Condition
	orderBys      []OrderBy
	page          PageInfo
}

func (d DefaultModel) Table() string {
	return d.table
}

func (d DefaultModel) Columns() []string {
	return d.columns
}

func (d DefaultModel) ColumnDataMap() map[string]interface{} {
	if d.columns == nil || len(d.columns) == 0 { //如果列过滤器为空，则直接返回
		return d.columnDataMap
	} else {
		maps := make(map[string]interface{})
		for _, colName := range d.columns {
			maps[colName] = d.columnDataMap[colName]
		}
		return maps
	}
}

func (d DefaultModel) Condition() Condition {
	return d.condition
}

func (d DefaultModel) OrderBys() []OrderBy {
	return d.orderBys
}

func (d DefaultModel) Page() PageInfo {
	return d.page
}

func (d DefaultModel) Clone() TableModel {
	return &DefaultModel{
		table:         d.table,
		columns:       d.columns,
		columnDataMap: d.columnDataMap,
		condition:     d.condition,
		orderBys:      d.orderBys,
		page:          d.page,
	}
}
