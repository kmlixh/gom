package gom

import (
	"database/sql"
	"errors"
	"reflect"
)

type DefaultStruct struct {
}

type ITableName interface {
	TableName() string
}

type Column struct {
	Data        interface{}
	ColumnName  string
	FieldName   string
	Primary     bool
	PrimaryAuto bool //If Primary Key Auto Generate Or2 Not
	ColumnType  string
	FieldType   reflect.Type
	Scanner     IScanner
}

type SqlProto struct {
	PreparedSql string
	Data        []interface{}
}

type SqlFunc func(model ...TableModel) []SqlProto
type SqlFactory interface {
	GetColumns(tableName string, db *sql.DB) []Column
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
	RawData   interface{}
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
	scanners []interface{}
}

func getDefaultScanner(v interface{}) DefaultScanner {
	r := GetRawTableInfo(v)

	if r.IsStruct {
		return DefaultScanner{r, nil}
	} else {
		//说明对象是简单类型，直接取类型即可
		return DefaultScanner{
			r,
			[]interface{}{GetIScannerOfColumn(reflect.New(r.Type).Elem())},
		}
	}
}

func (d DefaultScanner) Scan(rows *sql.Rows) (interface{}, error) {
	columns, er := rows.Columns()
	if er != nil {
		return nil, er
	}
	results := reflect.ValueOf(d.RawMetaInfo.RawData)
	if d.IsSlice {
		for rows.Next() {
			err := rows.Scan(d.scanners...)
			if err != nil {
				panic(err)
			}
			var val reflect.Value
			if d.IsStruct {
				val = ScannerResultToStruct(d.RawMetaInfo.Type, d.scanners, columns)
			} else {
				vv, er := (d.scanners[0].(IScanner)).Value()
				if er != nil {
					panic(er)
				}
				val = reflect.ValueOf(vv)
			}
			results.Set(reflect.Append(results, val))
		}
	} else {
		if rows.Next() {
			er := rows.Scan(d.scanners...)
			if er != nil {
				panic(er)
			}
			var val reflect.Value
			if d.IsStruct {
				val = ScannerResultToStruct(d.RawMetaInfo.Type, d.scanners, columns)
			} else {
				vv, er := (d.scanners[0].(IScanner)).Value()
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
