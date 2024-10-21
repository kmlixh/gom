package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/kmlixh/gom/v3/define"
	"reflect"
)

type FieldInfo struct {
	FieldName string
	FieldType reflect.Type
}
type ITableName interface {
	TableName() string
}

type OrderByImpl struct {
	name      string
	orderType define.OrderType
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

func MakeOrderBy(name string, orderType define.OrderType) define.OrderBy {
	return OrderByImpl{name, orderType}
}
func (o OrderByImpl) Name() string {
	return o.name
}
func (o OrderByImpl) Type() define.OrderType {
	return o.orderType
}

type PageImpl struct {
	index int64
	size  int64
}

func MakePage(page int64, size int64) define.PageInfo {
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

type DefaultScanner struct {
	RawMetaInfo
	columnMap map[string]FieldInfo
}

func getRowScanner(v interface{}, colFieldNameMap map[string]string) (define.IRowScanner, error) {
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

func getDefaultScanner(v interface{}, columns ...string) (define.IRowScanner, error) {
	r := GetRawTableInfo(v)
	if r.IsStruct {
		colMap, cols := getDefaultsColumnFieldMap(r.Type)
		if len(columns) > 0 {
			_, cc, right := ArrayIntersect2(cols, columns)
			if len(right) > 0 {
				return nil, errors.New(fmt.Sprintf("ColumnNames [%s] not compatible", fmt.Sprint(right)))
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
			scanners = append(scanners, GetIScannerOfSimple(reflect.New(f.FieldType).Elem().Interface()))
		}
	} else {
		scanners = append(scanners, GetIScannerOfSimple(reflect.New(d.Type).Elem().Interface()))
	}
	return scanners, nil
}

func (d DefaultScanner) Scan(rows *sql.Rows) (interface{}, error) {
	columns, es := rows.Columns()
	if es != nil {
		return nil, es
	}
	if d.columnMap == nil && len(columns) > 1 {
		return nil, errors.New("ColumnNames were too many")
	}
	results := d.RawMetaInfo.RawData
	if d.IsSlice {
		for rows.Next() {
			scanners, er := d.getScanners(columns...)
			if er != nil {
				return nil, er
			}
			if len(columns) != len(scanners) {
				return nil, errors.New("ColumnNames of query not compatible with input")
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
				if vv != nil {
					val = reflect.ValueOf(vv)
				} else {
					val = reflect.New(d.RawMetaInfo.Type).Elem()
				}

			}
			results.Set(val)
		}
	}
	return results.Interface(), nil

}

type DefaultModel struct {
	table          string
	primaryKeys    []string
	primaryAuto    []string
	columns        []string
	columnFieldMap map[string]string
	columnDataMap  map[string]any
	condition      define.Condition
	orderBys       []define.OrderBy
	page           define.PageInfo
}

func (d *DefaultModel) SetColumns(columns []string) error {
	d.columns = columns
	return nil
}

func (d DefaultModel) PrimaryKeys() []string {
	return d.primaryKeys
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

func (d DefaultModel) Condition() define.Condition {
	return d.condition
}

func (d DefaultModel) OrderBys() []define.OrderBy {
	return d.orderBys
}

func (d DefaultModel) Page() define.PageInfo {
	return d.page
}

func (d DefaultModel) Clone() define.TableModel {
	return &DefaultModel{
		table:         d.table,
		primaryKeys:   d.primaryKeys,
		columns:       d.columns,
		columnDataMap: d.columnDataMap,
		condition:     d.condition,
		orderBys:      d.orderBys,
		page:          d.page,
	}
}

type Record struct {
	Index        int64           `json:"index"`
	Columns      []define.Column `json:"columns"`
	columnIdxMap map[string]int
	ColumnNames  []string       `json:"columnNames"`
	DataMap      map[string]any `json:"dataMap"`
	primaryKeys  []string
}

type TableScanner struct {
	table        string
	condition    define.Condition
	orderBys     []define.OrderBy
	page         define.PageInfo
	ColumnData   []define.Column `json:"columns"`
	columnIdxMap map[string]int
	ColumnNames  []string       `json:"columnNames"`
	DataMap      map[string]any `json:"dataMap"`
	primaryKeys  []string
	Records      []Record
	currentIdx   int64
}

func (t TableScanner) Table() string {
	return t.table
}

func (t TableScanner) PrimaryKeys() []string {
	return t.Records[0].ColumnNames
}

func (t TableScanner) Columns() []string {
	return t.Records[0].ColumnNames
}

func (t TableScanner) ColumnDataMap() map[string]interface{} {
	return t.Records[0].DataMap
}

func (t TableScanner) Condition() define.Condition {
	return t.condition
}
func (t TableScanner) SetCondition(condition define.Condition) TableScanner {
	t.condition = condition
	return t
}

func (t TableScanner) OrderBy(field string, o define.OrderType) TableScanner {
	if t.orderBys == nil {
		t.orderBys = make([]define.OrderBy, 0)
	}
	t.orderBys = append(t.orderBys, MakeOrderBy(field, o))
	return t
}

func (t TableScanner) OrderBys() []define.OrderBy {
	return t.orderBys
}

func (t TableScanner) Page() define.PageInfo {
	return t.page
}
func (t *TableScanner) SetPage(pageNum int64, pageSize int64) *TableScanner {
	t.page = PageImpl{pageNum, pageSize}
	return t
}
func (t *TableScanner) AddRecord(record Record) *TableScanner {
	t.Records = append(t.Records, Record{
		Index:        t.currentIdx + 1,
		Columns:      record.Columns,
		columnIdxMap: record.columnIdxMap,
		ColumnNames:  record.ColumnNames,
		DataMap:      record.DataMap,
	})
	t.currentIdx++
	return t
}
func (t *TableScanner) AddColumn(column define.Column) *TableScanner {
	if t.Records == nil {
		t.Records = make([]Record, 0)
		t.Records = append(t.Records, Record{Index: 0}.AddColumn(column))
	} else {
		t.Records[t.currentIdx].AddColumn(column)
	}

	return t
}
func (r Record) AddColumn(column define.Column) Record {
	if r.Columns == nil {
		r.Columns = make([]define.Column, 0)
	}
	if r.DataMap == nil {
		r.DataMap = make(map[string]any)
	}
	if r.columnIdxMap == nil {
		r.columnIdxMap = make(map[string]int)
	}
	if r.ColumnNames == nil {
		r.ColumnNames = make([]string, 0)
	}
	if idx, ok := r.columnIdxMap[column.ColumnName]; ok {
		r.Columns[idx] = column
	} else {
		r.columnIdxMap[column.ColumnName] = len(r.Columns)
		r.Columns = append(r.Columns, column)
		r.ColumnNames = append(r.ColumnNames, column.ColumnName)
		r.DataMap[column.ColumnName] = column
		if column.IsPrimary {
			r.primaryKeys = append(r.primaryKeys, column.ColumnName)
		}
	}
	return r
}
func (r Record) AddColumn2(queryName string, columnName string) {
	r.AddColumn(define.Column{QueryName: queryName, ColumnName: columnName})
}

func (t TableScanner) Scan(rows *sql.Rows) (interface{}, error) {
	columns := make([]string, 0)
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	scanners := make([]any, 0)
	for _, columnType := range columnTypes {
		scanner := GetIScannerOfSimpleType(columnType.ScanType())
		columnName := columnType.Name()
		columns = append(columns, columnName)
		scanners = append(scanners, scanner)
	}
	rows.Scan(scanners...)
	for _, col := range columns {

	}
}
