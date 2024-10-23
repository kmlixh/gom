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
	Index   int64          `json:"index"`
	DataMap map[string]any `json:"dataMap"`
	Columns []string       `json:"columns"`
}

type TableScanner struct {
	TableName      string `json:"table"`
	condition      define.Condition
	orderBys       []define.OrderBy
	PageData       define.PageInfo `json:"pageData"`
	ColumnData     []define.Column `json:"columns"`
	QueryNames     []string        `json:"queryNames"`
	ColumnNames    []string        `json:"columnNames"`
	DataMap        map[string]any  `json:"dataMap"`
	PrimaryKeyData []string        `json:"primaryKeys"`
	Records        []Record        `json:"records"`
	RecordSize     int64           `json:"recordSize"`
	currentIdx     int64
}

func (t TableScanner) Table() string {
	return t.TableName
}

func (t TableScanner) PrimaryKeys() []string {
	return t.PrimaryKeyData
}

func (t TableScanner) Columns() []string {
	return t.ColumnNames
}

func (t TableScanner) ColumnDataMap() map[string]interface{} {
	return t.DataMap
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
	return t.PageData
}
func (t *TableScanner) SetPage(pageNum int64, pageSize int64) *TableScanner {
	t.PageData = PageImpl{pageNum, pageSize}
	return t
}

func (t *TableScanner) AddColumn(column define.Column) *TableScanner {
	if t.ColumnData == nil {
		t.ColumnData = make([]define.Column, 0)
	}
	if t.DataMap == nil {
		t.DataMap = make(map[string]any)
	}
	if t.QueryNames == nil {
		t.QueryNames = make([]string, 0)
	}
	if t.ColumnNames == nil {
		t.ColumnNames = make([]string, 0)
	}
	if t.PrimaryKeyData == nil {
		t.PrimaryKeyData = make([]string, 0)
	}
	t.ColumnData = append(t.ColumnData, column)
	t.DataMap[column.ColumnName] = column.ColumnValue
	t.QueryNames = append(t.QueryNames, column.QueryName)
	t.ColumnNames = append(t.ColumnNames, column.ColumnName)
	if column.IsPrimary {
		t.PrimaryKeyData = append(t.PrimaryKeyData, column.ColumnName)
	}
	return t
}
func (t *TableScanner) AddData(columnTypes []*sql.ColumnType, results []any) *TableScanner {
	columnNames := make([]string, 0)
	columns := make([]define.Column, 0)
	dataMap := make(map[string]any)

	for idx, columnType := range columnTypes {
		scanner := results[idx]
		var value any
		if _, ok := scanner.(EmptyScanner); ok {
			value = nil
		}
		value, _ = scanner.(IScanner).Value()
		column := define.Column{
			QueryName:     columnType.Name(),
			ColumnName:    columnType.Name(),
			IsPrimary:     false,
			IsPrimaryAuto: false,
			TypeName:      columnType.DatabaseTypeName(),
			Type:          columnType.ScanType(),
			ColumnValue:   value,
			Comment:       "",
		}
		columns = append(columns, column)
		dataMap[column.ColumnName] = column.ColumnValue
		columnNames = append(columnNames, column.ColumnName)
	}

	if t.currentIdx == 0 {
		t.ColumnData = columns
		t.ColumnNames = columnNames
	}
	record := Record{
		Index:   t.currentIdx,
		DataMap: dataMap,
		Columns: columnNames,
	}
	t.Records = append(t.Records, record)
	t.currentIdx++
	return t
}
func (t *TableScanner) CleanResult() *TableScanner {
	t.DataMap = make(map[string]any)
	t.QueryNames = make([]string, 0)
	t.ColumnNames = make([]string, 0)
	t.PrimaryKeyData = make([]string, 0)
	t.Records = make([]Record, 0)
	t.currentIdx = 0
	return t
}

func (r Record) AddData(name string, data any) Record {
	if r.Columns == nil {
		r.Columns = make([]string, 0)
	}
	if r.DataMap == nil {
		r.DataMap = make(map[string]any)
	}
	r.Columns = append(r.Columns, name)
	r.DataMap[name] = data
	return r
}

func (t *TableScanner) Scan(rows *sql.Rows) (interface{}, error) {
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
	t.CleanResult()
	for rows.Next() {
		rows.Scan(scanners...)
		t.AddData(columnTypes, scanners)
	}
	return t, nil
}
