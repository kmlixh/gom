package structs

import (
	"database/sql"
	"gitee.com/janyees/gom/v2/arrays"
	"gitee.com/janyees/gom/v2/cnds"
	"reflect"
)

var Debug bool

type DefaultStruct struct {
}

type RawTableInfo struct {
	reflect.Type
	RawTableName string
	IsSlice      bool
	IsPtr        bool
	IsStruct     bool
}

type SqlProto struct {
	PreparedSql string
	Data        []interface{}
}
type Column struct {
	Data        interface{}
	ColumnName  string
	FieldName   string
	Primary     bool
	PrimaryAuto bool //If Primary Key Auto Generate Or2 Not
}
type GenerateSQLFunc func(model ...TableModel) []SqlProto
type SqlFactory interface {
	GetSqlFunc(sqlType SqlType) GenerateSQLFunc
	ConditionToSql(condition cnds.Condition) (string, []interface{})
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

func MakeOrderBy(name string, orderType OrderType) OrderBy {
	return OrderByImpl{name, orderType}
}
func (o OrderByImpl) Name() string {
	return o.name
}
func (o OrderByImpl) Type() OrderType {
	return o.orderType
}

type Page interface {
	Page() (int, int)
}

type PageImpl struct {
	index int
	size  int
}

func MakePage(index int, size int) Page {
	return PageImpl{index, size}
}

func (p PageImpl) Page() (int, int) {
	return p.index, p.size
}

type CountResult struct {
	Count int64
	Error error
}

type TableModel interface {
	Table() string
	SetTable(tableName string)
	Columns() []string
	SetColumns([]string) error
	SetData(data interface{}, valueOfData reflect.Value, isStruct bool, isPtr bool, isSlice bool)
	GetScanners(columns []string) ([]interface{}, int, error)
	PrimaryAuto() bool
	ColumnDataMap() map[string]interface{}
	Condition() cnds.Condition
	SetCondition(c cnds.Condition) error
	OrderBys() []OrderBy
	SetOrderBys(orders []OrderBy) error
	Page() Page
	SetPage(p Page) error
	Scan(rows *sql.Rows) (interface{}, error)
	Clone() TableModel
}

type DefaultTableModel struct {
	rawType         reflect.Type
	rawTable        string
	rawColumnNames  []string
	rawColumns      []Column
	rawScanners     []IScanner
	rawColumnIdxMap map[string]int
	primaryAuto     bool
	isStruct        bool

	//以下内容动态添加
	data          reflect.Value
	isSlice       bool
	isPtr         bool
	table         string
	columns       []string
	columnsIdx    []int8
	columnDataMap map[string]interface{}
	condition     cnds.Condition
	orderBys      []OrderBy
	page          Page
}

func (d DefaultTableModel) GetScanners(columns []string) ([]interface{}, int, error) {
	var scanners []interface{}
	simpleIdx := 0
	if d.isStruct {
		for _, column := range columns {
			idx, ok := d.rawColumnIdxMap[column]
			if ok {
				scanners = append(scanners, d.rawScanners[idx])
			} else {
				scanners = append(scanners, EMPTY_SCANNER)
			}
		}
	} else if d.columns == nil || len(d.Columns()) <= 1 {
		colName := ""
		if d.columns == nil {
			colName = columns[0]
		} else {
			colName = d.columns[0]
		}
		for i, column := range columns {
			if column == colName {
				simpleIdx = i
				scanners = append(scanners, d.rawScanners[0])
			} else {
				scanners = append(scanners, EMPTY_SCANNER)
			}
		}
	}
	return scanners, simpleIdx, nil
}

func (d DefaultTableModel) Scan(rows *sql.Rows) (interface{}, error) {
	columns, er := rows.Columns()
	if er != nil {
		return nil, er
	}
	//解析查询结果列与原始column的对应关系
	var scanners, simpleIdx, err = d.GetScanners(columns)
	if err != nil {
		return nil, er
	}
	results := d.data
	if d.isSlice {
		for rows.Next() {
			err := rows.Scan(scanners...)
			if err != nil {
				panic(err)
			}
			var val reflect.Value
			if d.isStruct {
				val = ScannerResultToStruct(d.rawType, scanners, columns, d.rawColumnIdxMap)
			} else {
				vv, er := (scanners[simpleIdx].(IScanner)).Value()
				if er != nil {
					panic(er)
				}
				val = reflect.ValueOf(vv)
			}
			results.Set(reflect.Append(results, val))
		}
	} else {
		if rows.Next() {
			er := rows.Scan(scanners...)
			if er != nil {
				panic(er)
			}
			var val reflect.Value
			if d.isStruct {
				val = ScannerResultToStruct(d.rawType, scanners, columns, d.rawColumnIdxMap)
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
func (d DefaultTableModel) Table() string {
	if d.table != "" && len(d.table) > 0 {
		return d.table
	}
	return d.rawTable
}

func (d *DefaultTableModel) SetTable(tableName string) {
	d.table = tableName
}

func (d DefaultTableModel) Columns() []string {
	if d.columns != nil && len(d.columns) > 0 {
		return d.columns
	}
	return d.rawColumnNames
}

func (d *DefaultTableModel) SetColumns(columns []string) error {
	if columns != nil && len(columns) > 0 {
		if d.isStruct {
			d.columns = arrays.Intersect(d.rawColumnNames, append([]string{d.rawColumnNames[0]}, columns...))
		} else {
			d.columns = columns
		}
	}
	return nil
}

func (d *DefaultTableModel) SetData(_ interface{}, valueOfData reflect.Value, isStruct bool, isPtr bool, isSlice bool) {
	d.data = valueOfData
	d.isStruct = isStruct
	d.isPtr = isPtr
	d.isSlice = isSlice
	if isStruct && !isSlice { //为结构体并且非数组或切片的情况
		dataMap := make(map[string]interface{})
		_, columns, _ := getColumns(valueOfData)
		for _, column := range columns {
			dataMap[column.ColumnName] = column.Data
		}
		d.columnDataMap = dataMap
	}
}

func (d DefaultTableModel) PrimaryAuto() bool {
	return d.primaryAuto
}

func (d DefaultTableModel) ColumnDataMap() map[string]interface{} {
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

func (d DefaultTableModel) Condition() cnds.Condition {
	if d.condition != nil {
		return d.condition
	}
	if d.columnDataMap != nil {
		col, ok := d.columnDataMap[d.rawColumnNames[0]] //默认第一个为主键
		v := reflect.ValueOf(col)
		//TODO 此处逻辑不够完备，需要判断列本身是否为空
		if ok && !v.IsZero() {
			d.condition = cnds.New(d.rawColumnNames[0], cnds.Eq, col)
		}
	}
	return d.condition
}

func (d *DefaultTableModel) SetCondition(c cnds.Condition) error {
	d.condition = c
	return nil
}

func (d DefaultTableModel) OrderBys() []OrderBy {
	return d.orderBys
}

func (d *DefaultTableModel) SetOrderBys(orders []OrderBy) error {
	d.orderBys = orders
	return nil
}

func (d DefaultTableModel) Page() Page {
	return d.page
}

func (d *DefaultTableModel) SetPage(p Page) error {
	d.page = p
	return nil
}
func (d DefaultTableModel) Clone() TableModel {
	return &DefaultTableModel{
		rawScanners:     d.rawScanners,
		rawType:         d.rawType,
		rawTable:        d.rawTable,
		rawColumnNames:  d.rawColumnNames,
		rawColumns:      d.rawColumns,
		rawColumnIdxMap: d.rawColumnIdxMap,
		primaryAuto:     d.primaryAuto,
	}
}
