package define

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"
)

var Debug bool

type Linker int

const (
	_ Linker = iota
	And
	Or
)

type ITableName interface {
	TableName() string
}
type Operation int

const (
	_ Operation = iota
	Eq
	NotEq
	Ge
	Gt
	Le
	Lt
	Like
	LikeIgnoreStart
	LikeIgnoreEnd
	NotLike
	In
	NotIn
	IsNull
	IsNotNull
	RawOperation
)

type OrderType int

const (
	_ OrderType = iota
	Asc
	Desc
)

type SqlType int

const (
	_ SqlType = iota
	Query
	Insert
	Update
	Delete
)

type Condition interface {
	PayLoads() int64 //有效荷载，说明当前条件及其链式条件共有多少有多少个条件
	Linker() Linker
	Field() string
	Operation() Operation
	Values() []interface{}
	SetValues([]interface{})
	Items() []Condition
	HasSubConditions() bool
	RawExpression() string
	Eq(field string, values interface{}) Condition
	EqBool(b bool, field string, value interface{}) Condition
	OrEq(field string, value interface{}) Condition
	OrEqBool(b bool, field string, value interface{}) Condition
	Ge(field string, value interface{}) Condition
	GeBool(b bool, field string, value interface{}) Condition
	OrGe(field string, value interface{}) Condition
	OrGeBool(b bool, field string, value interface{}) Condition
	Gt(field string, values interface{}) Condition
	GtBool(b bool, field string, values interface{}) Condition
	OrGt(field string, values interface{}) Condition
	OrGtBool(b bool, field string, values interface{}) Condition
	Le(field string, values interface{}) Condition
	LeBool(b bool, field string, values interface{}) Condition
	OrLe(field string, values interface{}) Condition
	OrLeBool(b bool, field string, values interface{}) Condition
	Lt(field string, values interface{}) Condition
	LtBool(b bool, field string, values interface{}) Condition
	OrLt(field string, values interface{}) Condition
	OrLtBool(b bool, field string, values interface{}) Condition
	NotEq(field string, values interface{}) Condition
	NotEqBool(b bool, field string, values interface{}) Condition
	OrNotEq(field string, values interface{}) Condition
	OrNotEqBool(b bool, field string, values interface{}) Condition
	In(field string, values ...interface{}) Condition
	InBool(b bool, field string, values ...interface{}) Condition
	OrIn(field string, values ...interface{}) Condition
	OrInBool(b bool, field string, values ...interface{}) Condition
	NotIn(field string, values ...interface{}) Condition
	NotInBool(b bool, field string, values ...interface{}) Condition
	OrNotIn(field string, values ...interface{}) Condition
	OrNotInBool(b bool, field string, values ...interface{}) Condition
	Like(field string, values interface{}) Condition
	LikeBool(b bool, field string, values interface{}) Condition
	OrLike(field string, values interface{}) Condition
	OrLikeBool(b bool, field string, values interface{}) Condition
	NotLike(field string, values interface{}) Condition
	NotLikeBool(b bool, field string, values interface{}) Condition
	OrNotLike(field string, values interface{}) Condition
	OrNotLikeBool(b bool, field string, values interface{}) Condition
	LikeIgnoreStart(field string, values interface{}) Condition
	LikeIgnoreStartBool(b bool, field string, values interface{}) Condition
	OrLikeIgnoreStart(field string, values interface{}) Condition
	OrLikeIgnoreStartBool(b bool, field string, values interface{}) Condition
	LikeIgnoreEnd(field string, values interface{}) Condition
	LikeIgnoreEndBool(b bool, field string, values interface{}) Condition
	OrLikeIgnoreEnd(field string, values interface{}) Condition
	OrLikeIgnoreEndBool(b bool, field string, values interface{}) Condition
	IsNull(filed string) Condition
	IsNullBool(b bool, field string) Condition
	IsNotNull(field string) Condition
	IsNotNullBool(b bool, field string) Condition
	OrIsNull(filed string) Condition
	OrIsNullBool(b bool, field string) Condition
	OrIsNotNull(field string) Condition
	OrIsNotNullBool(b bool, field string) Condition
	And(field string, operation Operation, value ...interface{}) Condition
	AndBool(b bool, field string, operation Operation, value ...interface{}) Condition
	And2(condition Condition) Condition
	And3(rawExpresssion string, values ...interface{}) Condition
	And3Bool(b bool, rawExpresssion string, values ...interface{}) Condition
	Or(field string, operation Operation, value ...interface{}) Condition
	OrBool(b bool, field string, operation Operation, value ...interface{}) Condition
	Or2(condition Condition) Condition
	Or3(rawExpresssion string, values ...interface{}) Condition
	Or3Bool(b bool, rawExpresssion string, values ...interface{}) Condition
}

type SqlProto struct {
	PreparedSql string
	Data        []interface{}
	Scanner     IRowScanner
}

func NewSqlProto(sql string, data []interface{}, scanner IRowScanner) SqlProto {
	return SqlProto{
		PreparedSql: sql,
		Data:        data,
		Scanner:     scanner,
	}
}

type OrderBy interface {
	Name() string
	Type() OrderType
}
type GroupBy interface {
	Name() string
	Type() OrderType
}
type PageInfo interface {
	Page() (int64, int64)
}
type TableModel interface {
	Table() string
	PrimaryKeys() []string
	PrimaryAutos() []string
	Columns() []string
	ColumnDataMap() map[string]interface{}
	Condition() Condition
	OrderBys() []OrderBy
	Page() PageInfo
	Model() interface{}
}
type SqlFunc func(model ...TableModel) []SqlProto
type SqlFactory interface {
	OpenDb(dsn string) (*sql.DB, error)
	GetTables(db *sql.DB) ([]string, error)
	GetCurrentSchema(db *sql.DB) (string, error)
	GetColumns(tableName string, db *sql.DB) ([]Column, error)
	GetSqlFunc(sqlType SqlType) SqlFunc
	ConditionToSql(preTag bool, condition Condition) (string, []interface{})
	GetTableStruct(tableName string, db *sql.DB) (ITableStruct, error)
}
type ITableStruct interface {
	GetTableName() string
	GetTableComment() string
	GetColumns() ([]Column, error)
}
type IRowScanner interface {
	Scan(rows *sql.Rows) Result
}

type TableStruct struct {
	TableName    string
	TableComment string
	Columns      []Column
}

func (t TableStruct) GetTableName() string {
	return t.TableName
}

func (t TableStruct) GetTableComment() string {
	return t.TableComment
}

func (t TableStruct) GetColumns() ([]Column, error) {
	return t.Columns, nil
}

type Column struct {
	QueryName      string       `json:"queryName"`
	ColumnName     string       `json:"ColumnName"`
	IsPrimary      bool         `json:"isPrimary"`
	IsPrimaryAuto  bool         `json:"isPrimaryAuto"` //If IsPrimary Key Auto Generate Or2 Not
	ColumnTypeName string       `json:"type"`
	Type           reflect.Type `json:"-"`
	ColumnValue    any          `json:"value"`
	Comment        string       `json:"comment"`
}

type Result interface {
	LastInsertId() int64
	RowsAffected() int64
	Data() interface{}
	Error() error
}
type ResultImpl struct {
	lastId     int64
	rowsChange int64
	data       interface{}
	err        error
}

func ErrorResult(err error) Result {
	return &ResultImpl{
		lastId:     -1,
		rowsChange: -1,
		err:        err,
		data:       nil,
	}
}

func NewResult(lastId int64, rowAffect int64, data interface{}, err error) Result {
	return &ResultImpl{
		lastId:     lastId,
		rowsChange: rowAffect,
		data:       data,
		err:        err,
	}
}

func (r ResultImpl) LastInsertId() int64 {
	return r.lastId
}

func (r ResultImpl) RowsAffected() int64 {
	return r.rowsChange
}

func (r ResultImpl) Data() interface{} {
	return r.data
}

func (r ResultImpl) Error() error {
	return r.err
}

type RawMetaInfo struct {
	reflect.Type
	TableName string
	IsSlice   bool
	IsPtr     bool
	IsStruct  bool
	IsMap     bool
	RawData   reflect.Value
}

var columnToFieldNameMapCache = make(map[reflect.Type]map[string]FieldInfo)
var columnsCache = make(map[reflect.Type][]string)

type FieldInfo struct {
	FieldName string
	FieldType reflect.Type
}
type DefaultScanner struct {
	RawMetaInfo
	columnMap map[string]FieldInfo
}

func GetDefaultScanner(v interface{}, columns ...string) (IRowScanner, error) {
	r := GetRawTableInfo(v)
	if r.IsStruct {
		colMap, cols := GetDefaultsColumnFieldMap(r.Type)
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
func (d DefaultScanner) GetScanners(columns ...string) ([]any, error) {
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

func (d DefaultScanner) Scan(rows *sql.Rows) Result {
	count := int64(0)
	columns, es := rows.Columns()
	if es != nil {
		return ErrorResult(es)
	}
	if d.columnMap == nil && len(columns) > 1 {
		return ErrorResult(errors.New("ColumnNames were too many"))
	}
	scanners, er := d.GetScanners(columns...)
	if er != nil {
		return ErrorResult(er)
	}
	results := d.RawMetaInfo.RawData
	if d.IsSlice {
		for rows.Next() {

			if len(columns) != len(scanners) {
				return ErrorResult(errors.New("ColumnNames of excute not compatible with input"))
			}
			err := rows.Scan(scanners...)
			if err != nil {
				return ErrorResult(err)
			}
			var val reflect.Value
			if d.IsStruct {
				val = ScannerResultToStruct(d.RawMetaInfo, count, scanners, columns)
			} else {
				vv, er := (scanners[0].(IScanner)).Value()
				if er != nil {
					return ErrorResult(er)
				}
				val = reflect.ValueOf(vv)
			}
			results.Set(reflect.Append(results, val))
			count++
		}
	} else {
		if rows.Next() {

			er = rows.Scan(scanners...)
			if er != nil {
				panic(er)
			}
			var val reflect.Value
			if d.IsStruct {
				val = ScannerResultToStruct(d.RawMetaInfo, count, scanners, columns)
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
			count++
		}
	}
	return NewResult(0, count, results.Interface(), nil)

}
func GetRawTableInfo(v any) RawMetaInfo {
	var tt reflect.Type
	var rawData any
	if _, ok := v.(reflect.Type); ok {
		tt = v.(reflect.Type)
	} else if _, ok := v.(reflect.Value); ok {
		tt = v.(reflect.Value).Type()
		rawData = v.(reflect.Value)
	} else {
		tt = reflect.TypeOf(v)
	}
	isMap := false
	isStruct := false
	isPtr := false
	isSlice := false
	if tt.Kind() == reflect.Ptr {
		tt = tt.Elem()
		isPtr = true
	}
	if tt.Kind() == reflect.Slice || tt.Kind() == reflect.Array {
		tt = tt.Elem()
		isSlice = true
	}
	isStruct = tt.Kind() == reflect.Struct
	isMap = tt.Kind() == reflect.Map
	if Debug {
		fmt.Println("Test GetRawTableInfo, result:", tt, isPtr, isSlice)
	}
	tableName := ""
	if isStruct {
		tableName = CamelToSnakeString(tt.Name())
	}
	vs := reflect.Indirect(reflect.New(tt))
	iTable, ok := vs.Interface().(ITableName)
	if ok {
		tableName = iTable.TableName()
	}
	if rawData == nil {
		rawData = reflect.Indirect(reflect.ValueOf(v))
	}
	return RawMetaInfo{Type: tt, TableName: tableName, IsSlice: isSlice, IsPtr: isPtr, IsStruct: isStruct, IsMap: isMap, RawData: rawData.(reflect.Value)}
}

func StructToMap(vs interface{}, columns ...string) (map[string]interface{}, error) {
	if vs == nil {
		return nil, errors.New("nil can't be used to create Map")
	}
	rawInfo := GetRawTableInfo(vs)
	if rawInfo.IsSlice {
		return nil, errors.New("can't convert slice or array to map")
	}
	colMap := make(map[string]int)
	if len(columns) > 0 {
		for idx, col := range columns {
			colMap[col] = idx
		}
	}
	if rawInfo.Kind() == reflect.Struct {
		if rawInfo.Type.NumField() == 0 {
			//
			return nil, errors.New(fmt.Sprintf("[%s] was a \"empty struct\",it has no field or All fields has been ignored", rawInfo.Type.Name()))
		}
		newMap := make(map[string]interface{})
		cMap, _ := GetDefaultsColumnFieldMap(rawInfo.Type)
		for key, column := range cMap {
			if len(columns) > 0 {
				_, ok := colMap[key]
				if ok {
					newMap[key] = reflect.Indirect(reflect.ValueOf(vs)).FieldByName(column.FieldName).Interface()
				}
			} else {
				val := reflect.Indirect(reflect.ValueOf(vs)).FieldByName(column.FieldName)
				if !val.IsZero() {
					newMap[key] = val.Interface()
				}
			}
		}
		return newMap, nil
	}
	return nil, errors.New(fmt.Sprintf("can't convert %s to map", rawInfo.Name()))

}
func StructToCondition(vs interface{}, columns ...string) Condition {
	maps, err := StructToMap(vs, columns...)
	if err != nil {
		panic(err)
	}
	return MapToCondition(maps)
}
func MapToCondition(maps map[string]interface{}) Condition {
	if maps == nil {
		return nil
	}
	var cnd Condition
	for k, v := range maps {
		t := reflect.TypeOf(v)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct || t.Kind() == reflect.TypeOf(time.Now()).Kind() || ((t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && t.Elem().Kind() != reflect.Struct) {
			value := v
			if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && t.Elem().Kind() != reflect.Struct {
				if cnd == nil {
					cnd = CndIn(k, UnZipSlice(value)...)
				} else {
					cnd.In(k, UnZipSlice(value)...)
				}
			} else {
				if cnd == nil {
					cnd = Cnd(k, Eq, value)
				} else {
					cnd.And(k, Eq, value)
				}
			}

		}
	}
	return cnd
}
func UnZipSlice(vs interface{}) []any {
	var result = make([]any, 0)
	t := reflect.TypeOf(vs)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		v := reflect.ValueOf(vs)

		if v.Len() > 0 {
			for i := 0; i < v.Len(); i++ { //m为上述切片
				item := v.Index(i)
				if item.Interface() != nil {
					result = append(result, UnZipSlice(item.Interface())...)
				}
			}

		}
	} else {
		result = append(result, vs)
	}
	return result
}
func SliceToGroupSlice(vs interface{}) map[string][]interface{} {
	result := make(map[string][]interface{})
	slice := UnZipSlice(vs)
	for _, v := range slice {
		t := reflect.TypeOf(v).Name()
		lst, ok := result[t]
		if !ok {
			lst = make([]interface{}, 0)
		}
		lst = append(lst, v)
		result[t] = lst
	}
	return result
}
