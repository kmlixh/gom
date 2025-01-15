package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/kmlixh/gom/v4/define"
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
type ResultImpl struct {
}

func (r ResultImpl) LastInsertId() int64 {
	//TODO implement me
	panic("implement me")
}

func (r ResultImpl) RowsAffected() int64 {
	//TODO implement me
	panic("implement me")
}

func (r ResultImpl) Data() interface{} {
	//TODO implement me
	panic("implement me")
}

func (r ResultImpl) Error() error {
	//TODO implement me
	panic("implement me")
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

func GetDefaultScanner(v interface{}, columns ...string) (define.IRowScanner, error) {
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
				return nil, errors.New("ColumnNames of excute not compatible with input")
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
