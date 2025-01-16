package gom

import (
	"github.com/kmlixh/gom/v4/define"
)

type OrderByImpl struct {
	name      string
	orderType define.OrderType
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
	target         interface{}
}

func (d *DefaultModel) Model() interface{} {
	return d.target
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
