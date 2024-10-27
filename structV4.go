package gom

import (
	"database/sql"
	"github.com/kmlixh/gom/v3/define"
)

type ConditionData struct {
	condition define.Condition
}
type TableScanner struct {
	TableName string `json:"table"`
	ConditionData
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

type Selector struct {
	*DBv4
}
type Updator struct {
}

func NewTableScanner(table string) *TableScanner {
	t := TableScanner{TableName: table}
	t.CleanResult()
	return &t
}

func (t *TableScanner) Table() string {
	return t.TableName
}

func (t *TableScanner) SetTable(tableName string) *TableScanner {
	t.TableName = tableName
	return t
}
func (t *TableScanner) PrimaryKeys() []string {
	return t.PrimaryKeyData
}

func (t *TableScanner) Columns() []string {
	return t.ColumnNames
}

func (t *TableScanner) ColumnDataMap() map[string]interface{} {
	return t.DataMap
}

func (t *TableScanner) Condition() define.Condition {
	return t.condition
}
func (t *TableScanner) SetCondition(condition define.Condition) *TableScanner {
	t.condition = condition
	return t
}

func (t *TableScanner) OrderBy(field string, o define.OrderType) *TableScanner {
	if t.orderBys == nil {
		t.orderBys = make([]define.OrderBy, 0)
	}
	t.orderBys = append(t.orderBys, MakeOrderBy(field, o))
	return t
}

func (t *TableScanner) OrderBys() []define.OrderBy {
	return t.orderBys
}

func (t *TableScanner) Page() define.PageInfo {
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
		err := rows.Scan(scanners...)
		if err != nil {
			return nil, err
		}
		t.AddData(columnTypes, scanners)
	}
	return t, nil
}
