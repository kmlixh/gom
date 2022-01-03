package gom

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type SqlProto struct {
	Sql  string
	Data []interface{}
}

type GenerateSQLFunc func(model ...TableModel) []SqlProto
type TransactionWork func(databaseTx *DB) (int64, error)
type SqlFactory interface {
	GetSqlFunc(sqlType SqlType) GenerateSQLFunc
	ConditionToSql(condition Condition) (string, []interface{})
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
type _OrderBy struct {
	name      string
	orderType OrderType
}

func (o _OrderBy) Name() string {
	return o.name
}
func (o _OrderBy) Type() OrderType {
	return o.orderType
}

type Page interface {
	Page() (int, int)
}

type PageImpl struct {
	index int
	size  int
}

func (p PageImpl) Page() (int, int) {
	return p.index, p.size
}

type DB struct {
	factory  SqlFactory
	db       *sql.DB
	cnd      Condition
	table    string
	rawSql   string
	rawData  []interface{}
	tx       *sql.Tx
	orderBys []OrderBy
	groupBys []string
	cols     []string
	page     Page
	model    StructModel
}

func (this DB) RawDb() *sql.DB {
	return this.db
}
func (this DB) Table(table string) DB {
	this.table = table
	return this
}
func (this DB) Raw(sql string, datas []interface{}) DB {
	this.rawSql = sql
	this.rawData = datas
	return this
}

func (this DB) Columns(cols ...string) DB {
	this.cols = cols
	return this
}
func (this DB) OrderBy(field string, t OrderType) DB {
	this.orderBys = append(this.orderBys, _OrderBy{field, t})
	return this
}
func (this DB) CleanOrders() DB {
	this.orderBys = make([]OrderBy, 0)
	return this
}
func (this DB) OrderByAsc(field string) DB {
	this.orderBys = append(this.orderBys, _OrderBy{field, Asc})
	return this
}
func (this DB) OrderByDesc(field string) DB {
	this.orderBys = append(this.orderBys, _OrderBy{field, Desc})
	return this
}
func (this DB) GroupBy(names ...string) DB {
	this.groupBys = append(this.groupBys, names...)
	return this
}
func (this DB) Where2(sql string, patches ...interface{}) DB {
	return this.Where(CndRaw(sql, patches...))
}
func (this DB) Where(cnd Condition) DB {
	this.cnd = cnd
	return this
}
func (this DB) Clone() DB {
	return this.clone()
}
func (this DB) clone() DB {
	return DB{factory: this.factory, db: this.db}
}
func (this DB) Page(index int, pageSize int) DB {
	this.page = PageImpl{index: index, size: pageSize}
	return this
}

func (this DB) Count(columnName string, table string) (int64, error) {
	var counts int64
	columns := make(map[string]Column)
	columns["result"] = Column{ColumnName: "result", Type: reflect.TypeOf(counts), QueryField: "count(" + columnName + ") as result", IsPrimary: false, Auto: false}
	tableModel := StructModel{Columns: columns, ColumnNames: []string{"result"}, Type: reflect.TypeOf(counts), Value: reflect.ValueOf(counts), TableName: table}
	i, er := this.Select(tableModel)
	if er != nil {
		panic(er)
	}
	return i.(int64), nil
}
func (this DB) Select(model StructModel) (interface{}, error) {
	this.model = model
	if len(this.rawSql) > 0 {
		return this.query(this.rawSql, this.rawData, model)
	} else {
		selectFunc := this.factory.GetSqlFunc(Query)
		sqlProtos := selectFunc(TableModel{this.getTableName(), this.getQueryColumns(), nil, this.cnd, this.orderBys, this.groupBys, this.page})
		return this.query(sqlProtos[0].Sql, sqlProtos[0].Data, model)
	}
}
func (this DB) First(vs interface{}) (interface{}, error) {
	model, er := getStructModel(vs)
	if er != nil {
		panic(er)
	}
	return this.Page(0, 1).Select(model)
}
func (thiz DB) Update(vs ...interface{}) (int64, error) {
	return thiz.Execute(Update, vs...)
}
func (thiz DB) Insert(vs ...interface{}) (int64, error) {
	return thiz.Execute(Insert, vs...)
}
func (thiz DB) Delete(vs ...interface{}) (int64, error) {
	return thiz.Execute(Delete, vs...)
}
func (thiz DB) Execute(sqlType SqlType, vs ...interface{}) (int64, error) {
	count, er := thiz.Transaction(func(this *DB) (int64, error) {
		count := int64(0)
		var vmap = SliceToMapSlice(vs)
		for i, v := range vmap {
			if debug {
				fmt.Println("Model Type was:", i, "slice counts:", len(v))
			}
			c, er := this.subExecute(sqlType, vmap[i]...)
			if er != nil {
				return 0, er
			}
			count += c
		}
		return count, nil
	})
	return count, er
}
func (this DB) subExecute(sqlType SqlType, vs ...interface{}) (int64, error) {
	var models []TableModel
	updateFunc := this.factory.GetSqlFunc(sqlType)
	for _, v := range vs {
		structModel, er := getStructModel(v)
		if er != nil {
			return 0, er
		}
		this.model = structModel
		model := TableModel{this.getTableName(), this.getUpdateColumns(), this.model.StructToMap(), this.cnd, this.orderBys, this.groupBys, this.page}
		models = append(models, model)
	}
	sqlProtos := updateFunc(models...)
	cc := int64(0)
	for _, sqlProto := range sqlProtos {
		rs, er := this.execute(sqlProto.Sql, sqlProto.Data)
		if er != nil {
			return 0, er
		}
		cs, err := rs.RowsAffected()
		if err != nil {
			return cs, err
		}
		cc += cs
	}
	return cc, nil
}

func (this DB) getTableName() string {
	if len(this.table) == 0 {
		return this.model.TableName
	}
	return this.table
}
func (this DB) getQueryColumns() []string {
	if len(this.cols) > 0 {
		return this.cols
	}
	return this.model.ColumnNames
}

func (this DB) getUpdateColumns() []string {
	var cols []string
	if len(this.cols) > 0 {
		cols = this.cols
	} else {
		cols = this.model.ColumnNames
	}
	//del primary key
	primary := this.model.Primary.ColumnName
	var dst []string
	for _, k := range cols {
		if !strings.EqualFold(k, primary) {
			dst = append(dst, k)
		}
	}
	return dst
}
func (this DB) getInsertColumns(model StructModel) []string {
	var cols []string
	if len(this.cols) > 0 {
		cols = this.cols
	} else {
		cols = model.ColumnNames
	}
	if model.Primary.Auto {
		return cols
	} else {
		//del primary key
		primary := model.Primary.ColumnName
		var dst []string
		for _, k := range cols {
			if !strings.EqualFold(k, primary) {
				dst = append(dst, k)
			}
		}
		return dst
	}
}

func (this DB) query(sql string, data []interface{}, model StructModel) (interface{}, error) {
	st, err := this.db.Prepare(sql)
	defer st.Close()
	if err != nil {
		return nil, err
	}
	rows, errs := st.Query(data)
	if errs != nil {
		return nil, errs
	}
	defer rows.Close()
	columns, er := rows.Columns()
	transfer := getDataTransfer(md5V(sql), columns, model)
	if er != nil {
		panic(er)
	}
	if transfer.model.Type.Kind() == reflect.Slice {
		results := reflect.Indirect(transfer.model.Value)

		for rows.Next() {
			val := transfer.getValueOfTableRow(rows)
			results.Set(reflect.Append(results, val))
		}
		return results.Interface(), nil
	} else {
		if rows.Next() {
			val := transfer.getValueOfTableRow(rows)
			var vt reflect.Value
			if model.Type.Kind() == reflect.Ptr {
				vt = model.Value.Elem()
			} else {
				vt = reflect.New(model.Type).Elem()

			}
			vt.Set(val)
			return vt.Interface(), nil
		} else {
			return nil, nil
		}

	}
	return nil, nil
}
func (this DB) execute(sql string, data []interface{}) (sql.Result, error) {
	st, err := this.db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	return st.Exec(data)
}

func (this DB) Transaction(work TransactionWork) (int64, error) {
	result := int64(0)
	tx := this.tx
	if tx == nil { //if transaction is nil create it
		var err error
		tx, err = this.db.Begin()
		if err != nil {
			return result, err
		}
	}

	result, err := work(&DB{db: this.db, factory: this.factory})
	if err != nil {
		tx.Rollback()
		return result, err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println("transaction commit error:", err)
	}
	return result, err
}
