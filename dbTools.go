package gom

import (
	"errors"
	"fmt"
	"github.com/kmlixh/gom/v3/define"
	"os"
	"strings"
)

func GenDefaultStructFromDatabase(db *DB, packageName string, fileName string, tables ...string) error {
	f, er := os.Create(fileName)
	if er != nil {
		panic(er)
	}
	f.WriteString("package " + packageName + "\r\n")
	for _, tt := range tables {
		cols, er := db.factory.GetColumns(tt, db.db)
		if er == nil && len(cols) == 0 {
			er = errors.New("target table has no column")
		}
		if er != nil {
			return er
		}
		f.WriteString(fmt.Sprintf("type %s struct {\r\n", UnderscoreToUpperCamelCase(tt)))
		for _, col := range cols {
			f.WriteString("\t" + UnderscoreToUpperCamelCase(col.ColumnName) + "\t\t" + getTypeOfColumn(col.ColumnType) + "\t\t" + getColumnTag(col) + "\r\n")
		}
		f.WriteString("}\r\n")
		f.WriteString(fmt.Sprintf("func (t %s)TableName() string {\r\n", UnderscoreToUpperCamelCase(tt)))
		f.WriteString(fmt.Sprintf("return \"%s\"", tt))
		f.WriteString("}\r\n")

	}
	return nil
}
func getTypeOfColumn(columnType string) string {
	strs := []string{"CHAR", "VARCHAR", "TINYBLOB", "TINYTEXT", "BLOB", "TEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT"}
	ints := []string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "integer", "bigserial", "bigint"}
	floats := []string{"FLOAT", "DOUBLE", "DECIMAL", "real", "serial", "smallserial", "double precision"}
	datetimes := []string{"DATE", "TIME", "YEAR", "DATETIME", "TIMESTAMP", "timestamp", "time", "date"}
	if inArray(columnType, strs) {
		return "string"
	}
	if inArray(columnType, ints) {
		return "int64"
	}
	if inArray(columnType, floats) {
		return "float64"
	}
	if inArray(columnType, datetimes) {
		return "time.Time"
	}
	return "string"
}
func inArray(s string, a []string) bool {
	for _, i := range a {
		if strings.Contains(s, i) || s == i || strings.Index(s, i) == 0 || strings.Contains(strings.ToUpper(s), i) {
			return true
		}
	}
	return false
}
func getColumnTag(col define.Column) string {
	if col.Primary {
		return fmt.Sprintf("`gom:\"!,%s\"`", col.ColumnName)
	}
	if col.PrimaryAuto {
		return fmt.Sprintf("`gom:\"@,%s\"`", col.ColumnName)
	}
	return fmt.Sprintf("`gom:\"#,%s\"`", col.ColumnName)
}
