# gom

[![GoDoc](https://godoc.org/github.com/jinzhu/gorm?status.svg)](https://godoc.org/github.com/janyees/gom)

gom是一个基于golang语言的ORM框架,目标是实现数据操作的简化,直接针对结构体本身进行数据库操作

Gom is an ORM framework based on golang language, the target is to realize the data of simplified operation, directly to the structure itself for database operations

目前支持的数据库类型为***`mysql`***及其衍生品***`mariadb`***

Currently supported database types is _`mysql`_ and its derivatives _`mariadb`_

典型的使用范例如下:

_The use of a typical example is as follows:_

```golang
package main

import (
	"fmt"
	_ "github.com/janyees/gom/factory/mysql"
	"github.com/janyees/gom"
	"time"
)
type Log struct {
	Id string `json:"id" gom:"primary,id"`
	Level int `gom:"ignore"`
	Info string
	Date time.Time `gom:"column,date"`
}
func (Log) TableName() string {
	return "system_log"
}

func main() {
	var logs Log
	dsn:=`root:Nuatar171Yzy@tcp(120.25.254.189:3306)/moren`
	db,err:=gom.Open("mysql",dsn)
	if err!=nil{
		fmt.Println(err)
	}
	db.Query(&logs,gom.Cnds("id=?","0d9c1726873f4bc3b6fb955877e5a082"))
	idelte,ed:=db.Delete(logs)
	fmt.Println(idelte,ed)
	logs.Date=time.Now()
	ii,ie:=db.Insert(logs)
	fmt.Println(ii,ie)

}

```
聪明的你很可能
