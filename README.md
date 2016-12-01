# gom

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

func main() {
	var logs Log
	dsn:=`root:*********@tcp(xxx.xxx.xxx.xxx:3306)/test`
	db,err:=gom.Open("mysql",dsn)
	if err!=nil{
		fmt.Println(err)
	}
	c:=gom.Conditions{States:"where 1=1 and id = ? limit 0,20",Values:[]interface{}{"00225cbc1983410398722c8818345281"}}
	db.Query(c,&logs)
	fmt.Println(logs)
	logs.Level=int(time.Now().Unix())
	logs.Date=time.Now()
	fmt.Println(logs)
	dd ,err:=db.Update(logs)
	fmt.Println(dd,err)
	db.Query(c,&logs)
	fmt.Println(logs)
}

type Log struct {
	Id string `json:"id" gom:"primary,id"`
	Level int `gom:"ignore"`
	Info string
	Date time.Time `gom:"column,date"`
}
func (Log) TableName() string {
	return "system_log"
}
```
