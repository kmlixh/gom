# gom

[![GoDoc](https://godoc.org/github.com/jinzhu/gorm?status.svg)](https://godoc.org/github.com/janyees/gom)
[![wercker status](https://app.wercker.com/status/56931116573ad6b913d0c7176e72e759/s/master "wercker status")](https://app.wercker.com/project/byKey/56931116573ad6b913d0c7176e72e759)

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
	dsn:=`root:xxxx@tcp(1x.xx.2xx.xx:3306)/xxxx`
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

###### 聪明的你很可能已经知道怎么使用了.

### 第一步,如何引用?

**_目前仅支持mysql数据库,允许扩展,详情请看文末_**


```
_ "github.com/janyees/gom/factory/mysql"
"github.com/janyees/gom"
```
在import节点增加以上两行,第一行是注册相应的mysql工厂.第二行为引用gom

### 第二步,如何定义对象?
##### 请遵循以下原则定义你的数据对象

1.对象应当是一个struct结构,并且此结构拥有自己的"TableName"函数,函数返回表名

```
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

2.每一个字段应当设定一个gom的标签(Tag)

合法的标签写法有以下几种:
```
gom:"primary,id"
gom:"!"
gom:"auto,id"
gom:"@"
gom:"column,info"
gom:"info"
gom:"#"
gom:"ignore"
gom:"-"
```
其中,auto和primary等都是表示主键,但是唯一不同的是,auto表示的主键在insert操作中不会提交到服务器(表示此列由数据库自增长).而primary会被提交

gom:"!"也同样表示为主键,并且主键字段名为当前字段的小写,即 "Id"会转义为"id"
同理,gom:"@"则表示auto属性,字段名为当前字段的小写
column指定为表的列,只写列名(即,gom:"info"这种形式)也是可以的(如第四行表述的情况).

但是请注意是否会和ignore冲突?甚至于,只写gom:"#"也是说明此字段为数据库的列,列名称为当前字段名称的小写

ignore和-都表示此Field被忽略,不会出现查询更新操作的语句中

另外如果不设置gom标签,也同样会被忽略,主要考虑的是,当此字段不作为表字段,但是又被序列化的情况(诸如转化为json字符串的情况)

### 第三步,如何操作数据库?
        首先注意,gom不支持数据库create操作,目前仅考虑支持单表的增删改查操作
        
数据库操作分成以下几个简单步骤

1.连接数据库
```
dsn:=`root:xxxxx@tcp(120.xx.2xx.189:3306)/xxx`   //定义数据库连接的DSN字符串,不知道DSN怎么定义的,请参考google
	
db,err:=gom.Open("mysql",dsn)    //打开数据库连接池,数据库类型为mysql
if err!=nil{//检查是否有错误?
	fmt.Println(err)
}
```
2.查询数据
```
var logs []Log
db.Query(&logs,nil)
```
查询结果会存放在logs中,如果传递的不是logs的地址,那么接收查询的返回也是可以的:
```
var logs []Log
logs=db.Query(logs,nil)
```
只是这里需要说明的是,如果你传递的是一个struct对象进行查询,则返回的也只会是一个,如果是一个数组,那么返回的就是一个数组.所以,不会提供诸如Fetch之类的查询语句

3.增加数据
```
log:=Log{"dsfa",2,time.Now()}
db.Insert(log)
```
4.修改数据
```
db.Update(log)
```
5.删除数据
```
log:=Log{Id:"dsfa"}
db.Delete(log)
```
### 第四步,是否支持事务??
答案是肯定的,只要是包含InTransaction的函数都是事务类操作,例如:

```
UpdateInTransaction
DeleteInTransaction
InsertInTransaction
```
需要额外说明的是,只要操作过程中出现错误,所有的操作都会回滚.

到这里,框架怎么用,应该已经很清楚了

### 题外话,如何扩展支持其他数据库?

_有这方面的准备,但还在考虑中_

