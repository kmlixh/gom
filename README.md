# gom

#### 2017年6月18日22:47:53

    1.修复无法使用事务的bug
    2.修改了数据库操作的一些基础逻辑，每次操作前都会进行Prepare操作，以提高一些“性能”
    3.为了修复上面的bug，修改了整体的gom.Db结构

**额外说明的是，目前的测试代码是不充足的。也就是说，测试是不充足的，可能存在很多不易见的bug*

[![GoDoc](https://godoc.org/github.com/jinzhu/gorm?status.svg)](https://godoc.org/github.com/janyees/gom)
[![wercker status](https://app.wercker.com/status/56931116573ad6b913d0c7176e72e759/s/master "wercker status")](https://app.wercker.com/project/byKey/56931116573ad6b913d0c7176e72e759)

gom是一个基于golang语言的ORM框架,目标是实现数据操作的简化,直接针对结构体本身进行数据库操作(增删改查，不包含create和其他会改变表本身结构和数据本身结构的所有方法)

Gom is an ORM framework based on golang language, the target is to realize the data of simplified operation, directly to the structure itself for gom.Db operations

目前支持的数据库类型为*`mysql`*及其衍生品*`mariadb`*

Currently supported gom.Db types is _`mysql`_ and its derivatives _`mariadb`_

典型的使用范例如下:

_The use of a typical example is as follows:_

```go
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


```go
_ "github.com/janyees/gom/factory/mysql"    //这一行也是必须的，目的用于加载相应数据库的驱动和‘方言’
"github.com/janyees/gom"
```
在import节点增加以上两行,第一行是注册相应的mysql工厂.第二行为引用gom

### 第二步,如何定义对象?
##### 请遵循以下原则定义你的数据对象

1.对象应当是一个struct结构,并且此结构拥有自己的"TableName"函数,函数返回表名

```go
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
```go
gom:"primary,id"
gom:"!"
gom:"auto,id"
gom:"@"
gom:"column,info"
gom:"info"
gom:"#"
gom:"-"
```
其中,auto和primary等都是表示主键,但是唯一不同的是,auto表示的主键在insert操作中不会提交到服务器(表示此列由数据库自增长).而primary会被提交

gom:"!"也同样表示为主键,并且主键字段名为当前字段的小写,即 "Id"会转义为"id"
同理,gom:"@"则表示auto属性,字段名为当前字段的小写
column指定为表的列,只写列名(即,gom:"info"这种形式)也是可以的(如第六行表述的情况).

甚至于,只写gom:"#"也是说明此字段为数据库的列,列名称为当前字段名称的小写

-表示此Field被忽略,不会出现查询更新操作的语句中

另外如果不设置gom标签,也同样会被忽略,主要考虑的是,当此字段不作为表字段,但是又被序列化的情况(诸如转化为json字符串的情况)

### 第三步,如何操作数据库?
        首先注意,gom不支持数据库create操作,目前仅考虑支持单表的增删改查操作
        
数据库操作分成以下几个简单步骤

1.连接数据库
```go
dsn:=`root:xxxxx@tcp(120.xx.2xx.189:3306)/xxx`   //定义数据库连接的DSN字符串,不知道DSN怎么定义的,请参考google
	
db,err:=gom.Open("mysql",dsn)    //打开数据库连接池,数据库类型为mysql
if err!=nil{//检查是否有错误?
	fmt.Println(err)
}
```
2.查询数据
```go
var logs []Log
db.Query(&logs,nil)
```
查询结果会存放在logs中,如果传递的不是logs的地址,那么接收查询的返回也是可以的:
```go
var logs []Log
logs=db.Query(logs,nil)
db.QueryByTableModel(TableModel,interface{},gom.Cnd(""))
```
只是这里需要说明的是,如果你传递的是一个struct对象进行查询,则返回的也只会是一个,如果是一个数组、切片,那么返回的就是一个数组、切片.所以,不会提供诸如Fetch之类的查询语句

QueryByTableModel这个函数，目的实现对目标数据库中某列或者某几列的查询，避免每次都查询全部列的*`迷之尴尬`*

针对这个函数，作者提供了另外两个辅助的函数：

    func GetTableModel(v interface{}, names ...string) TableModel
    //过滤针对某个struct生成的TableModel进行精简，去掉names之外列。
    func CreateSingleValueTableModel(v interface{}, table string, field string) TableModel 
    //这个函数的目的是解决查询某一个列，并需要返回大量数据的情形。诸如查询某个表符合某些条件的某列。
    
```go
ids []int
model:=db.CreateSingleValueTableModel(ids,"user_info","id")
db.QueryByTalbeModel(model,&ids,gom.Cnd("create_time < ?",time.Now()))
```
然后在使用queryByTableModel就可以实现查询表“user_info”中id这列符合某个条件的所有值，并存入ids数组。是不是很简单快捷？

具体的原理可以从gom整体的实现逻辑来说明，通过tag标记struct并给struct增加TableName函数，来实现表模型的创建，其中会涉及到

3.增加数据
```go
log:=Log{"dsfa",2,time.Now()}
db.Insert(log)
db.Replace(log)
```
4.修改数据
```go
db.Update(log)
db.UpdateByCondition(log,gom.Cnd(""))
```
5.删除数据
```go
log:=Log{Id:"dsfa"}
db.Delete(log)
db.DeleteByCondition(log,gom.Cnd(""))
```
### 第四步,是否支持事务??
答案是肯定的。*WorkInTransaction*函数就是为事务而准备的,其参数是一个参数为gom.Db的函数，对，函数本身最为参数传入另一个函数，这个函数的原型是：

    TransactionWork func(gom.DbTx *gom.Db) (int, error)

而相应的例子如下：
```go
work=func(db *gom.gom.Db) (int,error){
    ......
    ......
    return something
}
```
这里传入了一个包含事务实例的gom.Db对象，原理是，当前gom.Db使用原始的*`*sql.DB`*对象创建了一个*`*sql.Tx`*事务对象，并使用该事务对象创建一个新的gom.Db对象，事实上，这个gom.Db对象并不知道自己包含了事务实例，换句话说，你可以无限制的在事务内部创建新的事务。
只是需要说明的是,只要操作过程中返回的error不为空,所有的操作都会回滚.
如果你觉得这样做不好，你可以使用RawDb函数引用原始的sql.DB对象。

到这里,框架怎么用,应该已经说的差不多了。

### 题外话,如何扩展支持其他数据库?

_有这方面的准备,但还在考虑中_

