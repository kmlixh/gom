# gom

gom - An Easy ORM library for Golang
====================================

[![golang](https://img.shields.io/badge/Language-Go-green.svg?style=flat)](https://golang.org)
[![Go Report Card](https://goreportcard.com/badge/github.com/kmlixh/gom/v2)](https://goreportcard.com/report/github.com/kmlixh/gom/v2)
![GitHub](https://img.shields.io/github/license/kmlixh/gom)
[![GoDoc](http://godoc.org/github.com/kmlixh/gom?status.svg)](http://godoc.org/github.com/kmlixh/gom)

## 基本介绍&特性

gom是一个基于golang语言的关系型数据库ORM框架（CRUD工具库，支持事务）

目前最新版本为v2.1.1，于2022年9月3 修复发布。修复问题详见下方的迭代注记

**当前支持的数据库类型仅为* `mysql`*及其衍生品* `mariadb`*

数据库类型支持自定义扩展（参考factory/mysql/mysql.go）

gom是goroutine安全的（自认为的安全）

## 稳定性及性能

和原生查询接近的查询性能（甚至更好），增删改性能略比原生差一些。

单元测试覆盖率90%,测试比较充分，但是仍不排除还有漏网之BUG

但是逻辑覆盖率没法做到百分之百，如使用过程中如出现问题，欢迎邮件我：kmlixh@foxmail.com或者直接给PR

本地测试的结果详见*迭代注记*

## 快速入门

使用go mod的情况下：

```go
require github.com/kmlixh/gom/v2 v2.0.4
require github.com/go-sql-driver/mysql v1.6.0 // indirect,
```

或者

```shell
go get github.com/kmlixh/gom/v2@v2.0.4
```

### 一个简单的CRUD示例

```go
package main

import (
	"github.com/google/uuid"
	"github.com/kmlixh/gom/v2"
	_ "github.com/kmlixh/gom/v2/factory/mysql"
	"time"
)

var dsn = "remote:remote123@tcp(10.0.1.5)/test?charset=utf8&loc=Asia%2FShanghai&parseTime=true"

type User struct {
	Id       int64     `json:"id" gom:"@,id"`
	Pwd      string    `json:"pwd" gom:"pwd"`
	Email    string    `json:"email" gom:"email"`
	Valid    int       `json:"valid" gom:"valid"`
	NickName string    `json:"nicks" gom:"nick_name"`
	RegDate  time.Time `json:"reg_date" gom:"reg_date"`
}

var db *gom.DB

func init() {
	//Create DB ，Global
	var er error
	db, er = gom.Open("mysql", dsn, true)
	if er != nil {
		panic(er)
	}
}

func main() {
	var users []User
	//Query
	db.Where(gom.Cnd("name", gom.Eq, "kmlixh")).Page(0, 100).Select(&users)
	//Update
	temp := users[0]
	temp.NickName = uuid.New().String()
	temp.RegDate = time.Now()
	db.Update(temp)
	//Delete
	db.Delete(users[1])
	tt := User{
		Pwd:      "123213",
		Email:    "1@test.com",
		Valid:    1,
		NickName: uuid.New().String(),
		RegDate:  time.Now(),
	}
	db.Insert(tt)

}


```

### DB结构体具有的方法（函数）如下：

```go
RawDb() 获取原生的sql.Db对象
Table(tableName string) 设置表名
Raw() *sql.Db 获取go底层的db对象
OrderBy()排序
CleanOrders清除排序
OrderByAsc
OrderByDesc
Where2
Where
Clone
Page
Count
Sum
Select
SelectByModel
First
Insert
Delete
Update
ExecuteRaw
ExecuteStatement
Begin
IsInTransaction
Commit
Rollback
DoTransaction
CleanDb
```

## 迭代注记

#### 2022年9月3日 修复In只有一个参数是sql异常的mysql报错；版本更新为v2.1.1

#### 2022年9月2日 修复MapToCondition 没有处理简单类型数组的bug；版本更新为2.1.0

#### 2022年9月1日 修复某些情况下，In条件解析数组参数异常的bug；版本更新为2.10

#### 2022年7月21日 修复复杂条件解析逻辑混乱的bug；版本更新为2.0.9(你猜的没错，2.0.8也是修复这个bug，没修好)

#### 2022年7月20日 修复Count和Sum时条件无效的bug，版本更新为v2.0.7(中间两个版本改了什么忘记了，懒得去🍵git)

#### 2022年4月17日 修复bug，更新版本为v2.0.4

    修复查询条件关系错误的bug；
    修复查询条件初始化为空时附加属性不合理的bug；
    新增CndEmpty()方法，用于创建空的Condition对象，此方法与CndRaw("")等价

#### 2022年4月15日 01:56:50 v2.0.0发布

```
v2.0
代码几乎全部重构，你大概可以认为这是一个全新的东西，API全变了（不过也没事，之前的版本也就我一个人在用^_^自嗨锅）
代码测试覆盖率93.0%(相关的测试覆盖率结果可以看test_cover.html以及cover.out)
```

此处略作测试摘录证明一下我真的做过测试了：

```shell
go test  -cover -coverprofile=cover.out -coverpkg=./...

init DB.............
PASS
coverage: 93.0% of statements in ./...
ok      github.com/kmlixh/gom   9.112s
```

然后Benchmark也顺手写了粗糙的两个：

```shell
go test -bench="." -benchmem -run="TestNothing" 
   
init DB.............
goos: darwin
goarch: amd64
pkg: github.com/kmlixh/gom
cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
BenchmarkBaseSelect-16                       138           8654269 ns/op          662728 B/op      10397 allocs/op
BenchmarkBaseSelectGom-16                    122           8936071 ns/op          679967 B/op      14406 allocs/op
BenchmarkDB_InsertSingle-16                   74          19828957 ns/op            5403 B/op        109 allocs/op
BenchmarkRaw_InsertSingle-16                  66          17606781 ns/op            1175 B/op         22 allocs/op
PASS
ok      github.com/kmlixh/gom   6.176s
```

查询的性能比原始查询是差了一些的，这个需要承认

#### 2019年6月19日 17:44:18

```
v1.1.2
修复CreateSingleTable的一些bug
```

#### 2019年6月15日 08:18:25

```
v1.1.1
修复一些bug；
增加NotIn模式
```

#### 2019年5月15日 09:18:06

```
v1.0.8
截止1.0.8又修复了若干bug，详细请看commit
```

#### 2019年4月30日 11:15:38

```
1.修复了大量的bug；（具体可以看提交记录）
2.改造了数据获取的方式，从原来的固定格式转换，变成了接近于数据库底层的Scanner模式的性能
3.优化了自定义类型的查询和存储
```

#### 2017年6月22日 12:54:36

```
1.修复若干bug(具体修复哪些bug记不清了 ^_^)
2.修复Update,Insert,Delete方法传入不定参数时的bug（无法解析，或者解析不正确，使用递归解决）
3.修复Condition为空的情况下会莫名注入一个“where”进入sql语句的bug 
4.Db对象增加了一个Count函数，故名思议，用来做count的
```

#### 2017年6月18日22:47:53

```
1.修复无法使用事务的bug
2.修改了数据库操作的一些基础逻辑，每次操作前都会进行Prepare操作，以提高一些“性能”
3.为了修复上面的bug，修改了整体的gom.Db结构
```

