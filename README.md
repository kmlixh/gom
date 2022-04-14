# gom


[![GoDoc](https://godoc.org/gitee.com/janyees/gom?status.svg)](https://godoc.org/gitee.com/janyees/gom)
[![wercker status](https://app.wercker.com/status/56931116573ad6b913d0c7176e72e759/s/master "wercker status")](https://app.wercker.com/project/byKey/56931116573ad6b913d0c7176e72e759)

## 基本介绍&特性
gom是一个基于golang语言的关系型数据库ORM框架（CRUD工具库，支持事务）

目前最新版本为v2.0，于2022年4月15 发布

**当前支持的数据库类型仅为*`mysql`*及其衍生品*`mariadb`*

数据库类型支持自定义扩展（参考factory/mysql/mysql.go）

gom是goroutine安全的（自认为的安全）



## 稳定性及性能

和原生查询接近的查询性能（甚至更好），增删改性能略比原生差一些。 

单元测试覆盖率93%，稳定性应该有保证。

但是逻辑覆盖率没法做到百分之百，如使用过程中如出现问题，欢迎邮件我：kmlixh@foxmail.com或者直接给PR

本地测试的结果详见*迭代注记*

## 快速入门

使用go mod的情况下：
```go
require gitee.com/janyees/gom v2.0
```
或者
```shell
go get gitee.com/janyees/gom@v2.0
```
### 一个简单的CRUD示例



## 迭代注记
#### 2022年4月15日 01:56:50
    v2.0
    代码几乎全部重构，你大概可以认为这是一个全新的东西，虽然还叫原来的名字，但是API全变了（不过也没事，之前的版本也就我一个人在用^_^）
    代码测试覆盖率93.0%(相关的测试覆盖率结果可以看test_cover.html以及cover.out)

此处略作测试摘录证明一下我真的做过测试了：
```shell
go test  -cover -coverprofile=cover.out -coverpkg=./...

init DB.............
PASS
coverage: 93.0% of statements in ./...
ok      gitee.com/janyees/gom   9.112s

```
然后Benchmark也顺手写了粗糙的两个：
```shell
go test -bench="." -benchmem -run="TestNothing" 
       
init DB.............
goos: darwin
goarch: amd64
pkg: gitee.com/janyees/gom
cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
BenchmarkBaseSelect-16                       138           8654269 ns/op          662728 B/op      10397 allocs/op
BenchmarkBaseSelectGom-16                    122           8936071 ns/op          679967 B/op      14406 allocs/op
BenchmarkDB_InsertSingle-16                   74          19828957 ns/op            5403 B/op        109 allocs/op
BenchmarkRaw_InsertSingle-16                  66          17606781 ns/op            1175 B/op         22 allocs/op
PASS
ok      gitee.com/janyees/gom   6.176s

```
查询的性能比原始查询是差了一些的，这个需要承认
#### 2019年6月19日 17:44:18
    v1.1.2
    修复CreateSingleTable的一些bug
    

#### 2019年6月15日 08:18:25
    v1.1.1
    修复一些bug；
    增加NotIn模式

#### 2019年5月15日 09:18:06
    v1.0.8
    截止1.0.8又修复了若干bug，详细请看commit
    

#### 2019年4月30日 11:15:38

    1.修复了大量的bug；（具体可以看提交记录）
    2.改造了数据获取的方式，从原来的固定格式转换，变成了接近于数据库底层的Scanner模式的性能
    3.优化了自定义类型的查询和存储

#### 2017年6月22日 12:54:36

    1.修复若干bug(具体修复哪些bug记不清了 ^_^)
    2.修复Update,Insert,Delete方法传入不定参数时的bug（无法解析，或者解析不正确，使用递归解决）
    3.修复Condition为空的情况下会莫名注入一个“where”进入sql语句的bug 
    4.Db对象增加了一个Count函数，故名思议，用来做count的

#### 2017年6月18日22:47:53

    1.修复无法使用事务的bug
    2.修改了数据库操作的一些基础逻辑，每次操作前都会进行Prepare操作，以提高一些“性能”
    3.为了修复上面的bug，修改了整体的gom.Db结构
