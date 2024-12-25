# Gom 示例代码

本目录包含了Gom框架的示例代码，展示了如何使用Gom进行数据库操作。

## 目录结构

- `model.go`: 通用的测试模型定义
- `mysql/`: MySQL数据库示例
- `postgres/`: PostgreSQL数据库示例

## 运行示例

### MySQL示例

1. 确保MySQL服务器已启动
2. 修改 `mysql/main.go` 中的数据库连接信息
3. 运行示例：

```bash
cd mysql
go run main.go
```

### PostgreSQL示例

1. 确保PostgreSQL服务器已启动
2. 修改 `postgres/main.go` 中的数据库连接信息
3. 运行示例：

```bash
cd postgres
go run main.go
```

## 示例功能

这些示例展示了以下功能：

1. 数据库连接
   - 建立连接
   - 开启调试模式
   - 使用预处理语句

2. 表操作
   - 创建表
   - 自动使用预处理语句执行SQL

3. 数据操作
   - 插入单条记录
   - 批量插入
   - 查询单条记录
   - 条件查询
   - 分页查询
   - 更新记录
   - 删除记录

4. 事务处理
   - 开始事务
   - 提交事务
   - 回滚事务
   - 事务中的多个操作

5. 预处理语句
   - 创建预处��语句
   - 执行预处理语句
   - 关闭预处理语句

6. From方法
   - 从结构体自动推导表名
   - 从结构体非空字段生成条件
   - 查询条件自动构建
   - 更新字段自动提取
   - 插入数据自动映射
   - 删除条件自动生成

### From方法使用示例

```go
// 查询示例
searchUser := &User{
    Username: "john_doe",
    Active:   true,
}
var users []User
db.Query("").From(searchUser).Into(&users)

// 更新示例
updateUser := &User{
    Username: "john_doe",
    Age:      27,
}
db.Update("").From(updateUser).Where("username = ?", updateUser.Username).Execute()

// 插入示例
newUser := &User{
    Username: "alice",
    Email:    "alice@example.com",
}
db.Insert("").From(newUser).Execute()

// 删除示例
deleteUser := &User{
    Username: "alice",
    Active:   true,
}
db.Delete("").From(deleteUser).Execute()
```

### From方法特性

1. 表名推导：
   - 自动将结构体名转换为表名
   - UserModel -> user_models
   - User -> users
   - 支持自定义表名映射

2. 字段映射：
   - 使用`gom`标签定义字段映射
   - 支持字段重命名
   - 支持忽略字段（使用`-`）

3. 条件生成：
   - 只使用非零值字段
   - 自动处理各种数据类型
   - 支持指针类型（区分nil和零值）

4. 智能更新：
   - 只更新非零值字段
   - 自动处理时间字段
   - 支持部分更新

5. 批量操作：
   - 支持批量插入
   - 支持批量更新
   - 支持批量删除

## 注意事项

1. 运行示例前请确保：
   - 已安装相应的数据库
   - 数据库服务已启动
   - 连接信息正确
   - 数据库用户有足够的权限

2. 示例代码使用了预处理语句来防止SQL注入：
   - 所有SQL操作都会自动使用预处理语句
   - 可以手动创建和使用预处理语句
   - 原始SQL执行也会使用预处理语句

3. 调试模式：
   - 示例代码默认开启了调试模式
   - 会打印所有SQL语句和参数
   - 生产环境建议关闭调试模式 