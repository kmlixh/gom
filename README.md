# GOM - Go ORM Framework

GOM 是一个功能强大的 Go 语言 ORM 框架，提供了灵活的数据库操作和高级的类型转换功能。

## 特性

- 支持多种数据库（MySQL、PostgreSQL）
- 链式操作 API
- 自动类型转换
- 自定义类型支持
- 完整的事务支持
- 详细的错误处理和日志记录

## 类型转换系统

### 基本类型支持

- 整数类型：`int`, `int8`, `int16`, `int32`, `int64`
- 无符号整数：`uint`, `uint8`, `uint16`, `uint32`, `uint64`
- 浮点数：`float32`, `float64`
- 布尔值：`bool`
- 字符串：`string`
- 字节数组：`[]byte`
- 时间：`time.Time`
- JSON 数组：支持 `[]string`, `[]int` 等

### 自定义类型

#### Status 枚举类型
```go
type Status string

const (
    StatusActive   Status = "active"
    StatusInactive Status = "inactive"
    StatusPending  Status = "pending"
)
```

#### JSONMap 类型
```go
type JSONMap struct {
    Data map[string]interface{}
}
```

#### IPAddress 类型
```go
type IPAddress struct {
    Address string
}
```

### 类型转换接口

实现以下接口以支持自定义类型转换：

```go
type TypeConverter interface {
    FromDB(value interface{}) error
    ToDB() (interface{}, error)
}
```

### 示例

1. 基本使用：
```go
type User struct {
    ID        int64     `gom:"id,@"`
    Name      string    `gom:"name"`
    CreatedAt time.Time `gom:"created_at"`
    Status    Status    `gom:"status"`
}

// 插入数据
db.Chain().Table("users").Values(map[string]interface{}{
    "name":   "John",
    "status": StatusActive,
}).Save()

// 查询数据
var user User
db.Chain().Table("users").Where("id = ?", 1).First().Into(&user)
```

2. 自定义类型：
```go
type CustomInt int

func (c *CustomInt) FromDB(value interface{}) error {
    switch v := value.(type) {
    case int64:
        *c = CustomInt(v)
    case string:
        if v == "zero" {
            *c = 0
        } else if v == "one" {
            *c = 1
        }
    }
    return nil
}

func (c *CustomInt) ToDB() (interface{}, error) {
    return int64(*c), nil
}
```

### 特性

1. NULL 值处理
- 所有基本类型都有合理的零值处理
- 支持指针类型处理 NULL 值
- 自定义类型可以定义自己的 NULL 值行为

2. 特殊字符支持
- 完整的 Unicode 支持
- HTML 和 SQL 特殊字符处理
- 支持换行符和制表符

3. 错误处理
- 详细的错误消息
- 类型转换错误追踪
- 验证错误处理

4. 日志记录
- 支持多个日志级别
- 详细的操作日志
- 可自定义日志处理器

### 最佳实践

1. 类型安全
```go
// 推荐：使用强类型
type UserStatus Status

// 不推荐：直接使用字符串
status string
```

2. 错误处理
```go
// 推荐：详细的错误处理
if err := result.Error; err != nil {
    log.Printf("Failed to save user: %v", err)
    return fmt.Errorf("save user: %w", err)
}

// 不推荐：忽略错误
result.Save()
```

3. 验证
```go
// 推荐：在 ToDB 方法中进行验证
func (ip *IPAddress) ToDB() (interface{}, error) {
    if err := ip.Validate(); err != nil {
        return nil, err
    }
    return ip.Address, nil
}
```

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

