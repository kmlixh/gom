# GOM - Go ORM Framework

GOM 是一个功能强大的 Go 语言 ORM 框架，提供了灵活的数据库操作和高级的类型转换功能。

## 发行注记

### v4.6.3 (2025-03-18)
- 修复 BatchInsert 在上下文超时时的错误处理机制
- 添加 Join(), LeftJoin(), RightJoin(), InnerJoin() 方法支持表连接查询
- 新增 SetContext() 方法直接修改当前 Chain 实例的上下文
- 优化事务处理和并发控制
- 提升批处理操作的性能和稳定性

### v4.6.1 (2025-03-10)
- 完善了批量操作相关的示例代码
- 增加了分组查询和聚合函数的使用示例
- 补充了事务隔离级别和嵌套事务的示例
- 优化了文档结构，提供更多实际应用场景的示例

### v4.3.3 (2025-02-04)
- 修复了主键字段识别问题
- 优化了 Save 方法的自增字段处理
- 改进了事务处理机制
- 增强了类型转换系统的稳定性
- 修复了批量操作中的并发问题

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

1. 基本查询操作：
```go
type User struct {
    ID        int64     `gom:"id,@"`
    Name      string    `gom:"name"`
    Age       int       `gom:"age"`
    CreatedAt time.Time `gom:"created_at"`
    Status    Status    `gom:"status"`
    Balance   float64   `gom:"balance"`
}

// 插入单条数据
db.Chain().Table("users").Values(map[string]interface{}{
    "name":    "John",
    "age":     25,
    "status":  StatusActive,
    "balance": 1000.00,
}).Save()

// 批量插入数据
users := []User{
    {Name: "Alice", Age: 28, Status: StatusActive, Balance: 2000.00},
    {Name: "Bob", Age: 32, Status: StatusActive, Balance: 3000.00},
}
// 转换为 map 切片
userMaps := make([]map[string]interface{}, len(users))
for i, user := range users {
    userMaps[i] = map[string]interface{}{
        "name":    user.Name,
        "age":     user.Age,
        "status":  user.Status,
        "balance": user.Balance,
    }
}
// 执行批量插入，batchSize=100，enableConcurrent=true 表示启用并发插入
affected, err := db.Chain().Table("users").
    BatchValues(userMaps).
    BatchInsert(100, true)
if err != nil {
    log.Printf("批量插入失败: %v", err)
    return
}
log.Printf("成功插入 %d 条记录", affected)

// 基本查询
var user User
db.Chain().Table("users").Where("id = ?", 1).First().Into(&user)

// 条件查询示例
var users []User
db.Chain().Table("users").
    Eq("status", StatusActive).                    // 等于
    Ne("role", "guest").                          // 不等于
    Gt("age", 18).                                // 大于
    Ge("score", 60).                              // 大于等于
    Lt("login_attempts", 5).                      // 小于
    Le("balance", 1000).                          // 小于等于
    Like("name", "%John%").                       // LIKE
    NotLike("email", "%test%").                   // NOT LIKE
    In("department", []string{"IT", "HR"}).       // IN
    NotIn("status", []string{"deleted", "banned"}).// NOT IN
    IsNull("deleted_at").                         // IS NULL
    IsNotNull("updated_at").                      // IS NOT NULL
    Between("created_at", startTime, endTime).    // BETWEEN
    NotBetween("price", 100, 1000).              // NOT BETWEEN
    OrderBy("created_at DESC").
    Limit(10).
    Into(&users)

// 复杂条件组合示例
var complexUsers []User
db.Chain().Table("users").
    Eq("status", StatusActive).
    OrEq("role", "admin").                        // OR status = 'active' OR role = 'admin'
    AndGt("age", 18).                            // AND age > 18
    OrBetween("score", 60, 100).                 // OR score BETWEEN 60 AND 100
    OrIn("department", []string{"IT", "HR"}).    // OR department IN ('IT', 'HR')
    OrderBy("created_at DESC").
    Limit(10).
    Into(&complexUsers)

// 原生条件表达式
db.Chain().Table("users").
    Where("age > ? AND status = ?", 18, StatusActive).
    OrWhereRaw("experience >= 5 AND department IN ('IT', 'HR')").
    OrderBy("created_at DESC").
    Limit(10).
    Into(&users)

// 聚合函数
// 1. 计算总余额
var totalBalance float64
db.Chain().Table("users").
    Where("status = ?", StatusActive).
    Sum("balance").
    Into(&totalBalance)

// 2. 计算年龄段的用户数量
var userCount int64
db.Chain().Table("users").
    Where("age BETWEEN ? AND ?", 20, 30).
    Count().
    Into(&userCount)

// 3. 按状态分组统计平均余额
type StatusBalance struct {
    Status  Status   `gom:"status"`
    Average float64  `gom:"avg_balance"`
}
var statusBalances []StatusBalance
db.Chain().Table("users").
    Select("status, AVG(balance) as avg_balance").
    GroupBy("status").
    Into(&statusBalances)

// 4. 复杂分组和聚合查询示例
type DepartmentStats struct {
    Department string  `gom:"department"`
    AvgAge    float64 `gom:"avg_age"`
    MaxSalary float64 `gom:"max_salary"`
    MinSalary float64 `gom:"min_salary"`
    EmpCount  int64   `gom:"emp_count"`
}

var deptStats []DepartmentStats
db.Chain().Table("employees").
    Select(`
        department,
        AVG(age) as avg_age,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary,
        COUNT(*) as emp_count
    `).
    GroupBy("department").
    Having("COUNT(*) > ?", 5).          // 只统计超过5人的部门
    OrderBy("avg_age DESC").
    Into(&deptStats)

// 5. 多表关联分组查询
type ProjectStats struct {
    ProjectID   int64   `gom:"project_id"`
    ProjectName string  `gom:"project_name"`
    TeamSize    int64   `gom:"team_size"`
    TotalCost   float64 `gom:"total_cost"`
    AvgProgress float64 `gom:"avg_progress"`
}

var projectStats []ProjectStats
db.Chain().Table("projects p").
    Select(`
        p.id as project_id,
        p.name as project_name,
        COUNT(DISTINCT t.user_id) as team_size,
        SUM(t.cost) as total_cost,
        AVG(t.progress) as avg_progress
    `).
    LeftJoin("tasks t ON t.project_id = p.id").
    GroupBy("p.id, p.name").
    Having("COUNT(DISTINCT t.user_id) >= ?", 3).  // 只统计团队成员至少3人的项目
    OrderBy("total_cost DESC").
    Into(&projectStats)

// 6. 时间维度的分组统计
type DailyStats struct {
    Date     time.Time `gom:"date"`
    NewUsers int64     `gom:"new_users"`
    Revenue  float64   `gom:"revenue"`
}

var dailyStats []DailyStats
db.Chain().Table("orders o").
    Select(`
        DATE(created_at) as date,
        COUNT(DISTINCT user_id) as new_users,
        SUM(amount) as revenue
    `).
    Join("users u ON u.id = o.user_id").
    Where("o.created_at >= ?", time.Now().AddDate(0, -1, 0)).  // 最近一个月的数据
    GroupBy("DATE(created_at)").
    Having("revenue > ?", 1000).                               // 只统计营收超过1000的日期
    OrderBy("date DESC").
    Into(&dailyStats)

// 7. 嵌套分组查询
type RegionSummary struct {
    Region       string  `gom:"region"`
    TotalStores  int64   `gom:"total_stores"`
    AvgEmployees float64 `gom:"avg_employees"`
    TotalSales   float64 `gom:"total_sales"`
}

var regionSummary []RegionSummary
db.Chain().Table("stores s").
    Select(`
        region,
        COUNT(*) as total_stores,
        AVG(employee_count) as avg_employees,
        SUM(
            (SELECT SUM(amount) 
             FROM orders o 
             WHERE o.store_id = s.id 
             AND o.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY))
        ) as total_sales
    `).
    GroupBy("region").
    Having("total_sales > ?", 10000).
    OrderBy("total_sales DESC").
    Into(&regionSummary)

// 8. 分页查询示例
type PageInfo struct {
    PageNum     int         `json:"page_num"`
    PageSize    int         `json:"page_size"`
    Total       int64       `json:"total"`
    Pages       int         `json:"pages"`
    HasPrev     bool        `json:"has_prev"`
    HasNext     bool        `json:"has_next"`
    List        interface{} `json:"list"`
    IsFirstPage bool        `json:"is_first_page"`
    IsLastPage  bool        `json:"is_last_page"`
}

var userPage PageInfo
err := db.Chain().Table("users").
    Where("status = ?", StatusActive).
    OrderBy("created_at DESC").
    Page(1, 10).  // 第1页，每页10条
    Into(&userPage)

// 9. 批量更新示例
// 批量更新用户状态
userIDs := []int64{1, 2, 3, 4, 5}
updateData := make([]map[string]interface{}, len(userIDs))
for i, id := range userIDs {
    updateData[i] = map[string]interface{}{
        "id":     id,
        "status": StatusInactive,
        "updated_at": time.Now(),
    }
}
affected, err := db.Chain().Table("users").
    BatchValues(updateData).
    BatchUpdate(100)  // 每批100条
if err != nil {
    log.Printf("批量更新失败: %v", err)
    return
}
log.Printf("成功更新 %d 条记录", affected)

// 10. 软删除示例
type SoftDeleteModel struct {
    ID        int64      `gom:"id,@"`
    DeletedAt *time.Time `gom:"deleted_at"`  // 使用指针类型处理 NULL 值
}

// 软删除记录
err = db.Chain().Table("users").
    Where("id = ?", 1).
    Update(map[string]interface{}{
        "deleted_at": time.Now(),
    })

// 查询时自动排除已删除记录
var activeUsers []User
db.Chain().Table("users").
    Where("deleted_at IS NULL").
    OrderBy("created_at DESC").
    Into(&activeUsers)

// 11. 嵌套事务示例
func complexTransaction(db *gom.DB) error {
    return db.Transaction(func(tx *gom.DB) error {
        // 第一层事务
        if err := createOrder(tx); err != nil {
            return err
        }

        // 嵌套事务
        return tx.Transaction(func(tx2 *gom.DB) error {
            // 第二层事务
            if err := processPayment(tx2); err != nil {
                return err
            }
            return updateInventory(tx2)
        })
    })
}

// 12. 子查询和EXISTS条件示例
var activeProjects []Project
db.Chain().Table("projects p").
    Where("EXISTS (SELECT 1 FROM tasks t WHERE t.project_id = p.id AND t.status = ?)", "in_progress").
    AndWhere("(SELECT COUNT(*) FROM team_members tm WHERE tm.project_id = p.id) > ?", 3).
    OrderBy("p.priority DESC").
    Into(&activeProjects)

// 13. 批量删除示例
// 批量软删除过期用户
expiredUserIDs := []int64{1, 2, 3, 4, 5}
deleteData := make([]map[string]interface{}, len(expiredUserIDs))
for i, id := range expiredUserIDs {
    deleteData[i] = map[string]interface{}{
        "id": id,
    }
}
affected, err := db.Chain().Table("users").
    BatchValues(deleteData).
    BatchDelete(100)  // 每批100条
if err != nil {
    log.Printf("批量删除失败: %v", err)
    return
}
log.Printf("成功删除 %d 条记录", affected)

// 14. 使用表达式更新
err = db.Chain().Table("products").
    Where("id = ?", 1).
    Update(map[string]interface{}{
        "stock":      gom.Expr("stock - ?", 1),
        "sold_count": gom.Expr("sold_count + ?", 1),
        "updated_at": time.Now(),
    })

// 15. 事务隔离级别示例
err = db.Transaction(func(tx *gom.DB) error {
    opts := &gom.TransactionOptions{
        IsolationLevel: sql.LevelSerializable,
        Timeout:       time.Second * 30,
        ReadOnly:      false,
    }
    return tx.TransactionWithOptions(opts, func(tx2 *gom.DB) error {
        // 在可序列化隔离级别下执行操作
        return processHighPriorityTransaction(tx2)
    })
})
```

2. 事务处理：
```go
// 转账示例
func transfer(db *gom.DB, fromID, toID int64, amount float64) error {
    return db.Transaction(func(tx *gom.DB) error {
        // 检查余额
        var fromUser User
        if err := tx.Chain().Table("users").
            Where("id = ? AND balance >= ?", fromID, amount).
            First().
            Into(&fromUser); err != nil {
            return fmt.Errorf("insufficient balance or user not found: %w", err)
        }

        // 更新转出方余额
        if err := tx.Chain().Table("users").
            Where("id = ?", fromID).
            Update(map[string]interface{}{
                "balance": gom.Expr("balance - ?", amount),
            }); err != nil {
            return fmt.Errorf("failed to update sender balance: %w", err)
        }

        // 更新接收方余额
        if err := tx.Chain().Table("users").
            Where("id = ?", toID).
            Update(map[string]interface{}{
                "balance": gom.Expr("balance + ?", amount),
            }); err != nil {
            return fmt.Errorf("failed to update receiver balance: %w", err)
        }

        return nil
    })
}
```

3. 复杂查询：
```go
// 子查询示例
type UserStats struct {
    UserID      int64   `gom:"user_id"`
    TotalOrders int     `gom:"total_orders"`
    TotalSpent  float64 `gom:"total_spent"`
}

var highValueUsers []UserStats
db.Chain().Table("orders o").
    Select(`
        u.id as user_id,
        COUNT(*) as total_orders,
        SUM(o.amount) as total_spent
    `).
    Join("users u ON u.id = o.user_id").
    Where("u.status = ?", StatusActive).
    GroupBy("u.id").
    Having("total_spent > ?", 10000).
    OrderBy("total_spent DESC").
    Into(&highValueUsers)

// 动态条件查询
func buildUserQuery(name string, minAge int, status Status) *gom.Chain {
    chain := db.Chain().Table("users")
    
    if name != "" {
        chain = chain.Where("name LIKE ?", "%"+name+"%")
    }
    if minAge > 0 {
        chain = chain.Where("age >= ?", minAge)
    }
    if status != "" {
        chain = chain.Where("status = ?", status)
    }
    
    return chain
}

// 使用动态查询
var users []User
buildUserQuery("John", 25, StatusActive).
    OrderBy("created_at DESC").
    Into(&users)
```

4. 自定义类型：
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

