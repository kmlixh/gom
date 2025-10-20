package main

import (
	"fmt"
	"log"

	"github.com/kmlixh/gom/v4"
	"github.com/kmlixh/gom/v4/define"
)

func main() {
	// 创建一个模拟的数据库连接和工厂
	db := &gom.DB{}
	factory := &define.MockSQLFactory{}
	chain := gom.NewChain(db, factory)

	// 测试Raw查询 - 应该不会校验tableName
	fmt.Println("测试Raw查询...")
	
	// 使用Raw().Query()方法
	result := chain.Raw("SELECT * FROM users WHERE id = ?", 1).Query()
	if result.Error != nil {
		fmt.Printf("Raw().Query() 错误: %v\n", result.Error)
	} else {
		fmt.Printf("Raw().Query() 成功，数据: %v\n", result.Data)
	}

	// 使用RawQuery方法
	result2 := chain.RawQuery("SELECT * FROM users WHERE id = ?", 1)
	if result2.Error != nil {
		fmt.Printf("RawQuery() 错误: %v\n", result2.Error)
	} else {
		fmt.Printf("RawQuery() 成功，数据: %v\n", result2.Data)
	}

	// 测试Raw查询通过List()方法 - 现在应该不会校验tableName
	fmt.Println("\n测试Raw查询通过List()方法...")
	result3 := chain.Raw("SELECT * FROM users WHERE id = ?", 1).List()
	if result3.Error != nil {
		fmt.Printf("Raw().List() 错误: %v\n", result3.Error)
	} else {
		fmt.Printf("Raw().List() 成功，数据: %v\n", result3.Data)
	}

	// 测试普通查询 - 应该校验tableName
	fmt.Println("\n测试普通查询...")
	result4 := chain.List()
	if result4.Error != nil {
		fmt.Printf("普通List() 错误: %v\n", result4.Error)
	} else {
		fmt.Printf("普通List() 成功，数据: %v\n", result4.Data)
	}

	fmt.Println("\n测试完成！")
}
