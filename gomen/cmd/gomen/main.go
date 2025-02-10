package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/kmlixh/gom/v4/gomen"
)

func main() {
	var opts gomen.Options

	// 定义命令行参数
	flag.StringVar(&opts.Driver, "type", "", "数据库类型 (mysql/postgres)")
	flag.StringVar(&opts.URL, "url", "", "数据库连接URL")
	flag.StringVar(&opts.OutputDir, "out", "models", "输出目录")
	flag.StringVar(&opts.PackageName, "package", "models", "包名")
	flag.StringVar(&opts.Pattern, "pattern", "*", "表名匹配模式 (PostgreSQL 可用 schema.table* 格式)")
	flag.StringVar(&opts.TagStyle, "tag", "gom", "标签风格 (gom/db)")
	flag.StringVar(&opts.Prefix, "prefix", "", "表名前缀（生成时会去掉）")
	flag.StringVar(&opts.Suffix, "suffix", "", "表名后缀（生成时会去掉）")
	flag.BoolVar(&opts.GenerateDB, "db", false, "生成db标签")
	flag.BoolVar(&opts.Debug, "debug", false, "是否开启调试模式")

	// 解析命令行参数
	flag.Parse()

	// 显示使用说明
	if len(os.Args) == 1 {
		fmt.Println("GOM 代码生成器")
		fmt.Println("\n用法:")
		fmt.Println("  gomen [选项]")
		fmt.Println("\n选项:")
		flag.PrintDefaults()
		fmt.Println("\n示例:")
		fmt.Println("  MySQL:")
		fmt.Println("    gomen -type mysql -url \"user:password@tcp(localhost:3306)/dbname\" \\")
		fmt.Println("          -prefix \"t_\"")
		fmt.Println("\n  PostgreSQL:")
		fmt.Println("    gomen -type postgres -url \"postgres://user:password@localhost:5432/dbname?sslmode=disable\" \\")
		fmt.Println("          -pattern \"public.user*\"")
		os.Exit(0)
	}

	// 创建生成器实例
	generator, err := gomen.NewGenerator(opts)
	if err != nil {
		log.Fatalf("创建生成器失败: %v", err)
	}
	defer generator.Close()

	// 执行代码生成
	file, er := os.Create(opts.OutputDir + "/" + "gomen.go")
	if er != nil {
		log.Fatalf("创建文件失败: %v", err)
	}
	defer file.Close()
	if err := generator.Generate(file); err != nil {
		log.Fatalf("生成代码失败: %v", err)
	}

	fmt.Println("代码生成完成!")
}
