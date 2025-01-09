package gom

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
	"github.com/stretchr/testify/assert"
)

type Service struct {
	ID          uint      `json:"id" gom:"id,@,auto"`
	Name        string    `json:"name" gom:"name,notnull"`
	Description string    `json:"description" gom:"description"`
	CreatedAt   time.Time `json:"createdAt" gom:"created_at,notnull,default"`
	UpdatedAt   time.Time `json:"updatedAt" gom:"updated_at,notnull,default"`
}

func (s *Service) TableName() string {
	return "services"
}

type Domain struct {
	ID           uint      `json:"id" gom:"id,@,auto"`
	Name         string    `json:"name" gom:"name,notnull,unique"`
	DomainName   string    `json:"domainName" gom:"identifier,notnull,unique"`
	Description  string    `json:"description" gom:"description"`
	ServiceCount int       `json:"serviceCount" gom:"service_count,default"`
	Status       int       `json:"status" gom:"status,notnull,default"`
	Services     []Service `json:"services" gom:"services,m2m:domain_services"`
	CreatedAt    time.Time `json:"createdAt" gom:"created_at,notnull,default"`
	UpdatedAt    time.Time `json:"updatedAt" gom:"updated_at,notnull,default"`
}

func (d *Domain) TableName() string {
	return "domains"
}

func TestDomainMapping(t *testing.T) {
	// 创建数据库连接
	db, err := Open("mysql", "remote:123456@tcp(192.168.110.249:3306)/test?charset=utf8mb4&parseTime=True", nil)
	assert.NoError(t, err)
	defer db.Close()

	// 清理旧表
	db.DB.Exec("DROP TABLE IF EXISTS domain_services")
	db.DB.Exec("DROP TABLE IF EXISTS services")
	db.DB.Exec("DROP TABLE IF EXISTS domains")

	// 创建测试表
	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS domains (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			identifier VARCHAR(255) NOT NULL,
			description TEXT,
			service_count INT DEFAULT 0,
			status INT NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_name (name),
			UNIQUE KEY uk_identifier (identifier)
		)
	`)
	assert.NoError(t, err)

	// 创建关联表
	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS services (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			description TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)
	`)
	assert.NoError(t, err)

	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS domain_services (
			domain_id BIGINT UNSIGNED NOT NULL,
			service_id BIGINT UNSIGNED NOT NULL,
			PRIMARY KEY (domain_id, service_id),
			CONSTRAINT fk_domain_services_domain FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE CASCADE,
			CONSTRAINT fk_domain_services_service FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
		)
	`)
	assert.NoError(t, err)

	// 清理测试数据
	defer func() {
		db.DB.Exec("DROP TABLE IF EXISTS domain_services")
		db.DB.Exec("DROP TABLE IF EXISTS services")
		db.DB.Exec("DROP TABLE IF EXISTS domains")
	}()

	// 测试创建
	domain := &Domain{
		Name:         "Test Domain",
		DomainName:   "test-domain",
		Description:  "Test Description",
		ServiceCount: 0,
		Status:       1,
	}

	result := db.Chain().Table("domains").Values(map[string]interface{}{
		"name":          domain.Name,
		"identifier":    domain.DomainName,
		"description":   domain.Description,
		"service_count": domain.ServiceCount,
		"status":        domain.Status,
	}).Save()
	assert.NoError(t, result.Error)
	assert.NotZero(t, result.ID)
	domain.ID = uint(result.ID)

	// 测试查询
	var domains []Domain
	listResult := db.Chain().Table("domains").Where("id", define.OpEq, domain.ID).List(&domains)
	assert.NoError(t, listResult.Error)
	assert.Len(t, domains, 1)
	fetchedDomain := domains[0]
	assert.Equal(t, domain.Name, fetchedDomain.Name)
	assert.Equal(t, domain.DomainName, fetchedDomain.DomainName)
	assert.Equal(t, domain.Description, fetchedDomain.Description)
	assert.Equal(t, domain.Status, fetchedDomain.Status)

	// 测试更新
	updateResult := db.Chain().Table("domains").Where("id", define.OpEq, domain.ID).Values(map[string]interface{}{
		"name":       "Updated Domain",
		"identifier": "updated-domain",
		"status":     2,
	}).Save()
	assert.NoError(t, updateResult.Error)

	// 验证更新
	domains = nil
	listResult = db.Chain().Table("domains").Where("id", define.OpEq, domain.ID).List(&domains)
	assert.NoError(t, listResult.Error)
	assert.Len(t, domains, 1)
	updatedDomain := domains[0]
	assert.Equal(t, "Updated Domain", updatedDomain.Name)
	assert.Equal(t, "updated-domain", updatedDomain.DomainName)
	assert.Equal(t, 2, updatedDomain.Status)

	// 测试删除
	deleteResult := db.Chain().Table("domains").Where("id", define.OpEq, domain.ID).Delete()
	assert.NoError(t, deleteResult.Error)

	// 验证删除
	count, err := db.Chain().Table("domains").Where("id", define.OpEq, domain.ID).Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestDomainServiceRelation(t *testing.T) {
	// 创建数据库连接
	db, err := Open("mysql", "remote:123456@tcp(192.168.110.249:3306)/test?charset=utf8mb4&parseTime=True", nil)
	assert.NoError(t, err)
	defer db.Close()

	// 清理旧表
	db.DB.Exec("DROP TABLE IF EXISTS domain_services")
	db.DB.Exec("DROP TABLE IF EXISTS services")
	db.DB.Exec("DROP TABLE IF EXISTS domains")

	// 创建测试表
	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS domains (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			identifier VARCHAR(255) NOT NULL,
			description TEXT,
			service_count INT DEFAULT 0,
			status INT NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_name (name),
			UNIQUE KEY uk_identifier (identifier)
		)
	`)
	assert.NoError(t, err)

	// 创建关联表
	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS services (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			description TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)
	`)
	assert.NoError(t, err)

	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS domain_services (
			domain_id BIGINT UNSIGNED NOT NULL,
			service_id BIGINT UNSIGNED NOT NULL,
			PRIMARY KEY (domain_id, service_id),
			CONSTRAINT fk_domain_services_domain FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE CASCADE,
			CONSTRAINT fk_domain_services_service FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
		)
	`)
	assert.NoError(t, err)

	// 清理测试数据
	defer func() {
		db.DB.Exec("DROP TABLE IF EXISTS domain_services")
		db.DB.Exec("DROP TABLE IF EXISTS services")
		db.DB.Exec("DROP TABLE IF EXISTS domains")
	}()

	// 创建测试数据
	domain := &Domain{
		Name:         "Test Domain",
		DomainName:   "test-domain",
		Description:  "Test Description",
		ServiceCount: 0,
		Status:       1,
	}

	result := db.Chain().Table("domains").Values(map[string]interface{}{
		"name":          domain.Name,
		"identifier":    domain.DomainName,
		"description":   domain.Description,
		"service_count": domain.ServiceCount,
		"status":        domain.Status,
	}).Save()
	assert.NoError(t, result.Error)
	domain.ID = uint(result.ID)

	// 创建服务
	services := []Service{
		{Name: "Service 1", Description: "Description 1"},
		{Name: "Service 2", Description: "Description 2"},
	}

	for i := range services {
		serviceResult := db.Chain().Table("services").Values(map[string]interface{}{
			"name":        services[i].Name,
			"description": services[i].Description,
		}).Save()
		assert.NoError(t, serviceResult.Error)
		services[i].ID = uint(serviceResult.ID)
	}

	// 添加关联关系
	for _, service := range services {
		result := db.Chain().Table("domain_services").Values(map[string]interface{}{
			"domain_id":  domain.ID,
			"service_id": service.ID,
		}).Save()
		assert.NoError(t, result.Error)
	}

	// 验证关联关系
	var count int64
	count, err = db.Chain().Table("domain_services").Where("domain_id", define.OpEq, domain.ID).Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// 清理测试数据
	db.DB.Exec("DELETE FROM domain_services WHERE domain_id = ?", domain.ID)
	for _, service := range services {
		db.DB.Exec("DELETE FROM services WHERE id = ?", service.ID)
	}
	db.DB.Exec("DELETE FROM domains WHERE id = ?", domain.ID)
}

func TestDomainComplexOperations(t *testing.T) {
	// 创建数据库连接
	db, err := Open("mysql", "remote:123456@tcp(192.168.110.249:3306)/test?charset=utf8mb4&parseTime=True", nil)
	assert.NoError(t, err)
	defer db.Close()

	// 清理旧表
	db.DB.Exec("DROP TABLE IF EXISTS domain_services")
	db.DB.Exec("DROP TABLE IF EXISTS services")
	db.DB.Exec("DROP TABLE IF EXISTS domains")

	// 创建测试表
	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS domains (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			identifier VARCHAR(255) NOT NULL,
			description TEXT,
			service_count INT DEFAULT 0,
			status INT NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_name (name),
			UNIQUE KEY uk_identifier (identifier)
		)
	`)
	assert.NoError(t, err)

	// 创建关联表
	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS services (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			description TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)
	`)
	assert.NoError(t, err)

	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS domain_services (
			domain_id BIGINT UNSIGNED NOT NULL,
			service_id BIGINT UNSIGNED NOT NULL,
			PRIMARY KEY (domain_id, service_id),
			CONSTRAINT fk_domain_services_domain FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE CASCADE,
			CONSTRAINT fk_domain_services_service FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
		)
	`)
	assert.NoError(t, err)

	// 清理测试数据
	defer func() {
		db.DB.Exec("DROP TABLE IF EXISTS domain_services")
		db.DB.Exec("DROP TABLE IF EXISTS services")
		db.DB.Exec("DROP TABLE IF EXISTS domains")
	}()

	// 1. 批量插入多个域名
	domains := []Domain{
		{
			Name:         "Domain 1",
			DomainName:   "domain-1",
			Description:  "Description 1",
			ServiceCount: 0,
			Status:       1,
		},
		{
			Name:         "Domain 2",
			DomainName:   "domain-2",
			Description:  "Description 2",
			ServiceCount: 0,
			Status:       2,
		},
		{
			Name:         "Domain 3",
			DomainName:   "domain-3",
			Description:  "Description 3",
			ServiceCount: 0,
			Status:       1,
		},
	}

	// 插入域名
	for i := range domains {
		result := db.Chain().Table("domains").Values(map[string]interface{}{
			"name":          domains[i].Name,
			"identifier":    domains[i].DomainName,
			"description":   domains[i].Description,
			"service_count": domains[i].ServiceCount,
			"status":        domains[i].Status,
		}).Save()
		assert.NoError(t, result.Error)
		domains[i].ID = uint(result.ID)
	}

	// 2. 测试复杂查询
	// 2.1 按状态分组统计
	var statusCounts []struct {
		Status int64 `gom:"status"`
		Count  int64 `gom:"count"`
	}
	result := db.Chain().
		Table("domains").
		Fields("status", "COUNT(*) as count").
		GroupBy("status").
		OrderBy("status").
		List(&statusCounts)
	assert.NoError(t, result.Error)
	assert.Len(t, statusCounts, 2)
	assert.Equal(t, int64(2), statusCounts[0].Count) // status 1 有两个
	assert.Equal(t, int64(1), statusCounts[1].Count) // status 2 有一个

	// 2.2 使用 IN 查询
	var activeDomains []Domain
	result = db.Chain().
		Table("domains").
		Where("status", define.OpIn, []interface{}{1}).
		OrderBy("id").
		List(&activeDomains)
	assert.NoError(t, result.Error)
	assert.Len(t, activeDomains, 2)
	assert.Equal(t, "Domain 1", activeDomains[0].Name)
	assert.Equal(t, "Domain 3", activeDomains[1].Name)

	// 2.3 模糊查询
	var searchedDomains []Domain
	result = db.Chain().
		Table("domains").
		Where("name", define.OpLike, "%2%").
		List(&searchedDomains)
	assert.NoError(t, result.Error)
	assert.Len(t, searchedDomains, 1)
	assert.Equal(t, "Domain 2", searchedDomains[0].Name)

	// 2.4 多条件组合查询
	var complexDomains []Domain
	result = db.Chain().
		Table("domains").
		Where("status", define.OpEq, 1).
		Where("service_count", define.OpEq, 0).
		OrderBy("id").
		List(&complexDomains)
	assert.NoError(t, result.Error)
	assert.Len(t, complexDomains, 2)

	// 3. 测试批量更新
	updateResult := db.Chain().
		Table("domains").
		Where("status", define.OpEq, 1).
		Values(map[string]interface{}{
			"service_count": 1,
		}).Save()
	assert.NoError(t, updateResult.Error)

	// 验证更新结果
	var updatedDomains []Domain
	result = db.Chain().
		Table("domains").
		Where("service_count", define.OpEq, 1).
		OrderBy("id").
		List(&updatedDomains)
	assert.NoError(t, result.Error)
	assert.Len(t, updatedDomains, 2)
	for _, d := range updatedDomains {
		assert.Equal(t, 1, d.ServiceCount)
		assert.Equal(t, 1, d.Status)
	}

	// 4. 测试分页查询
	var pagedDomains []Domain
	result = db.Chain().
		Table("domains").
		OrderBy("id").
		Limit(2).
		List(&pagedDomains)
	assert.NoError(t, result.Error)
	assert.Len(t, pagedDomains, 2)
	assert.Equal(t, "Domain 1", pagedDomains[0].Name)
	assert.Equal(t, "Domain 2", pagedDomains[1].Name)

	// 第二页
	result = db.Chain().
		Table("domains").
		OrderBy("id").
		Limit(2).
		Offset(2).
		List(&pagedDomains)
	assert.NoError(t, result.Error)
	assert.Len(t, pagedDomains, 1)
	assert.Equal(t, "Domain 3", pagedDomains[0].Name)

	// 5. 测试聚合函数
	var avgResults []struct {
		AvgStatus float64 `gom:"avg_status"`
	}
	result = db.Chain().
		Table("domains").
		Fields("AVG(status) as avg_status").
		List(&avgResults)
	assert.NoError(t, result.Error)
	assert.Len(t, avgResults, 1)
	assert.InDelta(t, 1.33, avgResults[0].AvgStatus, 0.01)

	var maxResults []struct {
		MaxStatus int64 `gom:"max_status"`
	}
	result = db.Chain().
		Table("domains").
		Fields("MAX(status) as max_status").
		List(&maxResults)
	assert.NoError(t, result.Error)
	assert.Len(t, maxResults, 1)
	assert.Equal(t, int64(2), maxResults[0].MaxStatus)

	// 6. 测试复杂条件查询
	var rawDomains []Domain
	result = db.Chain().
		Table("domains").
		Where("status", define.OpEq, 1).
		Where("service_count", define.OpGt, 0).
		OrderBy("id").
		List(&rawDomains)
	assert.NoError(t, result.Error)
	assert.Len(t, rawDomains, 2)
}

func TestDomainEdgeCases(t *testing.T) {
	// 创建数据库连接
	db, err := Open("mysql", "remote:123456@tcp(192.168.110.249:3306)/test?charset=utf8mb4&parseTime=True", nil)
	assert.NoError(t, err)
	defer db.Close()

	// 清理旧表
	db.DB.Exec("DROP TABLE IF EXISTS domain_services")
	db.DB.Exec("DROP TABLE IF EXISTS services")
	db.DB.Exec("DROP TABLE IF EXISTS domains")

	// 创建测试表
	_, err = db.DB.Exec(`
		CREATE TABLE IF NOT EXISTS domains (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			identifier VARCHAR(255) NOT NULL,
			description TEXT,
			service_count INT DEFAULT 0,
			status INT NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_name (name),
			UNIQUE KEY uk_identifier (identifier)
		)
	`)
	assert.NoError(t, err)

	// 清理测试数据
	defer func() {
		db.DB.Exec("DROP TABLE IF EXISTS domains")
	}()

	// 1. 测试空字段
	emptyDomain := &Domain{
		Name:       "Empty Domain",
		DomainName: "empty-domain",
		// Description 故意留空
		// ServiceCount 使用零值
		// Status 使用零值
	}

	result := db.Chain().Table("domains").Values(map[string]interface{}{
		"name":       emptyDomain.Name,
		"identifier": emptyDomain.DomainName,
	}).Save()
	assert.NoError(t, result.Error)
	emptyDomain.ID = uint(result.ID)

	// 验证空字段查询
	var emptyDomains []Domain
	listResult := db.Chain().Table("domains").Where("id", define.OpEq, emptyDomain.ID).List(&emptyDomains)
	assert.NoError(t, listResult.Error)
	assert.Len(t, emptyDomains, 1)
	fetchedEmpty := emptyDomains[0]
	assert.Equal(t, "", fetchedEmpty.Description)
	assert.Equal(t, 0, fetchedEmpty.ServiceCount)
	assert.Equal(t, 0, fetchedEmpty.Status)

	// 2. 测试特殊字符
	specialDomain := &Domain{
		Name:         "Special Domain !@#$%^&*()",
		DomainName:   "special-domain-123",
		Description:  "Description with 中文 and emoji 🎉",
		ServiceCount: 0,
		Status:       1,
	}

	result = db.Chain().Table("domains").Values(map[string]interface{}{
		"name":          specialDomain.Name,
		"identifier":    specialDomain.DomainName,
		"description":   specialDomain.Description,
		"service_count": specialDomain.ServiceCount,
		"status":        specialDomain.Status,
	}).Save()
	assert.NoError(t, result.Error)
	specialDomain.ID = uint(result.ID)

	// 验证特殊字符查询
	var specialDomains []Domain
	listResult = db.Chain().Table("domains").Where("id", define.OpEq, specialDomain.ID).List(&specialDomains)
	assert.NoError(t, listResult.Error)
	assert.Len(t, specialDomains, 1)
	fetchedSpecial := specialDomains[0]
	assert.Equal(t, specialDomain.Name, fetchedSpecial.Name)
	assert.Equal(t, specialDomain.Description, fetchedSpecial.Description)

	// 3. 测试极限值
	limitDomain := &Domain{
		Name:         strings.Repeat("a", 255),  // 最大长度
		DomainName:   strings.Repeat("b", 255),  // 最大长度
		Description:  strings.Repeat("c", 1000), // 大文本
		ServiceCount: math.MaxInt32,             // 最大int值
		Status:       -1,                        // 负值
	}

	result = db.Chain().Table("domains").Values(map[string]interface{}{
		"name":          limitDomain.Name,
		"identifier":    limitDomain.DomainName,
		"description":   limitDomain.Description,
		"service_count": limitDomain.ServiceCount,
		"status":        limitDomain.Status,
	}).Save()
	assert.NoError(t, result.Error)
	limitDomain.ID = uint(result.ID)

	// 验证极限值查询
	var limitDomains []Domain
	listResult = db.Chain().Table("domains").Where("id", define.OpEq, limitDomain.ID).List(&limitDomains)
	assert.NoError(t, listResult.Error)
	assert.Len(t, limitDomains, 1)
	fetchedLimit := limitDomains[0]
	assert.Equal(t, 255, len(fetchedLimit.Name))
	assert.Equal(t, 255, len(fetchedLimit.DomainName))
	assert.Equal(t, 1000, len(fetchedLimit.Description))
	assert.Equal(t, math.MaxInt32, fetchedLimit.ServiceCount)
	assert.Equal(t, -1, fetchedLimit.Status)

	// 4. 测试并发插入和查询
	var wg sync.WaitGroup
	domainCount := 10
	errorChan := make(chan error, domainCount*2) // 用于收集错误

	// 并发插入
	for i := 0; i < domainCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			concurrentDomain := &Domain{
				Name:         fmt.Sprintf("Concurrent Domain %d", index),
				DomainName:   fmt.Sprintf("concurrent-domain-%d", index),
				Description:  fmt.Sprintf("Description %d", index),
				ServiceCount: index,
				Status:       1,
			}

			result := db.Chain().Table("domains").Values(map[string]interface{}{
				"name":          concurrentDomain.Name,
				"identifier":    concurrentDomain.DomainName,
				"description":   concurrentDomain.Description,
				"service_count": concurrentDomain.ServiceCount,
				"status":        concurrentDomain.Status,
			}).Save()

			if result.Error != nil {
				errorChan <- fmt.Errorf("insert error at %d: %v", index, result.Error)
			}
		}(i)
	}

	// 并发查询
	for i := 0; i < domainCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			var domains []Domain
			result := db.Chain().
				Table("domains").
				Where("name", define.OpLike, fmt.Sprintf("%%Concurrent Domain %d", index)).
				List(&domains)

			if result.Error != nil {
				errorChan <- fmt.Errorf("query error at %d: %v", index, result.Error)
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// 检查是否有错误发生
	for err := range errorChan {
		assert.NoError(t, err)
	}

	// 验证并发操作结果
	var totalCount int64
	totalCount, err = db.Chain().Table("domains").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(domainCount+3), totalCount) // 3是之前插入的测试数据

	// 5. 测试唯一约束
	duplicateDomain := &Domain{
		Name:       "Empty Domain", // 重复的名称
		DomainName: "empty-domain", // 重复的标识符
	}

	result = db.Chain().Table("domains").Values(map[string]interface{}{
		"name":       duplicateDomain.Name,
		"identifier": duplicateDomain.DomainName,
	}).Save()
	assert.Error(t, result.Error) // 应该返回错误

	// 6. 测试事务操作
	tx, err := db.BeginChain()
	assert.NoError(t, err)

	// 在事务中执行插入
	txDomain := &Domain{
		Name:       "Transaction Domain",
		DomainName: "transaction-domain",
		Status:     1,
	}

	result = tx.Table("domains").Values(map[string]interface{}{
		"name":       txDomain.Name,
		"identifier": txDomain.DomainName,
		"status":     txDomain.Status,
	}).Save()
	assert.NoError(t, result.Error)

	// 故意制造错误（插入重复数据）
	result = tx.Table("domains").Values(map[string]interface{}{
		"name":       txDomain.Name,
		"identifier": txDomain.DomainName,
		"status":     txDomain.Status,
	}).Save()
	assert.Error(t, result.Error)

	// 回滚事务
	err = tx.Rollback()
	assert.NoError(t, err)

	// 验证事务回滚
	var txCount int64
	txCount, err = db.Chain().Table("domains").Where("name", define.OpEq, txDomain.Name).Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), txCount)
}
