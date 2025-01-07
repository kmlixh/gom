package security

import (
	"testing"
)

func TestDefaultMasker(t *testing.T) {
	masker := NewDefaultMasker()

	tests := []struct {
		name     string
		value    string
		ruleType string
		want     string
	}{
		{
			name:     "mask phone",
			value:    "13812345678",
			ruleType: "phone",
			want:     "138****5678",
		},
		{
			name:     "mask email",
			value:    "test@example.com",
			ruleType: "email",
			want:     "te****@example.com",
		},
		{
			name:     "mask idcard",
			value:    "440101199001011234",
			ruleType: "idcard",
			want:     "440101********1234",
		},
		{
			name:     "mask bankcard",
			value:    "6222021234567890123",
			ruleType: "bankcard",
			want:     "6222********0123",
		},
		{
			name:     "mask name",
			value:    "张三",
			ruleType: "name",
			want:     "张**",
		},
		{
			name:     "mask password",
			value:    "mypassword123",
			ruleType: "password",
			want:     "********",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := masker.Mask(tt.value, tt.ruleType)
			if got != tt.want {
				t.Errorf("Mask() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAESEncryptor(t *testing.T) {
	key := []byte("0123456789abcdef") // 16 bytes key for testing
	encryptor, err := NewAESEncryptor(key)
	if err != nil {
		t.Fatalf("NewAESEncryptor() error = %v", err)
	}

	tests := []struct {
		name  string
		value string
	}{
		{
			name:  "encrypt short text",
			value: "hello",
		},
		{
			name:  "encrypt long text",
			value: "this is a long text that needs to be encrypted with multiple blocks",
		},
		{
			name:  "encrypt with special chars",
			value: "hello!@#$%^&*()",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 测试加密
			encrypted, err := encryptor.Encrypt(tt.value)
			if err != nil {
				t.Errorf("Encrypt() error = %v", err)
				return
			}

			// 测试解密
			decrypted, err := encryptor.Decrypt(encrypted)
			if err != nil {
				t.Errorf("Decrypt() error = %v", err)
				return
			}

			// 验证解密后的结果是否与原文相同
			if decrypted != tt.value {
				t.Errorf("Decrypt() = %v, want %v", decrypted, tt.value)
			}
		})
	}
}

func TestDataProcessor(t *testing.T) {
	key := []byte("0123456789abcdef")
	processor, err := NewDataProcessor(key)
	if err != nil {
		t.Fatalf("NewDataProcessor() error = %v", err)
	}

	// 配置字段安全规则
	processor.ConfigureField("phone", FieldSecurity{MaskType: "phone"})
	processor.ConfigureField("password", FieldSecurity{NeedEncrypt: true})
	processor.ConfigureField("email", FieldSecurity{MaskType: "email"})

	// 准备测试数据
	data := map[string]interface{}{
		"phone":    "13812345678",
		"password": "mypassword123",
		"email":    "test@example.com",
		"name":     "John", // 未配置的字段
	}

	// 测试数据处理
	err = processor.ProcessData(data)
	if err != nil {
		t.Fatalf("ProcessData() error = %v", err)
	}

	// 验证脱敏结果
	if data["phone"] != "138****5678" {
		t.Errorf("ProcessData() phone = %v, want %v", data["phone"], "138****5678")
	}

	if data["email"] != "te****@example.com" {
		t.Errorf("ProcessData() email = %v, want %v", data["email"], "te****@example.com")
	}

	// 验证加密字段不等于原值
	if data["password"] == "mypassword123" {
		t.Error("ProcessData() password was not encrypted")
	}

	// 验证未配置的字段保持不变
	if data["name"] != "John" {
		t.Errorf("ProcessData() name = %v, want %v", data["name"], "John")
	}

	// 测试数据还原
	originalPassword := "mypassword123"
	err = processor.UnprocessData(data)
	if err != nil {
		t.Fatalf("UnprocessData() error = %v", err)
	}

	// 验证加密字段是否正确还原
	if data["password"] != originalPassword {
		t.Errorf("UnprocessData() password = %v, want %v", data["password"], originalPassword)
	}
}

func TestAddCustomRule(t *testing.T) {
	masker := NewDefaultMasker()

	// 添加自定义规则
	err := masker.AddRule("custom", `^(.{2})(.*)(.{2})$`)
	if err != nil {
		t.Fatalf("AddRule() error = %v", err)
	}

	// 测试自定义规则
	value := "abcdefgh"
	masked := masker.Mask(value, "custom")
	expected := "ab****gh"
	if masked != expected {
		t.Errorf("Mask() with custom rule = %v, want %v", masked, expected)
	}
}

func TestInvalidKey(t *testing.T) {
	// 测试无效的密钥长度
	invalidKey := []byte("invalid")
	_, err := NewAESEncryptor(invalidKey)
	if err == nil {
		t.Error("NewAESEncryptor() should return error for invalid key size")
	}
}

func TestInvalidEncryptedData(t *testing.T) {
	key := []byte("0123456789abcdef")
	encryptor, _ := NewAESEncryptor(key)

	// 测试解密无效的数据
	_, err := encryptor.Decrypt("invalid base64 data")
	if err == nil {
		t.Error("Decrypt() should return error for invalid data")
	}
}

func TestDefaultMaskerEdgeCases(t *testing.T) {
	masker := NewDefaultMasker()

	// 测试未知规则类型
	value := "test"
	result := masker.Mask(value, "unknown")
	if result != value {
		t.Errorf("Mask() with unknown rule = %v, want %v", result, value)
	}

	// 测试空值
	result = masker.Mask("", "phone")
	if result != "" {
		t.Errorf("Mask() with empty value = %v, want empty string", result)
	}

	// 测试无效的手机号格式
	result = masker.Mask("123", "phone")
	if result != "123" {
		t.Errorf("Mask() with invalid phone = %v, want %v", result, "123")
	}
}

func TestSecurityManager(t *testing.T) {
	key := []byte("0123456789abcdef")
	sm, err := NewSecurityManager(key)
	if err != nil {
		t.Fatalf("NewSecurityManager() error = %v", err)
	}

	// 测试脱敏
	masked := sm.MaskData("13812345678", "phone")
	if masked != "138****5678" {
		t.Errorf("MaskData() = %v, want %v", masked, "138****5678")
	}

	// 测试加密和解密
	original := "sensitive data"
	encrypted, err := sm.EncryptData(original)
	if err != nil {
		t.Errorf("EncryptData() error = %v", err)
		return
	}

	decrypted, err := sm.DecryptData(encrypted)
	if err != nil {
		t.Errorf("DecryptData() error = %v", err)
		return
	}

	if decrypted != original {
		t.Errorf("DecryptData() = %v, want %v", decrypted, original)
	}
}

func TestDataProcessorEdgeCases(t *testing.T) {
	key := []byte("0123456789abcdef")
	processor, _ := NewDataProcessor(key)

	// 测试处理非字符串值
	data := map[string]interface{}{
		"number": 123,
	}
	processor.ConfigureField("number", FieldSecurity{MaskType: "custom"})
	err := processor.ProcessData(data)
	if err != nil {
		t.Errorf("ProcessData() error = %v", err)
	}
	if data["number"] != 123 {
		t.Errorf("ProcessData() modified non-string value")
	}

	// 测试未配置的字段
	data = map[string]interface{}{
		"unconfigured": "value",
	}
	err = processor.ProcessData(data)
	if err != nil {
		t.Errorf("ProcessData() error = %v", err)
	}
	if data["unconfigured"] != "value" {
		t.Errorf("ProcessData() modified unconfigured field")
	}

	// 测试空数据
	err = processor.ProcessData(nil)
	if err != nil {
		t.Errorf("ProcessData(nil) error = %v", err)
	}
}

func TestInvalidCustomRule(t *testing.T) {
	masker := NewDefaultMasker()

	// 测试添加无效的正则表达式
	err := masker.AddRule("invalid", "[") // 无效的正则表达式
	if err == nil {
		t.Error("AddRule() should return error for invalid regex")
	}
}

func TestEncryptorErrors(t *testing.T) {
	key := []byte("0123456789abcdef")
	encryptor, _ := NewAESEncryptor(key)

	// 测试加密空字符串
	encrypted, err := encryptor.Encrypt("")
	if err != nil {
		t.Errorf("Encrypt() empty string error = %v", err)
	}
	decrypted, err := encryptor.Decrypt(encrypted)
	if err != nil || decrypted != "" {
		t.Errorf("Decrypt() empty string error = %v, got %v", err, decrypted)
	}

	// 测试解密损坏的数据
	_, err = encryptor.Decrypt("invalid")
	if err == nil {
		t.Error("Decrypt() should return error for corrupted data")
	}
}
