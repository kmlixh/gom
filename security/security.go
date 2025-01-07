package security

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// Masker 定义了数据脱敏接口
type Masker interface {
	Mask(value string, ruleType string) string
}

// Encryptor 定义了数据加密接口
type Encryptor interface {
	Encrypt(value string) (string, error)
	Decrypt(value string) (string, error)
}

// DefaultMasker 提供默认的脱敏实现
type DefaultMasker struct {
	// 自定义脱敏规则
	rules map[string]*regexp.Regexp
}

// NewDefaultMasker 创建默认的脱敏器
func NewDefaultMasker() *DefaultMasker {
	return &DefaultMasker{
		rules: map[string]*regexp.Regexp{
			"phone":    regexp.MustCompile(`^(\d{3})\d*(\d{4})$`),
			"email":    regexp.MustCompile(`^(.{2}).*(@.*)$`),
			"idcard":   regexp.MustCompile(`^(\d{6})\d*(\d{4})$`),
			"bankcard": regexp.MustCompile(`^(\d{4})\d*(\d{4})$`),
			"name":     regexp.MustCompile(`^([\p{L}]{1}).*$`),
			"address":  regexp.MustCompile(`^(.{6}).*(.{4})$`),
			"password": regexp.MustCompile(`^.*$`),
		},
	}
}

// AddRule 添加自定义脱敏规则
func (m *DefaultMasker) AddRule(name string, pattern string) error {
	reg, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	m.rules[name] = reg
	return nil
}

// Mask 执行脱敏操作
func (m *DefaultMasker) Mask(value string, ruleType string) string {
	if rule, ok := m.rules[ruleType]; ok {
		switch ruleType {
		case "phone":
			return rule.ReplaceAllString(value, "$1****$2")
		case "email":
			return rule.ReplaceAllString(value, "$1****$2")
		case "idcard":
			return rule.ReplaceAllString(value, "$1********$2")
		case "bankcard":
			return rule.ReplaceAllString(value, "$1********$2")
		case "name":
			return rule.ReplaceAllString(value, "$1**")
		case "address":
			return rule.ReplaceAllString(value, "$1****$2")
		case "password":
			return "********"
		case "custom":
			return rule.ReplaceAllString(value, "$1****$3")
		default:
			return strings.Repeat("*", len(value))
		}
	}
	return value
}

// AESEncryptor 提供AES加密实现
type AESEncryptor struct {
	key []byte
}

// NewAESEncryptor 创建AES加密器
func NewAESEncryptor(key []byte) (*AESEncryptor, error) {
	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return nil, errors.New("invalid key size: must be 16, 24, or 32 bytes")
	}
	return &AESEncryptor{key: key}, nil
}

// Encrypt 加密数据
func (e *AESEncryptor) Encrypt(value string) (string, error) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}

	// 创建随机IV
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}

	// 填充数据
	paddedData := pkcs7Pad([]byte(value), aes.BlockSize)

	// 加密
	mode := cipher.NewCBCEncrypter(block, iv)
	ciphertext := make([]byte, len(paddedData))
	mode.CryptBlocks(ciphertext, paddedData)

	// 组合IV和密文
	combined := make([]byte, len(iv)+len(ciphertext))
	copy(combined, iv)
	copy(combined[len(iv):], ciphertext)

	// Base64编码
	return base64.StdEncoding.EncodeToString(combined), nil
}

// Decrypt 解密数据
func (e *AESEncryptor) Decrypt(value string) (string, error) {
	// Base64解码
	combined, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}

	if len(combined) < aes.BlockSize {
		return "", errors.New("ciphertext too short")
	}

	// 分离IV和密文
	iv := combined[:aes.BlockSize]
	ciphertext := combined[aes.BlockSize:]

	// 解密
	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(ciphertext))
	mode.CryptBlocks(plaintext, ciphertext)

	// 去除填充
	unpaddedData, err := pkcs7Unpad(plaintext)
	if err != nil {
		return "", err
	}

	return string(unpaddedData), nil
}

// pkcs7Pad 添加PKCS7填充
func pkcs7Pad(data []byte, blockSize int) []byte {
	padding := blockSize - len(data)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(data, padtext...)
}

// pkcs7Unpad 移除PKCS7填充
func pkcs7Unpad(data []byte) ([]byte, error) {
	length := len(data)
	if length == 0 {
		return nil, errors.New("empty data")
	}
	padding := int(data[length-1])
	if padding > length {
		return nil, errors.New("invalid padding")
	}
	return data[:length-padding], nil
}

// SecurityManager 安全管理器，集成脱敏和加密功能
type SecurityManager struct {
	masker    Masker
	encryptor Encryptor
}

// NewSecurityManager 创建安全管理器
func NewSecurityManager(key []byte) (*SecurityManager, error) {
	encryptor, err := NewAESEncryptor(key)
	if err != nil {
		return nil, err
	}

	return &SecurityManager{
		masker:    NewDefaultMasker(),
		encryptor: encryptor,
	}, nil
}

// MaskData 脱敏数据
func (sm *SecurityManager) MaskData(value string, ruleType string) string {
	return sm.masker.Mask(value, ruleType)
}

// EncryptData 加密数据
func (sm *SecurityManager) EncryptData(value string) (string, error) {
	return sm.encryptor.Encrypt(value)
}

// DecryptData 解密数据
func (sm *SecurityManager) DecryptData(value string) (string, error) {
	return sm.encryptor.Decrypt(value)
}

// FieldSecurity 字段安全配置
type FieldSecurity struct {
	MaskType     string
	NeedEncrypt  bool
	CustomFormat string
}

// DataProcessor 数据处理器
type DataProcessor struct {
	securityManager *SecurityManager
	fieldConfig     map[string]FieldSecurity
}

// NewDataProcessor 创建数据处理器
func NewDataProcessor(key []byte) (*DataProcessor, error) {
	sm, err := NewSecurityManager(key)
	if err != nil {
		return nil, err
	}

	return &DataProcessor{
		securityManager: sm,
		fieldConfig:     make(map[string]FieldSecurity),
	}, nil
}

// ConfigureField 配置字段安全规则
func (dp *DataProcessor) ConfigureField(field string, config FieldSecurity) {
	dp.fieldConfig[field] = config
}

// ProcessData 处理数据
func (dp *DataProcessor) ProcessData(data map[string]interface{}) error {
	for field, value := range data {
		if config, ok := dp.fieldConfig[field]; ok {
			strValue, ok := value.(string)
			if !ok {
				continue
			}

			if config.NeedEncrypt {
				encrypted, err := dp.securityManager.EncryptData(strValue)
				if err != nil {
					return fmt.Errorf("failed to encrypt field %s: %v", field, err)
				}
				data[field] = encrypted
			} else if config.MaskType != "" {
				data[field] = dp.securityManager.MaskData(strValue, config.MaskType)
			}
		}
	}
	return nil
}

// UnprocessData 还原数据
func (dp *DataProcessor) UnprocessData(data map[string]interface{}) error {
	for field, value := range data {
		if config, ok := dp.fieldConfig[field]; ok {
			strValue, ok := value.(string)
			if !ok {
				continue
			}

			if config.NeedEncrypt {
				decrypted, err := dp.securityManager.DecryptData(strValue)
				if err != nil {
					return fmt.Errorf("failed to decrypt field %s: %v", field, err)
				}
				data[field] = decrypted
			}
		}
	}
	return nil
}
