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
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

// EncryptionAlgorithm defines the encryption algorithm to use
type EncryptionAlgorithm string

const (
	// AES256 uses AES-256 encryption
	AES256 EncryptionAlgorithm = "AES256"
	// AES192 uses AES-192 encryption
	AES192 EncryptionAlgorithm = "AES192"
	// AES128 uses AES-128 encryption
	AES128 EncryptionAlgorithm = "AES128"
	// ChaCha20Poly1305 uses ChaCha20-Poly1305 encryption
	ChaCha20Poly1305 EncryptionAlgorithm = "ChaCha20Poly1305"
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
	masker            Masker
	encryptor         Encryptor
	keyRotationInfo   *KeyRotationInfo
	oldEncryptor      Encryptor
	keyRotationConfig *KeyRotationConfig
}

// NewSecurityManager 创建安全管理器
func NewSecurityManager(key []byte) (*SecurityManager, error) {
	encryptor, err := NewAESEncryptor(key)
	if err != nil {
		return nil, err
	}

	return &SecurityManager{
		masker:            NewDefaultMasker(),
		encryptor:         encryptor,
		keyRotationInfo:   nil,
		oldEncryptor:      nil,
		keyRotationConfig: nil,
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

// KeyRotationInfo stores information about key rotation
type KeyRotationInfo struct {
	LastRotation time.Time
	KeyVersion   int
	KeyID        string
}

// RotateKey performs key rotation based on the configured schedule
func (sm *SecurityManager) RotateKey() error {
	if sm.encryptor == nil {
		return errors.New("encryptor not initialized")
	}

	// Check if key rotation is needed
	if sm.keyRotationInfo == nil || time.Since(sm.keyRotationInfo.LastRotation) >= sm.keyRotationConfig.Interval {
		// Generate new key
		newKey := make([]byte, 32)
		if _, err := rand.Read(newKey); err != nil {
			return fmt.Errorf("failed to generate new key: %w", err)
		}

		// Create new encryptor with new key
		newEncryptor, err := NewAESEncryptor(newKey)
		if err != nil {
			return fmt.Errorf("failed to create new encryptor: %w", err)
		}

		// Update key info
		sm.keyRotationInfo = &KeyRotationInfo{
			LastRotation: time.Now(),
			KeyVersion:   sm.keyRotationInfo.KeyVersion + 1,
			KeyID:        fmt.Sprintf("key-%d", sm.keyRotationInfo.KeyVersion+1),
		}

		// Store old encryptor for decryption of old data
		sm.oldEncryptor = sm.encryptor
		sm.encryptor = newEncryptor
	}

	return nil
}

// EncryptValue enhances encryption with ChaCha20-Poly1305 support
func EncryptValue(value string, config *EncryptionConfig) (string, error) {
	if config == nil {
		return "", newDBError(ErrConfiguration, "encryptValue", nil, "encryption configuration is required")
	}

	key, err := getEncryptionKey(config)
	if err != nil {
		return "", newDBError(ErrConfiguration, "encryptValue", err, "failed to get encryption key")
	}

	var aead cipher.AEAD
	switch config.Algorithm {
	case ChaCha20Poly1305:
		if len(key) != chacha20poly1305.KeySize {
			return "", newDBError(ErrConfiguration, "encryptValue", nil, fmt.Sprintf("invalid key size for ChaCha20-Poly1305: must be %d bytes", chacha20poly1305.KeySize))
		}
		aead, err = chacha20poly1305.New(key)
		if err != nil {
			return "", newDBError(ErrConfiguration, "encryptValue", err, "failed to create ChaCha20-Poly1305")
		}
	case AES256, AES192, AES128:
		var block cipher.Block
		block, err = aes.NewCipher(key)
		if err != nil {
			return "", newDBError(ErrConfiguration, "encryptValue", err, "failed to create cipher")
		}
		aead, err = cipher.NewGCM(block)
		if err != nil {
			return "", newDBError(ErrConfiguration, "encryptValue", err, "failed to create GCM")
		}
	default:
		return "", newDBError(ErrConfiguration, "encryptValue", nil, "unsupported encryption algorithm")
	}

	// Create nonce
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", newDBError(ErrConfiguration, "encryptValue", err, "failed to generate nonce")
	}

	// Encrypt and combine nonce with ciphertext
	ciphertext := aead.Seal(nonce, nonce, []byte(value), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// DecryptValue enhances decryption with ChaCha20-Poly1305 support
func DecryptValue(encryptedValue string, config *EncryptionConfig) (string, error) {
	if config == nil {
		return "", newDBError(ErrConfiguration, "decryptValue", nil, "encryption configuration is required")
	}

	key, err := getEncryptionKey(config)
	if err != nil {
		return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to get encryption key")
	}

	var aead cipher.AEAD
	switch config.Algorithm {
	case ChaCha20Poly1305:
		if len(key) != chacha20poly1305.KeySize {
			return "", newDBError(ErrConfiguration, "decryptValue", nil, fmt.Sprintf("invalid key size for ChaCha20-Poly1305: must be %d bytes", chacha20poly1305.KeySize))
		}
		aead, err = chacha20poly1305.New(key)
		if err != nil {
			return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to create ChaCha20-Poly1305")
		}
	case AES256, AES192, AES128:
		var block cipher.Block
		block, err = aes.NewCipher(key)
		if err != nil {
			return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to create cipher")
		}
		aead, err = cipher.NewGCM(block)
		if err != nil {
			return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to create GCM")
		}
	default:
		return "", newDBError(ErrConfiguration, "decryptValue", nil, "unsupported encryption algorithm")
	}

	// Decode base64
	ciphertext, err := base64.StdEncoding.DecodeString(encryptedValue)
	if err != nil {
		return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to decode base64")
	}

	// Extract nonce and decrypt
	nonceSize := aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", newDBError(ErrConfiguration, "decryptValue", nil, "ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", newDBError(ErrConfiguration, "decryptValue", err, "failed to decrypt")
	}

	return string(plaintext), nil
}

// ErrorCode represents error codes for security operations
type ErrorCode string

const (
	ErrConfiguration ErrorCode = "CONFIGURATION_ERROR"
	ErrEncryption    ErrorCode = "ENCRYPTION_ERROR"
	ErrDecryption    ErrorCode = "DECRYPTION_ERROR"
)

// DBError represents a database operation error
type DBError struct {
	Code    ErrorCode
	Op      string
	Err     error
	Message string
}

func (e *DBError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s - %s (%v)", e.Code, e.Op, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s - %s", e.Code, e.Op, e.Message)
}

func newDBError(code ErrorCode, op string, err error, message string) error {
	return &DBError{
		Code:    code,
		Op:      op,
		Err:     err,
		Message: message,
	}
}

// KeyRotationConfig defines the configuration for key rotation
type KeyRotationConfig struct {
	Interval    time.Duration
	AutoRotate  bool
	BackupCount int
}

// EncryptionConfig represents configuration for encryption operations
type EncryptionConfig struct {
	Algorithm       EncryptionAlgorithm
	KeySource       string
	KeySourceConfig map[string]string
}

// getEncryptionKey retrieves the encryption key from the configured source
func getEncryptionKey(config *EncryptionConfig) ([]byte, error) {
	if config == nil {
		return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "encryption configuration is required")
	}

	switch config.KeySource {
	case "env":
		keyName, ok := config.KeySourceConfig["key_name"]
		if !ok {
			return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "key_name not specified for env source")
		}
		keyStr := os.Getenv(keyName)
		if keyStr == "" {
			return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "encryption key not found in environment")
		}
		// Decode base64-encoded key
		key, err := base64.StdEncoding.DecodeString(keyStr)
		if err != nil {
			return nil, newDBError(ErrConfiguration, "getEncryptionKey", err, "failed to decode base64 key")
		}
		return key, nil
	case "file":
		keyPath, ok := config.KeySourceConfig["key_path"]
		if !ok {
			return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "key_path not specified for file source")
		}
		return ioutil.ReadFile(keyPath)
	case "vault":
		return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "vault key source not implemented")
	default:
		return nil, newDBError(ErrConfiguration, "getEncryptionKey", nil, "unsupported key source")
	}
}
