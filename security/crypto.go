package security

import (
	"encoding/json"
	"errors"
	"fmt"
)

func getKey(config *EncryptionConfig) ([]byte, error) {
	if keyStr, ok := config.KeySourceConfig["key"]; ok {
		if len(keyStr) < 32 {
			return nil, fmt.Errorf("key must be at least 32 bytes")
		}
		return []byte(keyStr[:32]), nil // 截取前32字节
	}
	return nil, errors.New("encryption key not configured")
}

func EncryptData(data interface{}, config *EncryptionConfig) (string, error) {
	// Convert data to string if needed
	var strData string
	switch v := data.(type) {
	case string:
		strData = v
	case []byte:
		strData = string(v)
	default:
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return "", err
		}
		strData = string(jsonBytes)
	}

	return EncryptValue(strData, config)
}
