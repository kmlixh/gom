package security

import (
	"encoding/base64"
	"os"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
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

func TestChaCha20Poly1305Encryption(t *testing.T) {
	// Create a 32-byte key for ChaCha20-Poly1305
	key := make([]byte, chacha20poly1305.KeySize)
	for i := range key {
		key[i] = byte(i % 256)
	}

	keyStr := base64.StdEncoding.EncodeToString(key)
	os.Setenv("TEST_ENCRYPTION_KEY", keyStr)
	defer os.Unsetenv("TEST_ENCRYPTION_KEY")

	config := &EncryptionConfig{
		Algorithm: ChaCha20Poly1305,
		KeySource: "env",
		KeySourceConfig: map[string]string{
			"key_name": "TEST_ENCRYPTION_KEY",
		},
	}

	testData := []struct {
		name  string
		value string
	}{
		{
			name:  "encrypt short text",
			value: "hello",
		},
		{
			name:  "encrypt long text",
			value: "this is a long text that needs to be encrypted with ChaCha20-Poly1305",
		},
		{
			name:  "encrypt with special chars",
			value: "hello!@#$%^&*()",
		},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			encrypted, err := EncryptValue(tt.value, config)
			if err != nil {
				t.Errorf("EncryptValue() error = %v", err)
				return
			}

			decrypted, err := DecryptValue(encrypted, config)
			if err != nil {
				t.Errorf("DecryptValue() error = %v", err)
				return
			}

			if decrypted != tt.value {
				t.Errorf("DecryptValue() = %v, want %v", decrypted, tt.value)
			}
		})
	}
}
