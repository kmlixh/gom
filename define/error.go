package define

import "fmt"

const (
	ErrEncryptionConfig = iota + 1000
	ErrEncryptionAlgorithm
	ErrKeySource
)

type DBError struct {
	Code    int
	Op      string
	Message string
	Err     error
}

func (e *DBError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] Code %d: %s (detail: %v)", e.Op, e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] Code %d: %s", e.Op, e.Code, e.Message)
}
