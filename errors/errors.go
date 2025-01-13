package errors

import (
	"fmt"
	"runtime"
	"strings"
)

// ErrorCode 错误码
type ErrorCode int

const (
	ErrCodeUnknown ErrorCode = iota
	ErrCodeConnection
	ErrCodeTransaction
	ErrCodeQuery
	ErrCodeExecution
	ErrCodeValidation
)

// DBError 数据库错误
type DBError struct {
	Code    ErrorCode              // 错误码
	Op      string                 // 操作名称
	Err     error                  // 原始错误
	Context map[string]interface{} // 上下文信息
	Stack   string                 // 堆栈信息
}

func (e *DBError) Error() string {
	var b strings.Builder

	// 写入基本错误信息
	fmt.Fprintf(&b, "[%d] %s: %v", e.Code, e.Op, e.Err)

	// 添加上下文信息
	if len(e.Context) > 0 {
		b.WriteString("\nContext:")
		for k, v := range e.Context {
			fmt.Fprintf(&b, "\n  %s: %v", k, v)
		}
	}

	// 添加堆栈信息
	if e.Stack != "" {
		fmt.Fprintf(&b, "\nStack:\n%s", e.Stack)
	}

	return b.String()
}

// New 创建新的数据库错误
func New(code ErrorCode, op string, err error, context map[string]interface{}) *DBError {
	return &DBError{
		Code:    code,
		Op:      op,
		Err:     err,
		Context: context,
		Stack:   getStack(),
	}
}

// Wrap 包装已有错误
func Wrap(err error, code ErrorCode, op string, context map[string]interface{}) *DBError {
	if err == nil {
		return nil
	}

	// 如果已经是DBError，则更新信息
	if dbErr, ok := err.(*DBError); ok {
		return &DBError{
			Code:    code,
			Op:      op,
			Err:     dbErr,
			Context: mergeContext(dbErr.Context, context),
			Stack:   getStack(),
		}
	}

	return New(code, op, err, context)
}

// getStack 获取堆栈信息
func getStack() string {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])

	var b strings.Builder
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if !strings.Contains(frame.File, "runtime/") {
			fmt.Fprintf(&b, "%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
		}
		if !more {
			break
		}
	}
	return b.String()
}

// mergeContext 合并上下文信息
func mergeContext(old, new map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// 复制旧的上下文
	for k, v := range old {
		result[k] = v
	}

	// 添加新的上下文
	for k, v := range new {
		result[k] = v
	}

	return result
}

// IsErrorCode 检查错误是否属于特定错误码
func IsErrorCode(err error, code ErrorCode) bool {
	if err == nil {
		return false
	}

	if dbErr, ok := err.(*DBError); ok {
		return dbErr.Code == code
	}

	return false
}
