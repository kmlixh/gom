package define

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

// ErrorCode represents specific error types
type ErrorCode string

const (
	// Database error codes
	ErrDatabaseConnection ErrorCode = "DB_CONNECTION_ERROR"
	ErrDatabaseQuery      ErrorCode = "DB_QUERY_ERROR"
	ErrDatabaseExec       ErrorCode = "DB_EXEC_ERROR"
	ErrDatabaseTx         ErrorCode = "DB_TRANSACTION_ERROR"

	// Validation error codes
	ErrValidation       ErrorCode = "VALIDATION_ERROR"
	ErrInvalidInput     ErrorCode = "INVALID_INPUT"
	ErrInvalidType      ErrorCode = "INVALID_TYPE"
	ErrRequired         ErrorCode = "REQUIRED_FIELD"
	ErrInvalidFormat    ErrorCode = "INVALID_FORMAT"
	ErrUniqueConstraint ErrorCode = "UNIQUE_CONSTRAINT"

	// Security error codes
	ErrEncryption    ErrorCode = "ENCRYPTION_ERROR"
	ErrDecryption    ErrorCode = "DECRYPTION_ERROR"
	ErrUnauthorized  ErrorCode = "UNAUTHORIZED"
	ErrKeyManagement ErrorCode = "KEY_MANAGEMENT_ERROR"

	// Configuration error codes
	ErrConfiguration ErrorCode = "CONFIGURATION_ERROR"
	ErrEnvironment   ErrorCode = "ENVIRONMENT_ERROR"

	// System error codes
	ErrInternal ErrorCode = "INTERNAL_ERROR"
	ErrTimeout  ErrorCode = "TIMEOUT_ERROR"
	ErrIO       ErrorCode = "IO_ERROR"
)

// ErrorSeverity represents the severity level of an error
type ErrorSeverity int

const (
	SeverityDebug ErrorSeverity = iota
	SeverityInfo
	SeverityWarning
	SeverityError
	SeverityCritical
)

// StackTrace represents a stack trace
type StackTrace struct {
	File     string
	Line     int
	Function string
}

// ErrorContext contains contextual information about an error
type ErrorContext struct {
	Timestamp time.Time
	Stack     []StackTrace
	Data      map[string]interface{}
}

// EnhancedError represents a detailed error with context
type EnhancedError struct {
	Code     ErrorCode
	Message  string
	Severity ErrorSeverity
	Context  ErrorContext
	Cause    error
}

func (e *EnhancedError) Error() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("[%s] %s", e.Code, e.Message))
	if e.Cause != nil {
		sb.WriteString(fmt.Sprintf(" - caused by: %v", e.Cause))
	}
	return sb.String()
}

// NewError creates a new enhanced error
func NewError(code ErrorCode, message string, severity ErrorSeverity, cause error) *EnhancedError {
	pc := make([]uintptr, 10)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])

	var stack []StackTrace
	for {
		frame, more := frames.Next()
		if !more {
			break
		}
		stack = append(stack, StackTrace{
			File:     frame.File,
			Line:     frame.Line,
			Function: frame.Function,
		})
	}

	return &EnhancedError{
		Code:     code,
		Message:  message,
		Severity: severity,
		Context: ErrorContext{
			Timestamp: time.Now(),
			Stack:     stack,
			Data:      make(map[string]interface{}),
		},
		Cause: cause,
	}
}

// WithData adds contextual data to the error
func (e *EnhancedError) WithData(key string, value interface{}) *EnhancedError {
	e.Context.Data[key] = value
	return e
}

// IsErrorCode checks if an error matches a specific error code
func IsErrorCode(err error, code ErrorCode) bool {
	if enhancedErr, ok := err.(*EnhancedError); ok {
		return enhancedErr.Code == code
	}
	return false
}

// GetErrorContext retrieves the context from an error if available
func GetErrorContext(err error) *ErrorContext {
	if enhancedErr, ok := err.(*EnhancedError); ok {
		return &enhancedErr.Context
	}
	return nil
}

// ErrorHandler handles and processes errors
type ErrorHandler struct {
	handlers map[ErrorCode]func(*EnhancedError) error
}

// NewErrorHandler creates a new error handler
func NewErrorHandler() *ErrorHandler {
	return &ErrorHandler{
		handlers: make(map[ErrorCode]func(*EnhancedError) error),
	}
}

// RegisterHandler registers a handler for a specific error code
func (h *ErrorHandler) RegisterHandler(code ErrorCode, handler func(*EnhancedError) error) {
	h.handlers[code] = handler
}

// Handle processes an error using registered handlers
func (h *ErrorHandler) Handle(err error) error {
	if enhancedErr, ok := err.(*EnhancedError); ok {
		if handler, exists := h.handlers[enhancedErr.Code]; exists {
			return handler(enhancedErr)
		}
	}
	return err
}

// RecoverHandler provides a recovery function for panics
func RecoverHandler(handler func(interface{}) error) func() {
	return func() {
		if r := recover(); r != nil {
			pc := make([]uintptr, 10)
			n := runtime.Callers(2, pc)
			frames := runtime.CallersFrames(pc[:n])

			var stack []StackTrace
			for {
				frame, more := frames.Next()
				if !more {
					break
				}
				stack = append(stack, StackTrace{
					File:     frame.File,
					Line:     frame.Line,
					Function: frame.Function,
				})
			}

			err := &EnhancedError{
				Code:     ErrInternal,
				Message:  fmt.Sprintf("panic recovered: %v", r),
				Severity: SeverityCritical,
				Context: ErrorContext{
					Timestamp: time.Now(),
					Stack:     stack,
					Data:      make(map[string]interface{}),
				},
			}

			if handler != nil {
				handler(err)
			}
		}
	}
}

// ErrorLogger provides error logging functionality
type ErrorLogger struct {
	minSeverity ErrorSeverity
	logFunc     func(error)
}

// NewErrorLogger creates a new error logger
func NewErrorLogger(minSeverity ErrorSeverity, logFunc func(error)) *ErrorLogger {
	return &ErrorLogger{
		minSeverity: minSeverity,
		logFunc:     logFunc,
	}
}

// Log logs an error if it meets the minimum severity level
func (l *ErrorLogger) Log(err error) {
	if enhancedErr, ok := err.(*EnhancedError); ok {
		if enhancedErr.Severity >= l.minSeverity {
			l.logFunc(err)
		}
	} else {
		l.logFunc(err)
	}
}

// ErrorRetrier provides retry functionality for operations that may fail
type ErrorRetrier struct {
	maxAttempts int
	backoff     time.Duration
}

// NewErrorRetrier creates a new error retrier
func NewErrorRetrier(maxAttempts int, backoff time.Duration) *ErrorRetrier {
	return &ErrorRetrier{
		maxAttempts: maxAttempts,
		backoff:     backoff,
	}
}

// Retry attempts an operation with retries
func (r *ErrorRetrier) Retry(operation func() error) error {
	var lastErr error
	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		if err := operation(); err != nil {
			lastErr = err
			if attempt < r.maxAttempts {
				time.Sleep(r.backoff * time.Duration(attempt))
				continue
			}
		} else {
			return nil
		}
	}
	return lastErr
}
