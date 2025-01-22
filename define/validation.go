package define

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// ValidationRule defines a validation rule
type ValidationRule struct {
	Name       string
	Validate   func(value interface{}) error
	Message    string
	Parameters map[string]interface{}
}

// ValidationContext provides context for validation
type ValidationContext struct {
	FieldName string
	Value     interface{}
	Parent    interface{}
	Rules     []ValidationRule
}

// Validator defines the validation interface
type Validator interface {
	Validate(ctx *ValidationContext) error
}

// ValidationError represents a validation error
type ValidationError struct {
	Field   string
	Rule    string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// CommonValidators provides commonly used validation rules
var CommonValidators = map[string]ValidationRule{
	"required": {
		Name: "required",
		Validate: func(value interface{}) error {
			if value == nil {
				return fmt.Errorf("field is required")
			}
			if str, ok := value.(string); ok && strings.TrimSpace(str) == "" {
				return fmt.Errorf("field is required")
			}
			return nil
		},
		Message: "field is required",
	},
	"email": {
		Name: "email",
		Validate: func(value interface{}) error {
			if str, ok := value.(string); ok {
				pattern := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
				matched, _ := regexp.MatchString(pattern, str)
				if !matched {
					return fmt.Errorf("invalid email format")
				}
			}
			return nil
		},
		Message: "invalid email format",
	},
	"min": {
		Name: "min",
		Validate: func(value interface{}) error {
			v := reflect.ValueOf(value)
			min := v.Interface().(int)
			if min < 0 {
				return fmt.Errorf("value must be at least %d", min)
			}
			return nil
		},
		Message: "value is below minimum",
	},
	"max": {
		Name: "max",
		Validate: func(value interface{}) error {
			v := reflect.ValueOf(value)
			max := v.Interface().(int)
			if max < 0 {
				return fmt.Errorf("value must be at most %d", max)
			}
			return nil
		},
		Message: "value exceeds maximum",
	},
	"pattern": {
		Name: "pattern",
		Validate: func(value interface{}) error {
			if str, ok := value.(string); ok {
				pattern, _ := value.(string)
				matched, _ := regexp.MatchString(pattern, str)
				if !matched {
					return fmt.Errorf("value does not match pattern")
				}
			}
			return nil
		},
		Message: "value does not match pattern",
	},
}

// ValidationManager handles validation rules and execution
type ValidationManager struct {
	rules map[string][]ValidationRule
}

// NewValidationManager creates a new validation manager
func NewValidationManager() *ValidationManager {
	return &ValidationManager{
		rules: make(map[string][]ValidationRule),
	}
}

// AddRule adds a validation rule for a field
func (vm *ValidationManager) AddRule(field string, rule ValidationRule) {
	if vm.rules[field] == nil {
		vm.rules[field] = make([]ValidationRule, 0)
	}
	vm.rules[field] = append(vm.rules[field], rule)
}

// AddCustomRule adds a custom validation rule
func (vm *ValidationManager) AddCustomRule(field string, validate func(value interface{}) error, message string) {
	rule := ValidationRule{
		Name:     "custom",
		Validate: validate,
		Message:  message,
	}
	vm.AddRule(field, rule)
}

// ValidateField validates a single field
func (vm *ValidationManager) ValidateField(field string, value interface{}) error {
	rules, ok := vm.rules[field]
	if !ok {
		return nil
	}

	for _, rule := range rules {
		if err := rule.Validate(value); err != nil {
			return &ValidationError{
				Field:   field,
				Rule:    rule.Name,
				Message: rule.Message,
			}
		}
	}
	return nil
}

// ValidateStruct validates a struct using reflection
func (vm *ValidationManager) ValidateStruct(s interface{}) []error {
	var errors []error
	v := reflect.ValueOf(s)

	// If pointer get the underlying element
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return append(errors, fmt.Errorf("validation target must be a struct"))
	}

	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i).Interface()

		// Check for validation tags
		if tag := field.Tag.Get("validate"); tag != "" {
			rules := strings.Split(tag, ",")
			for _, rule := range rules {
				if validator, ok := CommonValidators[rule]; ok {
					if err := validator.Validate(value); err != nil {
						errors = append(errors, &ValidationError{
							Field:   field.Name,
							Rule:    rule,
							Message: err.Error(),
						})
					}
				}
			}
		}

		// Check for custom rules
		if fieldRules, ok := vm.rules[field.Name]; ok {
			for _, rule := range fieldRules {
				if err := rule.Validate(value); err != nil {
					errors = append(errors, &ValidationError{
						Field:   field.Name,
						Rule:    rule.Name,
						Message: err.Error(),
					})
				}
			}
		}
	}

	return errors
}

// CrossFieldValidation validates fields that depend on each other
func (vm *ValidationManager) CrossFieldValidation(s interface{}, rules map[string]func(fields map[string]interface{}) error) []error {
	var errors []error
	fields := make(map[string]interface{})

	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return append(errors, fmt.Errorf("validation target must be a struct"))
	}

	// Collect field values
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fields[field.Name] = v.Field(i).Interface()
	}

	// Apply cross-field validation rules
	for name, rule := range rules {
		if err := rule(fields); err != nil {
			errors = append(errors, &ValidationError{
				Field:   name,
				Rule:    "cross_field",
				Message: err.Error(),
			})
		}
	}

	return errors
}
