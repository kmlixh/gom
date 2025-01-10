package define

import (
	"testing"
)

func TestDebugFlag(t *testing.T) {
	// Save original value
	originalDebug := Debug
	defer func() {
		Debug = originalDebug
	}()

	// Test setting Debug flag
	Debug = true
	if !Debug {
		t.Error("Debug flag should be true")
	}

	Debug = false
	if Debug {
		t.Error("Debug flag should be false")
	}
}

func TestErrManualRollback(t *testing.T) {
	// Test error message
	if ErrManualRollback.Error() != "manual rollback" {
		t.Errorf("ErrManualRollback message = %v, want %v", ErrManualRollback.Error(), "manual rollback")
	}

	// Test error comparison
	if ErrManualRollback != ErrManualRollback {
		t.Error("ErrManualRollback should equal itself")
	}
}
