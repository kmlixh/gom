package define

import "errors"

// Debug flag for enabling debug mode
var Debug bool

// ErrManualRollback is used to manually trigger a transaction rollback
var ErrManualRollback = errors.New("manual rollback")
