package pg

import (
	"context"
	"time"
)

// TimeoutOverrideForTesting SHOULD ONLY be used for testing.
//
// If you feel the need to extend the timeout of your transactions, start asking questions about your designs first,
// and discuss comprehensively with the team to ensure you have already exhausted all other options. Long-running
// transactions are bad for database because it hogs resources and prevents vacuuming.
func TimeoutOverrideForTesting(parentCtx context.Context, dur time.Duration) context.Context {
	return context.WithValue(parentCtx, timeoutKey, dur)
}
