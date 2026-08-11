package postgres

import (
	"context"
	"fmt"
)

// PinSearchPath fixes a connection's search_path to the empty path, so that
// every definition the server renders over it is fully schema-qualified and
// every definition executed over it resolves the same way.
//
// pg_get_indexdef, pg_get_constraintdef, pg_get_triggerdef and their siblings
// render an object reference bare when its schema is on the reading session's
// search_path and qualified when it is not. Two sessions with different paths
// therefore report one object with different text, which made identical objects
// on the source and the target compare unequal. Pinning both sides removes the
// difference; it is also what pg_dump does, for the same reason.
//
// pg_catalog remains implicitly searched, so catalog queries still resolve.
func PinSearchPath(ctx context.Context, db ProgressExecer) error {
	if _, err := db.Exec(ctx, "SELECT pg_catalog.set_config('search_path','',false)"); err != nil {
		return fmt.Errorf("pin search_path: %w", err)
	}
	return nil
}
