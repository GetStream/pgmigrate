package setup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestValidateCloneLSN(t *testing.T) {
	for _, test := range []struct {
		name, slot, seed, current string
		valid                     bool
	}{
		{"inside", "0/10", "0/20", "0/30", true},
		{"at slot", "0/10", "0/10", "0/30", true},
		{"at flushed end", "0/10", "0/30", "0/30", true},
		{"stale clone", "0/20", "0/10", "0/30", false},
		{"foreign timeline", "0/10", "0/40", "0/30", false},
		{"zero", "0/10", "0/0", "0/30", false},
		{"malformed", "0/10", "not an LSN", "0/30", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateCloneLSN(test.slot, test.seed, test.current); (err == nil) != test.valid {
				t.Fatalf("validateCloneLSN = %v, valid = %v", err, test.valid)
			}
		})
	}
}

func TestRetryCloneConnection(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"DNS not created", &net.DNSError{IsNotFound: true}, true},
		{"connection refused", &net.OpError{Op: "dial", Err: errors.New("refused")}, true},
		{"startup", &pgconn.PgError{Code: "57P03"}, true},
		{"connect timeout", context.DeadlineExceeded, true},
		{"startup disconnect", io.EOF, true},
		{"partial startup response", io.ErrUnexpectedEOF, true},
		{"authentication", &pgconn.PgError{Code: "28P01"}, false},
		{"wrong database", &pgconn.PgError{Code: "3D000"}, false},
		{"TLS configuration", errors.New("server refused TLS connection"), false},
		{"cancelled", context.Canceled, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := retryCloneConnection(fmt.Errorf("connect: %w", test.err)); got != test.retry {
				t.Fatalf("retry = %v, want %v", got, test.retry)
			}
		})
	}
}
