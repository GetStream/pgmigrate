package setup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestCopySourceFileHandoff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clone.dsn")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		dsn string
		err error
	}
	done := make(chan result, 1)
	go func() { dsn, err := waitCopySourceFile(ctx, path); done <- result{dsn, err} }()
	select {
	case got := <-done:
		t.Fatalf("missing file did not wait: %+v", got)
	case <-time.After(30 * time.Millisecond):
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		t.Fatalf("empty file did not wait: %+v", got)
	case <-time.After(30 * time.Millisecond):
	}
	const dsn = "postgres://reader:secret@clone.example/db"
	if err := os.WriteFile(path+".tmp", []byte(dsn+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || got.dsn != dsn {
		t.Fatalf("handoff failed: %v", got.err)
	}
}

func TestCopySourceFileErrors(t *testing.T) {
	t.Run("cancel missing file", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := waitCopySourceFile(ctx, filepath.Join(t.TempDir(), "missing"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("read failure", func(t *testing.T) {
		_, err := waitCopySourceFile(context.Background(), t.TempDir())
		if err == nil {
			t.Fatal("directory accepted")
		}
	})
	t.Run("invalid DSN redacted", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dsn")
		if err := os.WriteFile(path, []byte("postgres://reader:secret@clone:invalid/db"), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := waitCopySourceFile(context.Background(), path)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("invalid DSN must fail without disclosing credentials")
		}
	})
}
