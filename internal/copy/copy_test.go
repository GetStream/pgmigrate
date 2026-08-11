package copy

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestQuoteIdentifier(t *testing.T) {
	got := QuoteIdentifier(`odd"schema`, "select")
	if got != `"odd""schema"."select"` {
		t.Fatalf("quoted identifier = %s", got)
	}
}

func TestPlanIntegerAndCTID(t *testing.T) {
	table := Table{OID: 1, Schema: "public", Name: "items", Bytes: 400, EstimatedRows: 100, IntegerKey: "odd key", KeyMin: -5, KeyMax: 94, HasKeyBounds: true}
	parts := Plan(table, 100, 3, Binary)
	if len(parts) != 3 || !strings.Contains(parts[0].Predicate, `"odd key" >= -5`) || parts[2].RangeEnd != "94" || !parts[2].EndInclusive {
		t.Fatalf("integer parts: %#v", parts)
	}
	table.IntegerKey = ""
	table.HasKeyBounds = false
	table.Bytes = 8192 * 10
	table.HeapBlocks = 10
	table.RelPages = 1
	parts = Plan(table, 8192*2, 4, Text)
	if len(parts) != 4 || !strings.HasPrefix(parts[0].ID, "ctid-") || parts[3].RangeEnd != "10" {
		t.Fatalf("ctid parts: %#v", parts)
	}
}

func TestPlanEmptyAndInt64Maximum(t *testing.T) {
	empty := Table{IntegerKey: "id", Empty: true, Bytes: 1000}
	parts := Plan(empty, 1, 8, Binary)
	if len(parts) != 1 || !parts[0].Unsplit {
		t.Fatalf("empty plan = %#v", parts)
	}
	table := Table{IntegerKey: "id", HasKeyBounds: true, KeyMin: math.MaxInt64 - 2, KeyMax: math.MaxInt64, Bytes: 100}
	parts = Plan(table, 1, 2, Binary)
	if len(parts) != 2 || !strings.Contains(parts[len(parts)-1].Predicate, "<= 9223372036854775807") {
		t.Fatalf("max-int plan = %#v", parts)
	}
}

func TestGeneratedColumnsExcludedFromCopy(t *testing.T) {
	columns := []Column{{Name: "id", TypeName: "int8", BuiltIn: true}, {Name: "doubled", TypeName: "int8", BuiltIn: true, Generated: true}}
	if got := columnList(columns); got != `"id"` {
		t.Fatalf("copy columns = %s", got)
	}
	if ConservativeFormat(Table{Columns: append(columns, Column{TypeName: "custom", Generated: true})}, 17, 17) != Binary {
		t.Fatal("generated type must not affect wire format")
	}
}

func TestRetryClassification(t *testing.T) {
	if !retryableConnectionError(&pgconn.PgError{Code: "08006"}) {
		t.Fatal("connection exception was not retryable")
	}
	if retryableConnectionError(&pgconn.PgError{Code: "22000"}) {
		t.Fatal("data exception was retryable")
	}
}

type retryError struct{}

func (retryError) Error() string     { return "connection lost" }
func (retryError) SafeToRetry() bool { return true }

func TestConnectionRetryStopsOnDataError(t *testing.T) {
	attempts := 0
	err := retryConnections(context.Background(), 5, func(int) time.Duration { return 0 }, func() error {
		attempts++
		if attempts < 3 {
			return retryError{}
		}
		return nil
	})
	if err != nil || attempts != 3 {
		t.Fatalf("retry err=%v attempts=%d", err, attempts)
	}
	attempts = 0
	dataErr := &pgconn.PgError{Code: "22000"}
	err = retryConnections(context.Background(), 5, func(int) time.Duration { return 0 }, func() error {
		attempts++
		return dataErr
	})
	if !errors.Is(err, dataErr) || attempts != 1 {
		t.Fatalf("data err=%v attempts=%d", err, attempts)
	}
}

func TestConservativeFormatAndSchedule(t *testing.T) {
	table := Table{Columns: []Column{{TypeName: "int8", BuiltIn: true}}}
	if ConservativeFormat(table, 17, 17) != Binary || ConservativeFormat(table, 17, 18) != Text {
		t.Fatal("unexpected major-version format")
	}
	table.Columns = append(table.Columns, Column{TypeName: "mytype"})
	if ConservativeFormat(table, 17, 17) != Text {
		t.Fatal("extension type must use text")
	}
	input := []Part{{ID: "small", EstimatedBytes: 1}, {ID: "large", EstimatedBytes: 10}, {ID: "medium", EstimatedBytes: 5}}
	got := LargestFirst(input)
	if got[0].ID != "large" || got[2].ID != "small" || input[0].ID != "small" {
		t.Fatalf("schedule = %#v", got)
	}
}
