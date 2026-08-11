package cdc

import (
	"errors"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// isConnectionError deliberately recognizes only transport/connectivity
// failures. Protocol, decoding, segment, and deterministic SQL errors must not
// be hidden by an endless reconnect loop.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return strings.HasPrefix(pgError.Code, "08") ||
			pgError.Code == "57P01" || pgError.Code == "57P02" || pgError.Code == "57P03"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if pgconn.SafeToRetry(err) && strings.Contains(err.Error(), "conn closed") {
		return true
	}
	var operationError *net.OpError
	if errors.As(err, &operationError) {
		return true
	}
	var dnsError *net.DNSError
	return errors.As(err, &dnsError)
}

func classifyApplyError(relation *targetRelation, kind ChangeKind, err error) error {
	if err == nil || isConnectionError(err) {
		return err
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return divergenceFor(relation, kind, "SQLSTATE "+pgError.Code+": "+pgError.Message)
	}
	return err
}
