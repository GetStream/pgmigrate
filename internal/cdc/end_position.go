package cdc

import (
	"errors"
	"fmt"
	"io"
)

type EndPositionResolution struct {
	Requested LSN
	Boundary  LSN
	Exact     bool
}

type EndPositionUnavailableError struct {
	Requested LSN
	Durable   LSN
	Reason    string
}

func (e *EndPositionUnavailableError) Error() string {
	return fmt.Sprintf(
		"cdc: end position %x is not an available durable transaction boundary (durable %x): %s",
		e.Requested, e.Durable, e.Reason,
	)
}

// NormalizeEndPosition resolves requested to the newest durable KEEP or
// transaction EndLSN at or before it. Exact reports whether requested itself
// was a boundary, allowing applications to reject rather than normalize.
func NormalizeEndPosition(
	directory string,
	requested LSN,
	durable LSN,
) (EndPositionResolution, error) {
	resolution := EndPositionResolution{Requested: requested}
	if requested == 0 {
		return resolution, &EndPositionUnavailableError{
			Requested: requested,
			Durable:   durable,
			Reason:    "zero is not a transaction boundary",
		}
	}
	reader, err := NewReader(directory, 0, durable)
	if err != nil {
		return resolution, err
	}
	defer reader.Close()
	for {
		transaction, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return resolution, err
		}
		if transaction.EndLSN > requested {
			if err := transaction.CleanupSpill(); err != nil {
				return resolution, err
			}
			break
		}
		resolution.Boundary = transaction.EndLSN
		if transaction.EndLSN == requested {
			if err := transaction.CleanupSpill(); err != nil {
				return resolution, err
			}
			resolution.Exact = true
			return resolution, nil
		}
		if err := transaction.CleanupSpill(); err != nil {
			return resolution, err
		}
	}
	if resolution.Boundary == 0 {
		return resolution, &EndPositionUnavailableError{
			Requested: requested,
			Durable:   durable,
			Reason:    "no durable transaction ends at or before the requested position",
		}
	}
	return resolution, nil
}
