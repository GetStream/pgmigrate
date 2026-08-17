GO ?= go
GOFLAGS ?=

.PHONY: fmt vet test race integration bench bench-cdc e2e crash-e2e

fmt:
	$(GO) $(GOFLAGS) fmt ./...

vet:
	$(GO) $(GOFLAGS) vet ./...

test:
	$(GO) $(GOFLAGS) test ./...

race:
	$(GO) $(GOFLAGS) test -race ./...

integration:
	$(GO) $(GOFLAGS) test -tags=integration ./...

bench:
	$(GO) $(GOFLAGS) test -run='^$$' -bench=. ./...

bench-cdc:
	$(GO) $(GOFLAGS) run ./cmd/pgmigrate-bench $(BENCH_FLAGS)

e2e:
	$(GO) $(GOFLAGS) build -o ./pgmigrate ./cmd/pgmigrate
	test/e2e/scripts/run-migration.sh

crash-e2e:
	$(GO) $(GOFLAGS) build -o ./pgmigrate ./cmd/pgmigrate
	test/e2e/scripts/run-crash-loop.sh
