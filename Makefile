GO ?= go
GOFLAGS ?=

.PHONY: fmt vet test race integration bench cdc-bench e2e crash-e2e

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

cdc-bench:
	PGMIGRATE_CDC_BENCHMARK=1 $(GO) $(GOFLAGS) test -tags=integration ./internal/cdc \
		-run='^TestPG17CDCReplayThroughput$$' -count=1 -v

e2e:
	$(GO) $(GOFLAGS) build -o ./pgmigrate ./cmd/pgmigrate
	test/e2e/scripts/run-migration.sh

crash-e2e:
	$(GO) $(GOFLAGS) build -o ./pgmigrate ./cmd/pgmigrate
	test/e2e/scripts/run-crash-loop.sh
