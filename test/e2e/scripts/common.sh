#!/bin/sh

set -eu

E2E_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
COMPOSE_FILE="$E2E_DIR/compose.yaml"

compose() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

source_sql() {
    compose exec -T source psql -X -v ON_ERROR_STOP=1 -U app -d app "$@"
}

target_sql() {
    compose exec -T target psql -X -v ON_ERROR_STOP=1 -U app -d app "$@"
}
