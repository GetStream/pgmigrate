#!/bin/sh
#
# Asserts that the replica-identity fallback is applied narrowly and undone fully.
#
# Narrowness is the whole assertion. REPLICA IDENTITY FULL makes every source
# UPDATE and DELETE write all old column values to WAL, on production, for the
# length of the migration. A rehearsal once set 110 relations to FULL because one
# of them needed it, and every check that existed at the time passed. So this does
# not ask whether the needy relations were altered; it asks whether the set of
# altered relations is *exactly* the needy ones.
#
# The needy set is the seed's three deliberate gaps: audit_log, whose only unique
# index is undesignated and therefore not a replica identity, and the two leaves of
# the identity-less partitioned metrics table. e2e.empty_strings is FULL in the
# seed itself, so it appears throughout and is not attributable to pgmigrate.

set -eu
. "$(dirname -- "$0")/common.sh"

mode=${1:-}

# fallback_relations lists the gaps pgmigrate is responsible for, so a change to
# the fixture shows up as a failure here rather than as silently weaker coverage.
needy="audit_log,metrics_eu,metrics_us"
seed_full="empty_strings"

full_relations() {
    "$1" -Atqc "
        SELECT coalesce(string_agg(c.relname, ',' ORDER BY c.relname), '')
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'e2e' AND c.relkind = 'r' AND c.relreplident = 'f'"
}

# keyed_and_full names relations that pgmigrate set to FULL despite having a
# primary key. Every one of those never needed it. The seed's own FULL table is
# excluded because it arrived that way and is not pgmigrate's doing.
keyed_and_full() {
    "$1" -Atqc "
        SELECT coalesce(string_agg(c.relname, ',' ORDER BY c.relname), '')
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'e2e' AND c.relkind = 'r' AND c.relreplident = 'f'
          AND c.relname <> '$seed_full'
          AND EXISTS (SELECT 1 FROM pg_index i
                       WHERE i.indrelid = c.oid AND i.indisprimary AND i.indisvalid)"
}

identity_of() {
    "$1" -Atqc "SELECT relreplident FROM pg_class WHERE oid = 'e2e.$2'::regclass"
}

case "$mode" in
applied)
    # Sorted, so the expected string is comparable to string_agg's output.
    want="audit_log,empty_strings,metrics_eu,metrics_us"
    got=$(full_relations source_sql)
    if [ "$got" != "$want" ]; then
        echo "relations at REPLICA IDENTITY FULL are '$got', want '$want'" >&2
        echo "the fallback must alter the relations that cannot identify rows ($needy)" >&2
        echo "plus the seed's own FULL table ($seed_full), and nothing else" >&2
        exit 1
    fi

    strays=$(keyed_and_full source_sql)
    if [ -n "$strays" ]; then
        echo "relations with a primary key were set to FULL anyway: $strays" >&2
        exit 1
    fi

    # A partitioned parent holds no rows and never appears in the replication
    # stream, so altering it would be pure cost with no effect. Its leaves are
    # what had to change, and they did, above.
    parent=$(identity_of source_sql metrics)
    if [ "$parent" != "d" ]; then
        echo "the partitioned parent e2e.metrics was altered (relreplident=$parent)" >&2
        exit 1
    fi

    echo "replica-identity-applied=$got"
    ;;
reverted)
    want="$seed_full"
    got=$(full_relations source_sql)
    if [ "$got" != "$want" ]; then
        echo "source relations at FULL after cutover are '$got', want only '$want'" >&2
        exit 1
    fi

    # The undesignated unique index has to come back undesignated: restoring the
    # mode but designating an index would leave the source subtly different from
    # how it was found.
    for relation in audit_log metrics_eu metrics_us; do
        identity=$(identity_of source_sql "$relation")
        if [ "$identity" != "d" ]; then
            echo "e2e.$relation was not restored (relreplident=$identity)" >&2
            exit 1
        fi
    done
    if [ "$(source_sql -Atqc "SELECT count(*) FROM pg_index WHERE indrelid = 'e2e.audit_log'::regclass AND indisreplident")" -ne 0 ]; then
        echo "e2e.audit_log came back with a designated replica identity index" >&2
        exit 1
    fi

    # The target must not have inherited the workaround. pg_dump reads the source
    # after the fallback is applied and writes the replica identity inside the
    # CREATE TABLE entry, so without an explicit restore the new production
    # database would carry FULL, and its WAL cost, forever.
    target_full=$(full_relations target_sql)
    if [ "$target_full" != "$want" ]; then
        echo "target relations at FULL are '$target_full', want only '$want'" >&2
        echo "the target inherited REPLICA IDENTITY FULL from the altered source" >&2
        exit 1
    fi

    echo "replica-identity-reverted=$got"
    ;;
*)
    echo "usage: assert-replica-identity.sh applied|reverted" >&2
    exit 1
    ;;
esac
