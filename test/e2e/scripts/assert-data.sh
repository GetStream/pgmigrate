#!/bin/sh

set -eu
. "$(dirname -- "$0")/common.sh"

tables="seed_metadata customers accounts orders order_items traffic_metrics
        events events_2026_01 events_2026_02 events_2026_02_early
        events_2026_02_late events_overflow event_notes empty_strings
        audit_log metrics metrics_eu metrics_us"
failed=0

hash_query() {
    table=$1
    printf '%s\n' \
        "SET TimeZone='UTC'; SET DateStyle='ISO'; SET IntervalStyle='postgres'; SET extra_float_digits=3; SET bytea_output='hex';" \
        "SELECT count(*) || '|' || md5(coalesce(string_agg(md5(row_to_json(t)::text), '' ORDER BY md5(row_to_json(t)::text)), '')) FROM e2e.$table AS t;"
}

source_tables=$(source_sql -Atqc "SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_tables WHERE schemaname = 'e2e'")
target_tables=$(target_sql -Atqc "SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_tables WHERE schemaname = 'e2e'")

if [ "$source_tables" != "$target_tables" ]; then
    echo "table inventory mismatch" >&2
    echo "source=$source_tables" >&2
    echo "target=$target_tables" >&2
    exit 1
fi

for table in $tables; do
    source_result=$(hash_query "$table" | source_sql -Atq)
    target_result=$(hash_query "$table" | target_sql -Atq)
    if [ "$source_result" != "$target_result" ]; then
        echo "$table mismatch: source=$source_result target=$target_result" >&2
        failed=1
    else
        echo "$table count/hash=$source_result"
    fi
done

if [ "$failed" -ne 0 ]; then
    exit 1
fi

source_public=$(source_sql -Atqc "SELECT row_to_json(t)::text FROM public.restart_fixture t")
target_public=$(target_sql -Atqc "SELECT row_to_json(t)::text FROM public.restart_fixture t")
if [ "$source_public" != "$target_public" ]; then
    echo "public-schema restart fixture mismatch" >&2
    exit 1
fi
if [ "$(target_sql -Atqc "SELECT public.restart_label(1)")" != "restart-1" ]; then
    echo "public-schema function was not restored" >&2
    exit 1
fi

# The GIN index depends on a user-defined text-search configuration. That
# configuration's archive entry once aborted the schema phase, so assert both
# reached the target and that searching agrees with the source.
if [ "$(target_sql -Atqc "SELECT count(*) FROM pg_ts_config WHERE cfgname = 'order_search'")" -ne 1 ]; then
    echo "user-defined text-search configuration was not restored" >&2
    exit 1
fi
if [ "$(target_sql -Atqc "SELECT count(*) FROM pg_indexes WHERE schemaname = 'e2e' AND indexname = 'orders_note_search'")" -ne 1 ]; then
    echo "index depending on the text-search configuration was not rebuilt" >&2
    exit 1
fi
search_sql="SELECT count(*) FROM e2e.orders WHERE to_tsvector('e2e.order_search'::regconfig, coalesce(note, '')) @@ to_tsquery('e2e.order_search', 'seed')"
source_search=$(source_sql -Atqc "$search_sql")
target_search=$(target_sql -Atqc "$search_sql")
if [ "$source_search" != "$target_search" ] || [ "$source_search" -eq 0 ]; then
    echo "text-search results differ: source=$source_search target=$target_search" >&2
    exit 1
fi

# The extension lives in a schema the dump has to create. Restoring
# CREATE EXTENSION ... WITH SCHEMA e2e ahead of CREATE SCHEMA e2e aborted the
# schema phase, so assert the extension landed in its own schema.
extension_schema=$(target_sql -Atqc "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'unaccent'")
if [ "$extension_schema" != "e2e" ]; then
    echo "extension schema on target=$extension_schema, want e2e" >&2
    exit 1
fi

# Comments are restored one at a time by the deferred post-data phase, which
# resolves each object by the name and schema pg_dump records for it.
comment_sql="SELECT concat_ws('|', obj_description('e2e'::regnamespace, 'pg_namespace'), obj_description('e2e.orders'::regclass, 'pg_class'), col_description('e2e.orders'::regclass, (SELECT attnum FROM pg_attribute WHERE attrelid = 'e2e.orders'::regclass AND attname = 'note')), obj_description('public.restart_kind'::regtype, 'pg_type'))"
source_comments=$(source_sql -Atqc "$comment_sql")
target_comments=$(target_sql -Atqc "$comment_sql")
if [ "$source_comments" != "$target_comments" ]; then
    echo "comments differ: source=$source_comments target=$target_comments" >&2
    exit 1
fi

# The partition tree itself has to match. Rows in the parent alone prove
# nothing: a target holding one default partition would answer every parent
# query correctly while storing every row in the wrong place, and the per-leaf
# hashes above only mean something if the leaves are the same leaves.
tree_sql="SELECT string_agg(child.relkind::text || ' ' || parent.relname || '->' || child.relname
                           || ' ' || coalesce(pg_get_expr(child.relpartbound, child.oid), '-'),
                           ' | ' ORDER BY parent.relname, child.relname)
          FROM pg_inherits i
          JOIN pg_class child ON child.oid = i.inhrelid
          JOIN pg_class parent ON parent.oid = i.inhparent
          JOIN pg_namespace n ON n.oid = parent.relnamespace
         WHERE n.nspname = 'e2e'"
source_tree=$(source_sql -Atqc "$tree_sql")
target_tree=$(target_sql -Atqc "$tree_sql")
if [ "$source_tree" != "$target_tree" ]; then
    echo "partition tree mismatch" >&2
    echo "source=$source_tree" >&2
    echo "target=$target_tree" >&2
    exit 1
fi

# A partitioned index is created on the parent alone and stays invalid until
# every child index is attached to it. An invalid index is silently ignored by
# the planner, so nothing but the catalog reports it.
invalid=$(target_sql -Atqc "SELECT string_agg(c.relname, ',' ORDER BY c.relname) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'e2e' AND NOT i.indisvalid")
if [ -n "$invalid" ]; then
    echo "invalid indexes on the target: $invalid" >&2
    exit 1
fi

attached=$(target_sql -Atqc "SELECT count(*) FROM pg_inherits i JOIN pg_class parent ON parent.oid = i.inhparent WHERE parent.relname = 'events_kind_idx'")
if [ "$attached" -ne 3 ]; then
    echo "events_kind_idx has $attached attached partitions, want 3" >&2
    exit 1
fi

# Constraints on a partitioned parent cannot be attached to an existing index,
# so they are declared outright; and an unvalidated constraint enforces nothing
# for the rows already there.
unvalidated=$(target_sql -Atqc "SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = 'e2e' AND c.contype IN ('c','f') AND NOT c.convalidated")
if [ -n "$unvalidated" ]; then
    echo "unvalidated constraints on the target: $unvalidated" >&2
    exit 1
fi

# Both sides must be read on the same search_path: these functions render an
# object reference bare when its schema is on the reading session's path, and
# the source fixture deliberately runs on a non-default one.
definition_sql() {
    printf '%s\n' "SET search_path = '';" "$1"
}
constraint_sql="SELECT string_agg(c.conname || ':' || pg_catalog.pg_get_constraintdef(c.oid), ' | ' ORDER BY c.conname)
                FROM pg_catalog.pg_constraint c
               WHERE c.conrelid IN ('e2e.events'::regclass, 'e2e.event_notes'::regclass)"
source_constraints=$(definition_sql "$constraint_sql" | source_sql -Atq)
target_constraints=$(definition_sql "$constraint_sql" | target_sql -Atq)
if [ "$source_constraints" != "$target_constraints" ]; then
    echo "partitioned-table constraints differ" >&2
    echo "source=$source_constraints" >&2
    echo "target=$target_constraints" >&2
    exit 1
fi

index_sql="SELECT string_agg(pg_catalog.pg_get_indexdef(i.indexrelid), ' | ' ORDER BY c.relname)
           FROM pg_catalog.pg_index i
           JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = 'e2e'"
source_indexes=$(definition_sql "$index_sql" | source_sql -Atq)
target_indexes=$(definition_sql "$index_sql" | target_sql -Atq)
if [ "$source_indexes" != "$target_indexes" ]; then
    echo "index definitions differ" >&2
    echo "source=$source_indexes" >&2
    echo "target=$target_indexes" >&2
    exit 1
fi

# The parent must accept a routed insert. Before children were restored it took
# none, which is the failure a migrated application meets first.
if ! target_sql -Atqc "INSERT INTO e2e.events (id, customer_id, occurred_on, kind, payload) VALUES (999999999, 1, '2026-01-15', 'click', '{\"probe\": true}')" >/dev/null 2>&1; then
    echo "target partitioned table rejected a routed insert" >&2
    exit 1
fi
routed=$(target_sql -Atqc "SELECT c.relname FROM e2e.events e JOIN pg_class c ON c.oid = e.tableoid WHERE e.id = 999999999")
target_sql -Atqc "DELETE FROM e2e.events WHERE id = 999999999" >/dev/null
if [ "$routed" != "events_2026_01" ]; then
    echo "routed insert landed in $routed, want events_2026_01" >&2
    exit 1
fi

# An empty string must arrive as an empty string. The whole-table digest above
# already covers this, because row_to_json renders "" and null differently, but a
# digest mismatch says only that a table differs. Counting the two shapes per
# column names the defect instead, and does it for the nullable columns where
# writing NULL for '' raises no error at all.
empty_sql="SELECT count(*) FILTER (WHERE required = '')
                  || '|' || count(*) FILTER (WHERE optional = '')
                  || '|' || count(*) FILTER (WHERE optional IS NULL)
                  || '|' || count(*) FILTER (WHERE payload = ''::bytea)
                  || '|' || count(*) FILTER (WHERE payload IS NULL)
           FROM e2e.empty_strings"
source_empty=$(source_sql -Atqc "$empty_sql")
target_empty=$(target_sql -Atqc "$empty_sql")
if [ "$source_empty" != "$target_empty" ]; then
    echo "empty-string columns differ (required=''|optional=''|optional NULL|payload=''|payload NULL)" >&2
    echo "source=$source_empty" >&2
    echo "target=$target_empty" >&2
    exit 1
fi
if [ "$(target_sql -Atqc "SELECT count(*) FROM e2e.empty_strings WHERE required = ''")" -eq 0 ]; then
    echo "target holds no empty string at all, so this proves nothing" >&2
    exit 1
fi
echo "empty-strings=$target_empty"

echo "data-correctness=ok"
