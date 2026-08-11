#!/bin/sh

set -eu
. "$(dirname -- "$0")/common.sh"

source_sql -At <<'SQL'
DO $$
DECLARE
    setting_value text;
    missing_identity integer;
BEGIN
    SELECT current_setting('wal_level') INTO setting_value;
    IF setting_value <> 'logical' THEN
        RAISE EXCEPTION 'wal_level is %, expected logical', setting_value;
    END IF;

    IF current_setting('max_wal_senders')::integer < 2 THEN
        RAISE EXCEPTION 'max_wal_senders must be at least 2';
    END IF;

    IF current_setting('max_replication_slots')::integer < 2 THEN
        RAISE EXCEPTION 'max_replication_slots must be at least 2';
    END IF;

    -- The database sets a search_path that puts e2e ahead of public. Definitions
    -- the server renders over such a session come back unqualified where the
    -- default-path target renders them qualified, which is the difference that
    -- was once read as drift. Losing this setting retires that coverage.
    SELECT current_setting('search_path') INTO setting_value;
    IF setting_value NOT LIKE 'e2e,%' THEN
        RAISE EXCEPTION 'source search_path is %, expected the fixture to lead with e2e', setting_value;
    END IF;

    -- Every relation except the deliberate gaps below has to be identifiable, or
    -- the fixture stops distinguishing a tool that alters only what needs it from
    -- one that alters everything.
    SELECT count(*)
      INTO missing_identity
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'e2e'
       AND c.relkind = 'r'
       AND c.relreplident = 'd'
       AND c.relname NOT IN ('audit_log', 'metrics_eu', 'metrics_us')
       AND NOT EXISTS (
           SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary
       );

    IF missing_identity <> 0 THEN
        RAISE EXCEPTION '% e2e tables lack a primary key or replica identity', missing_identity;
    END IF;

    -- The gaps themselves, asserted positively: each must start out unable to
    -- identify its rows, because that is the state pgmigrate has to detect. A
    -- fixture that acquired a primary key would keep passing while covering
    -- nothing.
    IF EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'e2e'
           AND c.relname IN ('audit_log', 'metrics_eu', 'metrics_us')
           AND (c.relreplident <> 'd'
                OR EXISTS (SELECT 1 FROM pg_index i
                            WHERE i.indrelid = c.oid AND i.indisprimary))
    ) THEN
        RAISE EXCEPTION 'the replica-identity gap fixtures are no longer un-replicable';
    END IF;

    -- A unique index that nothing designated is not a replica identity. The
    -- detection rule originally proposed for this treated one as sufficient, so
    -- the index has to stay, and stay undesignated.
    IF NOT EXISTS (
        SELECT 1 FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
       WHERE i.indrelid = 'e2e.audit_log'::regclass
         AND i.indisunique AND NOT i.indisreplident
         AND c.relname = 'audit_log_entry_key'
    ) THEN
        RAISE EXCEPTION 'the undesignated unique index fixture is gone';
    END IF;

    -- The identity-less partitioned table is only interesting while its parent
    -- holds no rows of its own and both leaves take traffic: ALTER TABLE on the
    -- parent does not reach them, so each leaf has to be found individually.
    IF (SELECT count(*) FROM pg_partition_tree('e2e.metrics') WHERE isleaf) <> 2 THEN
        RAISE EXCEPTION 'the identity-less partitioned fixture no longer has two leaves';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM e2e.metrics_eu)
       OR NOT EXISTS (SELECT 1 FROM e2e.metrics_us) THEN
        RAISE EXCEPTION 'an identity-less partition holds no rows';
    END IF;

    -- Archive-entry descriptions that the TOC parser must recognize. Their
    -- absence would silently retire a regression fixture.
    IF NOT EXISTS (SELECT 1 FROM pg_ts_config WHERE cfgname = 'order_search') THEN
        RAISE EXCEPTION 'seed text-search configuration is missing';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_opclass WHERE opcname = 'text_ops_copy') THEN
        RAISE EXCEPTION 'seed operator class is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_depend d ON d.classid = 'pg_class'::regclass AND d.objid = i.indexrelid
        JOIN pg_ts_config t ON t.oid = d.refobjid
       WHERE c.relname = 'orders_note_search'
         AND d.refclassid = 'pg_ts_config'::regclass
    ) THEN
        RAISE EXCEPTION 'seed index does not depend on the text-search configuration';
    END IF;

    -- The extension must live outside public and be reachable from a selected
    -- index, or the fixture stops covering the restore order of
    -- CREATE EXTENSION ... WITH SCHEMA against its own schema.
    IF NOT EXISTS (
        SELECT 1 FROM pg_extension e
        JOIN pg_namespace n ON n.oid = e.extnamespace
       WHERE e.extname = 'unaccent' AND n.nspname = 'e2e'
    ) THEN
        RAISE EXCEPTION 'seed extension is not installed in the e2e schema';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_ts_config_map m
        JOIN pg_ts_dict d ON d.oid = m.mapdict
        JOIN pg_depend dep ON dep.classid = 'pg_ts_dict'::regclass AND dep.objid = d.oid
       WHERE m.mapcfg = 'e2e.order_search'::regconfig
         AND dep.refclassid = 'pg_extension'::regclass
    ) THEN
        RAISE EXCEPTION 'seed text-search configuration does not use the extension dictionary';
    END IF;

    -- The partitioned fixture: a parent whose primary key cannot be adopted
    -- from a bare index, a partitioned index that is invalid until its children
    -- attach, a sub-partitioned child that only a recursive walk finds, and rows
    -- in every leaf so a missing partition cannot pass as an empty one.
    IF (SELECT count(*) FROM pg_class WHERE relname = 'events' AND relkind = 'p') <> 1 THEN
        RAISE EXCEPTION 'seed partitioned table is missing';
    END IF;

    IF (SELECT count(*) FROM pg_partition_tree('e2e.events') WHERE level > 0) <> 5 THEN
        RAISE EXCEPTION 'seed partition count changed';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
         WHERE c.relname = 'events_2026_02' AND c.relkind = 'p' AND c.relispartition
    ) THEN
        RAISE EXCEPTION 'seed sub-partitioned child is missing';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_partition_tree('e2e.events') t
         WHERE t.isleaf
           AND NOT EXISTS (SELECT 1 FROM e2e.events e
                            WHERE e.tableoid = t.relid)
    ) THEN
        RAISE EXCEPTION 'a seed partition holds no rows';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'e2e.events'::regclass AND contype = 'p'
    ) THEN
        RAISE EXCEPTION 'seed partitioned table has no primary key';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE confrelid = 'e2e.events'::regclass AND contype = 'f'
    ) THEN
        RAISE EXCEPTION 'no seed foreign key references the partitioned table';
    END IF;

    -- An empty string in a replicated tuple was applied as NULL, and no fixture
    -- held one. All three shapes have to stay: a NOT NULL column defaulting to
    -- '' (loud), a nullable column holding both '' and NULL (silent, and only
    -- distinguishable by comparing values), and a zero-length bytea. Losing any
    -- of them retires the coverage without failing anything.
    IF NOT EXISTS (SELECT 1 FROM e2e.empty_strings WHERE required = '')
       OR NOT EXISTS (SELECT 1 FROM e2e.empty_strings WHERE optional = '')
       OR NOT EXISTS (SELECT 1 FROM e2e.empty_strings WHERE optional IS NULL)
       OR NOT EXISTS (SELECT 1 FROM e2e.empty_strings WHERE payload = ''::bytea)
       OR NOT EXISTS (SELECT 1 FROM e2e.empty_strings WHERE payload IS NULL) THEN
        RAISE EXCEPTION 'seed no longer holds every empty-string and NULL shape';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute a
         WHERE a.attrelid = 'e2e.empty_strings'::regclass
           AND a.attname = 'required' AND a.attnotnull
    ) THEN
        RAISE EXCEPTION 'seed empty-string column is no longer NOT NULL';
    END IF;

    -- Full replica identity is what puts an empty string into an UPDATE or
    -- DELETE predicate, where a nil binding matches a NULL row instead.
    IF (SELECT relreplident FROM pg_class WHERE oid = 'e2e.empty_strings'::regclass) <> 'f' THEN
        RAISE EXCEPTION 'seed empty-string table lost its full replica identity';
    END IF;

    -- Comments cover the deferred post-data path.
    IF obj_description('e2e.orders'::regclass, 'pg_class') IS NULL
       OR col_description('e2e.orders'::regclass,
              (SELECT attnum FROM pg_attribute
                WHERE attrelid = 'e2e.orders'::regclass AND attname = 'note')) IS NULL
       OR obj_description('e2e'::regnamespace, 'pg_namespace') IS NULL THEN
        RAISE EXCEPTION 'seed comments are missing';
    END IF;
END
$$;

SELECT 'source-health=ok';
SELECT 'server-version=' || current_setting('server_version');
SELECT 'wal-level=' || current_setting('wal_level');
SELECT 'seed-customers=' || count(*) FROM e2e.customers;
SELECT 'seed-orders-at-least=' || count(*) FROM e2e.orders;
SQL
