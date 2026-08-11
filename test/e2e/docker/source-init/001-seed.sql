\set ON_ERROR_STOP on

CREATE TYPE public.restart_kind AS ENUM ('fixed');
CREATE FUNCTION public.restart_label(integer) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT 'restart-' || $1::text $$;
CREATE TABLE public.restart_fixture (
    id integer PRIMARY KEY,
    kind public.restart_kind NOT NULL,
    label text NOT NULL
);
INSERT INTO public.restart_fixture VALUES (1, 'fixed', public.restart_label(1));

CREATE SCHEMA e2e;

-- Extensions live in a schema the dump has to create, which is the shape every
-- production source has and no earlier fixture had. pg_dump emits
-- `CREATE EXTENSION ... WITH SCHEMA e2e`, so the restore list has to keep
-- `CREATE SCHEMA e2e` ahead of it; hoisting extensions to the front of the list
-- made the statement unsatisfiable and aborted the schema phase. Nothing here
-- adds a column of an extension type, so the run still exercises binary COPY
-- and binary CDC.
CREATE EXTENSION unaccent WITH SCHEMA e2e;

CREATE TABLE e2e.seed_metadata (
    id integer PRIMARY KEY CHECK (id = 1),
    seed_name text NOT NULL,
    seeded_at timestamptz NOT NULL
);

INSERT INTO e2e.seed_metadata
VALUES (1, 'pgmigrate-e2e-v1', '2026-01-01 00:00:00+00');

CREATE TABLE e2e.customers (
    id bigint PRIMARY KEY,
    email text NOT NULL UNIQUE,
    display_name text NOT NULL,
    profile jsonb NOT NULL,
    lifetime_value numeric(14,2) NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL
);

INSERT INTO e2e.customers
SELECT id,
       format('customer-%s@example.test', id),
       format('Customer %s', id),
       jsonb_build_object('segment', CASE id % 3 WHEN 0 THEN 'enterprise' WHEN 1 THEN 'standard' ELSE 'trial' END,
                          'seed', md5(id::text)),
       (id * 101 % 10000)::numeric / 100,
       '2026-01-01 00:00:00+00'::timestamptz + id * interval '1 minute'
FROM generate_series(1, 100) AS id;

CREATE TABLE e2e.accounts (
    id bigint PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES e2e.customers(id),
    balance bigint NOT NULL,
    revision bigint NOT NULL DEFAULT 0,
    flags integer[] NOT NULL DEFAULT '{}'
);

INSERT INTO e2e.accounts
SELECT id, id, 100000 + id * 17, 0, ARRAY[(id % 5)::integer, (id % 7)::integer]
FROM generate_series(1, 100) AS id;

CREATE SEQUENCE e2e.order_id_seq START WITH 1001;

CREATE TABLE e2e.orders (
    id bigint PRIMARY KEY DEFAULT nextval('e2e.order_id_seq'),
    customer_id bigint NOT NULL REFERENCES e2e.customers(id),
    status text NOT NULL CHECK (status IN ('pending', 'paid', 'cancelled')),
    amount numeric(14,2) NOT NULL,
    note text,
    created_at timestamptz NOT NULL
);

ALTER SEQUENCE e2e.order_id_seq OWNED BY e2e.orders.id;

INSERT INTO e2e.orders (id, customer_id, status, amount, note, created_at)
SELECT id,
       ((id - 1) % 100) + 1,
       CASE id % 3 WHEN 0 THEN 'paid' WHEN 1 THEN 'pending' ELSE 'cancelled' END,
       ((id * 7919) % 250000)::numeric / 100,
       CASE WHEN id % 11 = 0 THEN NULL ELSE format('seed-order-%s', id) END,
       '2026-01-01 00:00:00+00'::timestamptz + id * interval '1 second'
FROM generate_series(1, 1000) AS id;

SELECT setval('e2e.order_id_seq', 1000, true);

CREATE TABLE e2e.order_items (
    order_id bigint NOT NULL REFERENCES e2e.orders(id) ON DELETE CASCADE,
    line_no smallint NOT NULL,
    sku text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    unit_price numeric(14,2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

INSERT INTO e2e.order_items
SELECT order_id,
       line_no,
       format('SKU-%04s', (order_id * 13 + line_no) % 997),
       ((order_id + line_no) % 5) + 1,
       ((order_id * 31 + line_no * 17) % 10000)::numeric / 100
FROM generate_series(1, 1000) AS order_id
CROSS JOIN generate_series(1, 3) AS line_no;

-- A user-defined text-search configuration reached by an index expression.
-- pg_dump gives it its own TOC entry, and one aborted the schema phase before
-- the TOC parser recognized TEXT SEARCH CONFIGURATION. The index makes the
-- configuration part of the dependency closure, so it must also restore before
-- indexes are built.
--
-- Mapping it onto the extension's dictionary is what pulls the extension, and
-- in turn the schema holding it, into that closure: index -> configuration ->
-- dictionary -> extension -> schema.
CREATE TEXT SEARCH CONFIGURATION e2e.order_search (COPY = simple);

ALTER TEXT SEARCH CONFIGURATION e2e.order_search
    ALTER MAPPING FOR hword, hword_part, word WITH e2e.unaccent, simple;

CREATE INDEX orders_note_search ON e2e.orders
    USING gin (to_tsvector('e2e.order_search'::regconfig, coalesce(note, '')));

-- OPERATOR is a word prefix of OPERATOR CLASS and OPERATOR FAMILY, which the
-- TOC parser previously mis-split without raising an error.
CREATE OPERATOR CLASS e2e.text_ops_copy
    FOR TYPE text USING btree AS
    OPERATOR 1 < (text, text),
    OPERATOR 2 <= (text, text),
    OPERATOR 3 = (text, text),
    OPERATOR 4 >= (text, text),
    OPERATOR 5 > (text, text),
    FUNCTION 1 bttextcmp(text, text);

-- Comments exercise the deferred post-data path. pg_dump writes them with a nil
-- catalog identity and names the object without its schema, so they were both
-- dropped from the restore list and unresolvable on the source.
COMMENT ON SCHEMA e2e IS 'end-to-end fixture schema';
COMMENT ON TABLE e2e.orders IS 'seeded and live orders';
COMMENT ON COLUMN e2e.orders.note IS 'free-form order note';
COMMENT ON TYPE public.restart_kind IS 'restart fixture kind';
COMMENT ON FUNCTION public.restart_label(integer) IS 'restart fixture label';

-- A partitioned table, two levels deep, with the four shapes that each broke a
-- different stage: a primary key on the parent (which cannot be adopted from a
-- bare index), a partitioned index (which stays invalid until every child index
-- is attached), a child-only index (which belongs to the child alone), and
-- foreign keys in both directions across the partition boundary. Children were
-- once dropped from the restore list, which left the target with a parent that
-- rejected every insert, so the partitions have to carry rows.
CREATE TABLE e2e.events (
    id bigint NOT NULL,
    customer_id bigint NOT NULL REFERENCES e2e.customers(id),
    occurred_on date NOT NULL,
    kind text NOT NULL CHECK (kind IN ('click', 'view', 'purchase')),
    payload jsonb NOT NULL,
    PRIMARY KEY (id, occurred_on)
) PARTITION BY RANGE (occurred_on);

CREATE TABLE e2e.events_2026_01 PARTITION OF e2e.events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

-- Sub-partitioned, so discovering children has to recurse rather than read one
-- level of pg_inherits. A unique constraint on a partitioned table has to
-- contain every partitioning column, so every level partitions on occurred_on.
CREATE TABLE e2e.events_2026_02 PARTITION OF e2e.events
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')
    PARTITION BY RANGE (occurred_on);
CREATE TABLE e2e.events_2026_02_early PARTITION OF e2e.events_2026_02
    FOR VALUES FROM ('2026-02-01') TO ('2026-02-15');
CREATE TABLE e2e.events_2026_02_late PARTITION OF e2e.events_2026_02
    FOR VALUES FROM ('2026-02-15') TO ('2026-03-01');

CREATE TABLE e2e.events_overflow PARTITION OF e2e.events DEFAULT;

CREATE INDEX events_kind_idx ON e2e.events (kind);
CREATE INDEX events_2026_01_payload_idx ON e2e.events_2026_01 USING gin (payload);

CREATE SEQUENCE e2e.event_id_seq START WITH 100000;

INSERT INTO e2e.events (id, customer_id, occurred_on, kind, payload)
SELECT id,
       ((id - 1) % 100) + 1,
       DATE '2026-01-01' + (id % 75),
       CASE id % 3 WHEN 0 THEN 'click' WHEN 1 THEN 'view' ELSE 'purchase' END,
       jsonb_build_object('seed', md5(id::text), 'n', id)
FROM generate_series(1, 300) AS id;

-- A foreign key that points at the partitioned parent, so the parent's primary
-- key has to exist and be valid on the target before this can be validated.
CREATE TABLE e2e.event_notes (
    id bigint PRIMARY KEY,
    event_id bigint NOT NULL,
    occurred_on date NOT NULL,
    note text NOT NULL,
    FOREIGN KEY (event_id, occurred_on) REFERENCES e2e.events (id, occurred_on)
);

INSERT INTO e2e.event_notes (id, event_id, occurred_on, note)
SELECT id, id, DATE '2026-01-01' + (id % 75), format('seed-event-note-%s', id)
FROM generate_series(1, 50) AS id;

-- An empty string is a present zero-length value; NULL is the absence of one.
-- pgoutput distinguishes them by a kind byte, and the apply path collapsed both
-- into a nil Go slice, which the bind protocol reads as NULL. Every earlier
-- fixture avoided the case entirely: no column here ever held '', and one
-- integration fixture even forbade it by CHECK. So the tool wrote NULL for every
-- replicated empty string, loudly where a column was NOT NULL and silently
-- everywhere else, and reached a 274 GB rehearsal before anyone noticed.
--
-- `required` is the loud shape (NOT NULL DEFAULT '', which is common in real
-- schemas), `optional` the silent one, where '' and NULL both occur and only a
-- value comparison can tell which arrived. REPLICA IDENTITY FULL puts the empty
-- strings into the UPDATE and DELETE predicates too, where a nil binding matches
-- a NULL row instead of an empty-string one.
CREATE TABLE e2e.empty_strings (
    id bigint PRIMARY KEY,
    required text NOT NULL DEFAULT '',
    optional text,
    payload bytea,
    label text NOT NULL
);

ALTER TABLE e2e.empty_strings REPLICA IDENTITY FULL;

INSERT INTO e2e.empty_strings (id, required, optional, payload, label)
SELECT id,
       CASE id % 3 WHEN 1 THEN format('required-%s', id) ELSE '' END,
       CASE id % 3 WHEN 0 THEN '' WHEN 1 THEN NULL ELSE format('optional-%s', id) END,
       CASE id % 3 WHEN 0 THEN ''::bytea WHEN 1 THEN NULL ELSE '\x00ff'::bytea END,
       format('seed-empty-%s', id)
FROM generate_series(1, 60) AS id;

-- A relation logical replication cannot identify rows in: no primary key, and a
-- unique index that nothing designated as the replica identity. PostgreSQL
-- rejects every UPDATE and DELETE on such a relation once it is published, so
-- pgmigrate sets REPLICA IDENTITY FULL on it for the run and puts it back after.
--
-- The undesignated unique index is the point of the fixture rather than
-- decoration. A unique index is not a replica identity until
-- REPLICA IDENTITY USING INDEX names it, and a detection rule that accepted one
-- as sufficient — the rule originally proposed for this — would skip this table
-- and the migration would break the source's writes.
CREATE TABLE e2e.audit_log (
    entry_id bigint NOT NULL,
    actor text NOT NULL,
    action text NOT NULL,
    recorded_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX audit_log_entry_key ON e2e.audit_log (entry_id);

INSERT INTO e2e.audit_log (entry_id, actor, action, recorded_at)
SELECT id,
       format('actor-%s', ((id - 1) % 100) + 1),
       CASE id % 3 WHEN 0 THEN 'create' WHEN 1 THEN 'update' ELSE 'delete' END,
       '2026-01-01 00:00:00+00'::timestamptz + id * interval '1 minute'
FROM generate_series(1, 40) AS id;

-- The same gap on a partitioned table, which is the shape the tool could not fix
-- at all. The publication names the parent, but with publish_via_partition_root
-- off the stream carries the leaves, and ALTER TABLE on a parent does not cascade
-- to its partitions in any released PostgreSQL. So the parent's own identity is
-- irrelevant and each leaf has to be found and altered individually.
CREATE TABLE e2e.metrics (
    sample_id bigint NOT NULL,
    region text NOT NULL,
    value numeric(12,4) NOT NULL,
    sampled_at timestamptz NOT NULL
) PARTITION BY LIST (region);

CREATE TABLE e2e.metrics_eu PARTITION OF e2e.metrics FOR VALUES IN ('eu');
CREATE TABLE e2e.metrics_us PARTITION OF e2e.metrics FOR VALUES IN ('us');

INSERT INTO e2e.metrics (sample_id, region, value, sampled_at)
SELECT id,
       CASE WHEN id % 2 = 0 THEN 'eu' ELSE 'us' END,
       ((id * 37) % 100000)::numeric / 100,
       '2026-01-01 00:00:00+00'::timestamptz + id * interval '1 minute'
FROM generate_series(1, 40) AS id;

CREATE TABLE e2e.traffic_metrics (
    id integer PRIMARY KEY CHECK (id = 1),
    steps bigint NOT NULL DEFAULT 0,
    inserts bigint NOT NULL DEFAULT 0,
    updates bigint NOT NULL DEFAULT 0,
    deletes bigint NOT NULL DEFAULT 0,
    last_step bigint NOT NULL DEFAULT 0,
    last_traffic_at timestamptz
);

INSERT INTO e2e.traffic_metrics (id) VALUES (1);

CREATE SEQUENCE e2e.traffic_step_seq;

CREATE FUNCTION e2e.run_traffic_step() RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    step_id bigint := nextval('e2e.traffic_step_seq');
    selected_customer bigint := ((step_id - 1) % 100) + 1;
    inserted_order bigint;
    inserted_event bigint;
    event_day date := DATE '2026-01-01' + (step_id % 75)::integer;
    moved_event bigint;
    moved_rows integer := 0;
    event_deletes integer := 0;
    empty_deletes integer := 0;
    audit_deletes integer := 0;
    metric_deletes integer := 0;
    deleted_rows integer := 0;
BEGIN
    INSERT INTO e2e.orders (customer_id, status, amount, note, created_at)
    VALUES (selected_customer,
            CASE WHEN step_id % 4 = 0 THEN 'cancelled' ELSE 'pending' END,
            ((step_id * 97) % 50000)::numeric / 100,
            format('traffic-step-%s', step_id),
            clock_timestamp())
    RETURNING id INTO inserted_order;

    INSERT INTO e2e.order_items (order_id, line_no, sku, quantity, unit_price)
    VALUES (inserted_order, 1, format('LIVE-%04s', step_id % 1000), (step_id % 5) + 1,
            ((step_id * 43) % 10000)::numeric / 100);

    UPDATE e2e.accounts
       SET balance = balance + CASE WHEN step_id % 2 = 0 THEN 7 ELSE -3 END,
           revision = revision + 1,
           flags = array_append(flags, (step_id % 11)::integer)
     WHERE id = selected_customer;

    UPDATE e2e.customers
       SET lifetime_value = lifetime_value + 0.01,
           updated_at = clock_timestamp()
     WHERE id = selected_customer;

    IF step_id % 3 = 0 THEN
        UPDATE e2e.orders
           SET status = 'paid', note = note || ':paid'
         WHERE id = inserted_order;
    END IF;

    -- Live writes to a partitioned table. The publication holds the parent, but
    -- with publish_via_partition_root off the stream names the child that took
    -- the row, so every one of these has to resolve to a partition on the
    -- target. The row that changes partition key arrives as a delete from one
    -- child and an insert into another.
    inserted_event := nextval('e2e.event_id_seq');
    INSERT INTO e2e.events (id, customer_id, occurred_on, kind, payload)
    VALUES (inserted_event, selected_customer, event_day,
            CASE step_id % 3 WHEN 0 THEN 'click' WHEN 1 THEN 'view' ELSE 'purchase' END,
            jsonb_build_object('step', step_id, 'at', clock_timestamp()));

    IF step_id % 4 = 0 THEN
        INSERT INTO e2e.event_notes (id, event_id, occurred_on, note)
        VALUES (inserted_event, inserted_event, event_day,
                format('traffic-event-note-%s', step_id));
    END IF;

    IF step_id % 2 = 0 THEN
        UPDATE e2e.events
           SET payload = payload || jsonb_build_object('touched', step_id)
         WHERE id = inserted_event AND occurred_on = event_day;
    END IF;

    IF step_id % 7 = 0 THEN
        SELECT e.id INTO moved_event
          FROM e2e.events e
         WHERE e.id >= 100000
           AND e.occurred_on < DATE '2026-02-01'
           AND NOT EXISTS (SELECT 1 FROM e2e.event_notes n WHERE n.event_id = e.id)
         ORDER BY e.id
         LIMIT 1;
        IF moved_event IS NOT NULL THEN
            UPDATE e2e.events
               SET occurred_on = occurred_on + 45
             WHERE id = moved_event;
            GET DIAGNOSTICS moved_rows = ROW_COUNT;
        END IF;
    END IF;

    -- Live empty strings, so they cross the CDC path and not only the base copy.
    -- The insert defaults `required` to '', the update moves `optional` between
    -- '' and NULL in both directions, and both statements match on a row whose
    -- replica identity contains an empty string.
    INSERT INTO e2e.empty_strings (id, optional, payload, label)
    VALUES (1000 + step_id,
            CASE WHEN step_id % 2 = 0 THEN '' ELSE NULL END,
            CASE WHEN step_id % 2 = 0 THEN ''::bytea ELSE NULL END,
            format('traffic-empty-%s', step_id));

    UPDATE e2e.empty_strings
       SET optional = CASE WHEN optional IS NULL THEN '' ELSE NULL END
     WHERE id = 1000 + step_id;

    IF step_id % 9 = 0 THEN
        DELETE FROM e2e.empty_strings
         WHERE id = (SELECT min(id) FROM e2e.empty_strings WHERE id > 1000);
        GET DIAGNOSTICS empty_deletes = ROW_COUNT;
    END IF;

    -- Traffic against the relations with no usable replica identity. These two
    -- statements are what a missed detection breaks: PostgreSQL refuses an UPDATE
    -- or DELETE on a published relation that cannot identify its rows, so the
    -- traffic generator would start failing outright rather than subtly.
    INSERT INTO e2e.audit_log (entry_id, actor, action, recorded_at)
    VALUES (1000 + step_id, format('actor-%s', selected_customer),
            CASE step_id % 3 WHEN 0 THEN 'create' WHEN 1 THEN 'update' ELSE 'delete' END,
            clock_timestamp());

    UPDATE e2e.audit_log
       SET action = action || ':confirmed'
     WHERE entry_id = 1000 + step_id;

    IF step_id % 8 = 0 THEN
        DELETE FROM e2e.audit_log
         WHERE entry_id = (SELECT min(entry_id) FROM e2e.audit_log WHERE entry_id > 1000);
        GET DIAGNOSTICS audit_deletes = ROW_COUNT;
    END IF;

    -- Alternating regions, so both leaves of the identity-less partitioned table
    -- take insert, update and delete traffic. One leaf altered and the other
    -- missed would pass every assertion that only looks at the parent.
    INSERT INTO e2e.metrics (sample_id, region, value, sampled_at)
    VALUES (1000 + step_id, CASE WHEN step_id % 2 = 0 THEN 'eu' ELSE 'us' END,
            ((step_id * 37) % 100000)::numeric / 100, clock_timestamp());

    UPDATE e2e.metrics
       SET value = value + 1
     WHERE sample_id = 1000 + step_id;

    IF step_id % 10 = 0 THEN
        DELETE FROM e2e.metrics
         WHERE sample_id = (SELECT min(sample_id) FROM e2e.metrics WHERE sample_id > 1000);
        GET DIAGNOSTICS metric_deletes = ROW_COUNT;
    END IF;

    IF step_id % 5 = 0 THEN
        WITH victim AS (
            SELECT id
              FROM e2e.orders
             WHERE status = 'cancelled' AND id <> inserted_order
             ORDER BY id
             LIMIT 1
             FOR UPDATE SKIP LOCKED
        )
        DELETE FROM e2e.orders o USING victim v WHERE o.id = v.id;
        GET DIAGNOSTICS deleted_rows = ROW_COUNT;
    END IF;

    IF step_id % 6 = 0 THEN
        DELETE FROM e2e.events
         WHERE (id, occurred_on) = (
                   SELECT e.id, e.occurred_on
                     FROM e2e.events e
                    WHERE e.id >= 100000
                      AND NOT EXISTS (
                          SELECT 1 FROM e2e.event_notes n WHERE n.event_id = e.id)
                    ORDER BY e.id
                    LIMIT 1);
        GET DIAGNOSTICS event_deletes = ROW_COUNT;
    END IF;

    UPDATE e2e.traffic_metrics
       SET steps = steps + 1,
           inserts = inserts + 6 + CASE WHEN step_id % 4 = 0 THEN 1 ELSE 0 END,
           updates = updates + 5 + moved_rows
                     + CASE WHEN step_id % 3 = 0 THEN 1 ELSE 0 END
                     + CASE WHEN step_id % 2 = 0 THEN 1 ELSE 0 END,
           deletes = deletes + deleted_rows + event_deletes + empty_deletes
                     + audit_deletes + metric_deletes,
           last_step = step_id,
           last_traffic_at = clock_timestamp()
     WHERE id = 1;
END
$$;

-- The source answers on a non-default search_path, which is what most
-- production databases do and what no earlier fixture did. pg_get_indexdef and
-- its siblings render an object reference bare when its schema is on the
-- reading session's path and qualified when it is not, so the same index read
-- from this source and from the default-path target came back as two different
-- strings and was reported as a collision. The GIN index above names
-- e2e.order_search, whose rendering flips exactly here. Every connection that
-- renders or compares a definition must pin its own path instead of inheriting
-- this one.
--
-- It also means nothing pgmigrate runs against the source may depend on the
-- default path: an unqualified reference resolves differently here.
ALTER DATABASE app SET search_path TO e2e, public;
