#!/usr/bin/env python3
"""Physical-clone -> COPY + original-primary -> CDC, using the actual CLI.

Requires Docker and a built PGMIGRATE_BIN. Client tools run in the matching image.
No mocked snapshots, LSNs, provider functions, or logical copies. A physical
backup is replayed and promoted, the upstream equivalent of snapshot restoration.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

BINARY = str(Path(os.environ.get("PGMIGRATE_BIN", "./pgmigrate")).resolve())
MAJOR = os.environ.get("PG_MAJOR", "17")
IMAGE = "postgres:" + MAJOR
PREFIX = "pgmigrate-clone-" + uuid.uuid4().hex[:10]
CONTAINERS = []
PROCESSES = []


def command(*args, **kwargs):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT,
                                       timeout=120, **kwargs).strip()
    except subprocess.CalledProcessError as error:
        raise RuntimeError(error.output) from error


def docker(*args):
    return command("docker", *args)


def sql(server, query):
    return docker("exec", server, "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                  "-U", "app", "-d", "app", "-c", query)


def wait(predicate, description):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError("timed out: " + description)


def start(name, *args):
    container = PREFIX + "-" + name
    CONTAINERS.append(container)
    docker("run", "-d", "--name", container, "--network", PREFIX,
           "-p", "127.0.0.1::5432", "-e", "POSTGRES_USER=app",
           "-e", "POSTGRES_PASSWORD=app", "-e", "POSTGRES_DB=app",
           IMAGE, *args)
    return container


def dsn(server):
    port = docker("port", server, "5432/tcp").split(":")[-1]
    return "postgres://app:app@127.0.0.1:" + port + "/app?sslmode=disable"


def ready(server):
    return subprocess.call(["docker", "exec", server, "pg_isready", "-h", "127.0.0.1", "-U", "app", "-d", "app"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0


def open_transaction(server, query):
    process = subprocess.Popen(["docker", "exec", "-i", server, "psql", "-XAt",
                                "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "app"],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True)
    PROCESSES.append(process)
    process.stdin.write("BEGIN;\n" + query + ";\nSELECT 'transaction-ready';\n")
    process.stdin.flush()
    while True:
        line = process.stdout.readline()
        if line.strip() == "transaction-ready":
            return process
        if not line or line.startswith("ERROR"):
            raise AssertionError("transaction did not start: " + line)


def finish_transaction(process, action):
    output, _ = process.communicate(action + ";\n", timeout=20)
    assert process.returncode == 0, output


def main(directory):
    source = start("source", "postgres", "-c", "wal_level=logical", "-c",
                   "max_replication_slots=10", "-c", "max_wal_senders=10")
    target = start("target")
    wait(lambda: ready(source) and ready(target), "source and target")
    sql(target, "ALTER SYSTEM SET synchronous_commit='off'")
    sql(target, "SELECT pg_reload_conf()")
    sql(source, """
        CREATE TABLE items(id integer PRIMARY KEY, value text NOT NULL);
        INSERT INTO items SELECT n, 'seed-' || n FROM generate_series(1,20000) n;
        CREATE TABLE events(id integer, value text) PARTITION BY RANGE(id);
        CREATE TABLE events_a PARTITION OF events FOR VALUES FROM (0) TO (100);
        CREATE TABLE events_b PARTITION OF events FOR VALUES FROM (100) TO (200);
        ALTER TABLE events ADD PRIMARY KEY(id);
        INSERT INTO events VALUES (1,'before'), (101,'before');
        CREATE TABLE keyless(value text);
        INSERT INTO keyless VALUES ('before');
        INSERT INTO keyless SELECT repeat('duplicate',32) FROM generate_series(1,2000);
        CREATE TABLE copy_blocker(value text);
        INSERT INTO copy_blocker VALUES ('blocked');
    """)
    # pg_basebackup connects as the source's superuser on this private test
    # network. The published clone port stays closed until recovery starts; pgmigrate
    # waits for promotion before accepting a clone-local snapshot.
    docker("exec", source, "sh", "-c",
           "printf '\nhost replication app all scram-sha-256\n' >> \"$PGDATA/pg_hba.conf\"")
    sql(source, "SELECT pg_reload_conf()")
    clone = start("clone", "bash", "-ceu", """
        until test -f /tmp/take-clone; do sleep .2; done
        export PGPASSWORD=app
        install -d -o postgres -g postgres -m 700 /var/lib/postgresql/clone
        gosu postgres pg_basebackup -h SOURCE -U app -D /var/lib/postgresql/clone -X stream -R -c fast
        gosu postgres pg_ctl -D /var/lib/postgresql/clone -l /tmp/recovery.log \
          -o "-c listen_addresses='*' -c hot_standby=on" -w start
        touch /tmp/standby-ready
        tail -f /dev/null
    """.replace("SOURCE", source))
    source_dsn, target_dsn, clone_dsn = dsn(source), dsn(target), dsn(clone)
    state = directory / "state"
    log = directory / "run.log"
    # Use the server major's real client tools, independent of host versions.
    # Newer pg_dump can emit settings unsupported by an older target.
    connections = {dsn(server): "postgres://app:app@" + server + ":5432/app?sslmode=disable"
                   for server in (source, target, clone)}
    for tool in ("pg_dump", "pg_restore"):
        wrapper = directory / tool
        wrapper.write_text("#!/usr/bin/env python3\nimport os, sys\n"
                           + "connections = " + repr(connections) + "\n"
                           + "args = [connections.get(arg, arg) for arg in sys.argv[1:]]\n"
                           + "os.execvp('docker', " + repr([
                               "docker", "run", "--rm", "--network", PREFIX,
                               "--user", str(os.getuid()) + ":" + str(os.getgid()),
                               "--volume", str(directory) + ":" + str(directory),
                               "--workdir", str(directory), "--env", "PGOPTIONS", IMAGE, tool])
                           + " + args)\n")
        wrapper.chmod(0o700)
    flags = ["--source", source_dsn, "--target", target_dsn, "--dir", str(state),
             "--pg-dump", str(directory / "pg_dump"), "--pg-restore", str(directory / "pg_restore")]
    copy_source_file = directory / "copy-source.dsn"
    with log.open("w") as output:
        run = subprocess.Popen([BINARY, "run", *flags, "--copy-source-file", str(copy_source_file),
                                "--ack-warnings", "--skip-target-tuning",
                                "--wal-sample-duration", "10ms", "--workers", "4",
                                "--split-threshold", "65536"], stdout=output, stderr=subprocess.STDOUT,
                                env={**os.environ, "PGMIGRATE_TEST_PAUSE_PHASE": "copy"})
        PROCESSES.append(run)
        def running_until(predicate):
            assert run.poll() is None, log.read_text()
            return predicate()
        wait(lambda: running_until(lambda: "Source slot ready" in log.read_text()), "slot ready")
        # These commits are later than the slot but MUST be present only once
        # in the copied data, not replayed again from the slot's original point.
        sql(source, """INSERT INTO items VALUES(30001,'before-clone');
            UPDATE items SET value='before-clone' WHERE id=1;
            DELETE FROM items WHERE id=2;
            INSERT INTO keyless VALUES('before-clone');""")
        spanning = open_transaction(source, """
            INSERT INTO items VALUES(30002,'spanning');
            UPDATE items SET value='spanning' WHERE id=3;
            DELETE FROM items WHERE id=4""")
        aborted = open_transaction(source, "INSERT INTO items VALUES(30003,'must-not-exist')")
        docker("exec", clone, "touch", "/tmp/take-clone")
        wait(lambda: subprocess.call(["docker", "exec", clone, "test", "-f", "/tmp/standby-ready"],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0,
             "real physical backup and recovery")
        # Include this commit in recovery before promoting. Exact equality
        # with Commit.EndLSN is tested by TestPGCloneSeedAtCommitEnd.
        sql(source, "INSERT INTO items VALUES(30004,'last-replayed')")
        end = sql(source, "SELECT pg_current_wal_flush_lsn()")
        wait(lambda: sql(clone, "SELECT pg_last_wal_replay_lsn() >= '" + end + "'::pg_lsn") == "t",
             "clone caught up")
        sql(clone, "SELECT pg_wal_replay_pause()")
        wait(lambda: sql(clone, "SELECT pg_get_wal_replay_pause_state()") == "paused", "replay paused")
        seed = sql(clone, "SELECT pg_last_wal_replay_lsn()")
        # Promotion aborts the still-open source transactions in the clone.
        # On the primary one commits and the other rolls back, after the seed.
        sql(clone, "SELECT pg_promote(true)")
        assert sql(clone, "SELECT pg_last_wal_replay_lsn()") == seed
        finish_transaction(spanning, "COMMIT")
        finish_transaction(aborted, "ROLLBACK")
        sql(source, """INSERT INTO items VALUES(30005,'after-clone');
            UPDATE items SET value='after-clone' WHERE id=5;
            DELETE FROM items WHERE id=6;
            UPDATE events SET value='after-clone' WHERE id=101;
            INSERT INTO keyless VALUES('after-clone');""")
        # The endpoint is supplied only AFTER promotion, while the original
        # pgmigrate process is still waiting. No placeholder DNS or restart.
        assert not copy_source_file.exists()
        assert json.loads(command(BINARY, "status", "--dir", str(state), "--json"))["phase"] == "setup"
        handoff = directory / "copy-source.tmp"
        handoff.write_text(clone_dsn + "\n")
        handoff.chmod(0o600)
        handoff.replace(copy_source_file)
        def following():
            return json.loads(command(BINARY, "status", "--dir", str(state), "--json"))["phase"] == "follow"
        wait(lambda: running_until(lambda: json.loads(command(BINARY, "status", "--dir", str(state), "--json"))["phase"] == "copy"), "COPY phase")
        sql(target, """CREATE FUNCTION assert_copy_durable() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF current_setting('synchronous_commit') <> 'on' THEN
                    RAISE EXCEPTION 'COPY commit must be durable';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER assert_copy_durable BEFORE INSERT ON copy_blocker
                FOR EACH ROW EXECUTE FUNCTION assert_copy_durable();
            ALTER TABLE copy_blocker ENABLE ALWAYS TRIGGER assert_copy_durable;""")
        blocked = open_transaction(target, "LOCK TABLE copy_blocker IN ACCESS EXCLUSIVE MODE")
        if MAJOR == "18":
            # The phase boundary is observable before any worker starts. A
            # crash here must still have a complete durable plan to resume.
            run.kill()
            run.wait(timeout=20)
            run = subprocess.Popen([BINARY, "run", *flags, "--copy-source-file", str(copy_source_file),
                                    "--ack-warnings", "--skip-target-tuning", "--workers", "4"],
                                   stdout=output, stderr=subprocess.STDOUT)
            PROCESSES.append(run)
        wait(lambda: running_until(lambda: sql(target, "SELECT to_regclass('pgmigrate_internal.copy_parts')") != ""), "COPY receipts table")
        wait(lambda: running_until(lambda: sql(target, "SELECT count(*) FROM pgmigrate_internal.copy_parts") not in ("0", "")), "at least one committed COPY chunk")
        receipts = sql(target, "SELECT table_oid,part_id,rows_copied,bytes_copied,xmin::text FROM pgmigrate_internal.copy_parts ORDER BY 1,2")
        snapshot_before = json.loads((state / "snapshot.json").read_text())
        wait(lambda: sql(source, "SELECT confirmed_flush_lsn > '" + seed + "'::pg_lsn FROM pg_replication_slots WHERE slot_name='" + snapshot_before["slot"] + "'") == "t", "source acknowledges durable CDC")
        run.kill()
        run.wait(timeout=20)
        # Failed resume must not clean the slot, target rows, or chunk receipts.
        bad = subprocess.run([BINARY, "run", *flags, "--copy-source", source_dsn,
                              "--ack-warnings", "--skip-target-tuning"],
                             stdout=output, stderr=subprocess.STDOUT, timeout=30)
        assert bad.returncode != 0, "accepted primary as clone"
        assert receipts == sql(target, "SELECT table_oid,part_id,rows_copied,bytes_copied,xmin::text FROM pgmigrate_internal.copy_parts ORDER BY 1,2")
        assert snapshot_before == json.loads((state / "snapshot.json").read_text())
        # Losing acknowledged local CDC must fail closed, even with a valid clone.
        (state / "cdc").rename(state / "cdc-saved")
        missing = subprocess.run([BINARY, "run", *flags, "--copy-source", clone_dsn,
                                  "--ack-warnings", "--skip-target-tuning"],
                                 stdout=output, stderr=subprocess.STDOUT, timeout=30)
        assert missing.returncode != 0, "accepted missing acknowledged CDC"
        assert "advanced beyond locally recovered CDC" in log.read_text(), log.read_text()
        import shutil
        shutil.rmtree(state / "cdc")
        (state / "cdc-saved").rename(state / "cdc")
        assert receipts == sql(target, "SELECT table_oid,part_id,rows_copied,bytes_copied,xmin::text FROM pgmigrate_internal.copy_parts ORDER BY 1,2")
        # Simulate a legacy async commit lost on the target while SQLite
        # retains completion: remove a committed range and its receipt atomically.
        import sqlite3
        with sqlite3.connect(state / "state.db") as db:
            plan = json.loads(db.execute("SELECT detail FROM steps WHERE name='copy.plan'").fetchone()[0])
        lost = next(p for p in plan if p["Table"]["Name"] == "items" and
                    sql(target, "SELECT count(*) FROM pgmigrate_internal.copy_parts WHERE table_oid=" + str(p["Table"]["OID"]) + " AND part_id='" + p["ID"] + "'") == "1")
        sql(target, "BEGIN; DELETE FROM items WHERE " + lost["Predicate"] +
            "; DELETE FROM pgmigrate_internal.copy_parts WHERE table_oid=" + str(lost["Table"]["OID"]) +
            " AND part_id='" + lost["ID"] + "'; COMMIT")
        receipts = sql(target, "SELECT table_oid,part_id,rows_copied,bytes_copied,xmin::text FROM pgmigrate_internal.copy_parts ORDER BY 1,2")
        if os.environ.get("PGMIGRATE_TEST_LEGACY_PLAN") == "1":
            import sqlite3
            with sqlite3.connect(state / "state.db") as db:
                db.execute("DELETE FROM steps WHERE name='copy.plan'")
        sql(source, "INSERT INTO items VALUES(30007,'during-copy-downtime')")
        previous_resumes = log.read_text().count("Resuming clone COPY")
        run = subprocess.Popen([BINARY, "run", *flags, "--copy-source-file", str(copy_source_file),
                                "--ack-warnings", "--skip-target-tuning", "--workers", "16"],
                               stdout=output, stderr=subprocess.STDOUT)
        PROCESSES.append(run)
        wait(lambda: running_until(lambda: log.read_text().count("Resuming clone COPY") > previous_resumes), "resumed COPY started")
        run.terminate()
        run.wait(timeout=20)
        finish_transaction(blocked, "ROLLBACK")
        run = subprocess.Popen([BINARY, "run", *flags, "--copy-source-file", str(copy_source_file),
                                "--ack-warnings", "--skip-target-tuning", "--workers", "16"],
                               stdout=output, stderr=subprocess.STDOUT)
        PROCESSES.append(run)
        wait(lambda: running_until(following), "resume COPY with 16 workers and catch up")
        assert "Resuming clone COPY" in log.read_text(), log.read_text()
        final_receipts = sql(target, "SELECT table_oid,part_id,rows_copied,bytes_copied,xmin::text FROM pgmigrate_internal.copy_parts ORDER BY 1,2")
        assert set(receipts.splitlines()) <= set(final_receipts.splitlines()), "committed COPY chunks were rewritten"
        assert snapshot_before == json.loads((state / "snapshot.json").read_text())

        snapshot = json.loads((state / "snapshot.json").read_text())
        assert snapshot["copy_source"] and snapshot["consistent_point"] == seed, snapshot
        assert snapshot["slot_consistent_point"] != seed, snapshot
        # Kill the whole clone: post-copy resume, live CDC and cutover must no
        # longer need its connection or exported snapshot.
        docker("stop", "-t", "2", clone)
        run.terminate()
        run.wait(timeout=20)
        resumed = subprocess.Popen([BINARY, "run", *flags, "--ack-warnings", "--skip-target-tuning",
                                    "--wal-sample-duration", "10ms"], stdout=output, stderr=subprocess.STDOUT)
        PROCESSES.append(resumed)
        sql(source, "UPDATE items SET value='after-resume' WHERE id=7; INSERT INTO items VALUES(30006,'after-resume')")
        def resumed_change():
            assert resumed.poll() is None, log.read_text()
            return sql(target, "SELECT count(*) FROM items WHERE id=30006") == "1"
        wait(resumed_change, "CDC after resume without clone")
        command(BINARY, "cutover", *flags)
        assert resumed.wait(timeout=30) == 0, log.read_text()
        for table in ("items", "events", "keyless", "copy_blocker"):
            query = "SELECT row_to_json(t)::text FROM " + table + " t ORDER BY row_to_json(t)::text"
            assert sql(source, query) == sql(target, query), "exact rows differ: " + table
        assert sql(source, "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgmigrate_slot_%'") == "0"
        assert sql(target, "SELECT count(*) FROM items WHERE id=30003") == "0"
        print("clone-migration-e2e=ok (PostgreSQL " + MAJOR + ", seed " + seed + ")")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix=PREFIX) as temp:
        directory = Path(temp)
        try:
            docker("network", "create", PREFIX)
            main(directory)
        except BaseException:
            for log in directory.glob("*.log"):
                print(log.read_text())
            raise
        finally:
            for process in PROCESSES:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            for container in reversed(CONTAINERS):
                subprocess.run(["docker", "rm", "-fv", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["docker", "network", "rm", PREFIX], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
