"""Promote a real physical controller standby with live pgwrh subscribers."""
from dataclasses import dataclass

import pytest

from .pgwrh_testkit import (
    MasterHandle, PgwrhCluster, ReplicaHandle, ReplicaSpec,
    query_scalar, quote_literal, wait_until,
)


SUBSCRIPTION = 'pgwrh_replica_subscription'
PHYSICAL_SLOT = 'controller_standby'


def wait_for_replay(standby, lsn):
    wait_until(lambda: query_scalar(standby,
        f"SELECT pg_last_wal_replay_lsn() >= {quote_literal(lsn)}::pg_lsn"),
        timeout=60, message=f'controller standby did not replay through {lsn}')


def slot_readiness(standby, slots):
    # A missing slot must produce false, rather than disappear from the result.
    expected = ', '.join(f'({quote_literal(slot)})' for slot in slots)
    return standby.execute(f"""
        WITH required(slot_name) AS (VALUES {expected})
        SELECT r.slot_name, COALESCE(s.synced AND NOT s.temporary
            AND s.invalidation_reason IS NULL, false)
        FROM required r LEFT JOIN pg_replication_slots s USING (slot_name)
        ORDER BY r.slot_name
    """)


@dataclass
class ControllerHA:
    cluster: PgwrhCluster
    primary: object
    standby: object

    def subscriptions(self):
        return [replica.execute(f"""SELECT oid, subslotname, subfailover, subconninfo
            FROM pg_subscription WHERE subname = '{SUBSCRIPTION}'""")[0]
            for replica in self.cluster.replicas]

    def wait_for_slots(self):
        slots = [row[1] for row in self.subscriptions()]
        assert slots and all(row[2] for row in self.subscriptions())
        wait_until(lambda: all(ready for _, ready in slot_readiness(self.standby, slots)),
                   timeout=60, message='logical slots did not become failover-ready')
        return slots


@pytest.fixture
def controller_ha(postgres_node_factory):
    primary = postgres_node_factory('controller_primary')
    master = MasterHandle(primary)
    master.execute('''
        CREATE ROLE test_replica;
        CREATE SCHEMA data AUTHORIZATION test_replica;
        CREATE TABLE data.root (id int, value text) PARTITION BY RANGE (id);
        CREATE TABLE data.p0 PARTITION OF data.root (PRIMARY KEY (id))
            FOR VALUES FROM (0) TO (100);
        ALTER TABLE data.p0 OWNER TO test_replica;
        INSERT INTO data.root SELECT n, 'row ' || n FROM generate_series(1, 10) n;
        SELECT pgwrh.create_replica_cluster('g1');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
        VALUES ('g1', 'data', 'root', 100);
    ''')
    # Use a real base backup and streaming connection, not copied catalog rows.
    with primary.replicate(name=PHYSICAL_SLOT, slot=PHYSICAL_SLOT,
                           options=['--checkpoint=fast']) as standby:
        user = query_scalar(primary, 'SELECT current_user')
        standby.append_conf(
            f"primary_conninfo = 'host=127.0.0.1 port={primary.port} "
            f"user={user} dbname=postgres application_name={PHYSICAL_SLOT}'\n"
            "hot_standby_feedback = on\n"
            "sync_replication_slots = on\n"
            f"unix_socket_directories = '{standby.base_dir}'")
        standby.start()
        primary.append_conf(f"synchronized_standby_slots = '{PHYSICAL_SLOT}'\n"
                            f"synchronous_standby_names = 'FIRST 1 ({PHYSICAL_SLOT})'\n"
                            "synchronous_commit = on")
        primary.reload()
        cluster = PgwrhCluster(master, postgres_node_factory)
        try:
            for name in ('ha_reader_a', 'ha_reader_b'):
                spec = ReplicaSpec(name)
                node = postgres_node_factory(name)
                username, password = master.create_replica_login(spec)
                master.execute(f"SELECT pgwrh.add_replica('g1', '{name}', '127.0.0.1', {node.port})")
                replica = ReplicaHandle(spec, node, username, password)
                # Put the reachable, read-only standby first. Both transports
                # must select the primary and later reconnect after promotion.
                node.execute("ALTER SERVER replica_controller OPTIONS (SET load_balance_hosts 'disable')")
                replica.configure_controller(host='127.0.0.1,127.0.0.1',
                    master_port=f'{standby.port},{primary.port}', start_daemon=False)
                node.execute('''CREATE FOREIGN TABLE public.controller_settings
                    (name text, setting text) SERVER replica_controller
                    OPTIONS (schema_name 'pg_catalog', table_name 'pg_settings')''')
                cluster.replicas.append(replica)
            yield ControllerHA(cluster, primary, standby)
        finally:
            # Stop logical subscribers before their publisher. Also release
            # synchronous waits if an assertion failed before promotion.
            for replica in cluster.replicas:
                replica.node.stop(['-m', 'immediate'])
            if primary.is_started:
                primary.append_conf("synchronous_standby_names = ''\nsynchronized_standby_slots = ''")
                primary.reload()


def deploy(ha):
    for replica in ha.cluster.replicas:
        replica.execute('SELECT pgwrh.start_sync_daemon(0.1)')
    ha.cluster.deploy(timeout=60)
    ha.wait_for_slots()


def test_controller_connections_skip_physical_standby(controller_ha):
    ha = controller_ha
    for replica in ha.cluster.replicas:
        assert replica.execute("SELECT setting FROM public.controller_settings WHERE name = 'port'") == [
            (str(ha.primary.port),)]
        assert replica.query_scalar("SELECT count(*) FROM pg_stat_subscription WHERE pid IS NOT NULL") == 1
    deploy(ha)
    ha.cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')


@pytest.mark.parametrize('shutdown', ['fast', 'immediate'], ids=['switchover', 'crash'])
def test_controller_promotion_preserves_subscriptions_and_resumes_rollouts(controller_ha, shutdown):
    ha = controller_ha
    deploy(ha)
    original = ha.subscriptions()
    # In the crash case leave one subscriber running so it must reconnect on
    # its own. The other has a known backlog, as can happen during an outage.
    paused = ha.cluster.replicas if shutdown == 'fast' else ha.cluster.replicas[:1]
    for replica in paused:
        replica.execute(f'ALTER SUBSCRIPTION {SUBSCRIPTION} DISABLE')
        wait_until(lambda: replica.query_scalar(
            'SELECT count(*) FROM pg_stat_subscription WHERE pid IS NOT NULL') == 0,
            message='subscriber did not stop before injecting a replication backlog')
    ha.primary.execute("""
        INSERT INTO data.root VALUES (11, 'committed before promotion');
        UPDATE data.root SET value = 'updated before promotion' WHERE id = 1;
        DELETE FROM data.root WHERE id = 2;
    """)
    expected = ha.primary.execute('SELECT * FROM data.root ORDER BY id')
    fence_lsn = query_scalar(ha.primary, 'SELECT pg_current_wal_flush_lsn()::text')
    wait_for_replay(ha.standby, fence_lsn)
    ha.wait_for_slots()
    # The old primary is fenced by stopping it and never restarting it.
    # immediate models a crash: there is no final shutdown checkpoint/slot sync.
    ha.primary.stop(['-m', shutdown])
    ha.standby.promote()
    ha.cluster.master.node = ha.standby
    assert ha.standby.execute('SELECT * FROM data.root ORDER BY id') == expected
    for replica in paused:
        replica.execute(f'ALTER SUBSCRIPTION {SUBSCRIPTION} ENABLE')
    assert ha.subscriptions() == original  # same OID, slot and multi-host conninfo
    wait_until(lambda: all(replica.execute('SELECT * FROM data.root ORDER BY id') == expected
                           for replica in ha.cluster.replicas),
               timeout=60, message='subscribers did not apply the pre-failover backlog')
    for replica in ha.cluster.replicas:
        assert replica.execute("SELECT setting FROM public.controller_settings WHERE name = 'port'") == [
            (str(ha.standby.port),)]
    ha.standby.execute("""
        INSERT INTO data.root VALUES (12, 'committed after promotion');
        UPDATE data.root SET value = 'updated after promotion' WHERE id = 3;
        DELETE FROM data.root WHERE id = 4;
    """)
    expected = ha.standby.execute('SELECT * FROM data.root ORDER BY id')
    wait_until(lambda: all(replica.execute('SELECT * FROM data.root ORDER BY id') == expected
                           for replica in ha.cluster.replicas),
               timeout=60, message='subscribers stopped receiving new-primary writes')
    # A new shard requires metadata reads, readiness writes and table copy on
    # the new primary, exercising more than an already-running WAL stream.
    ha.standby.execute('''
        CREATE TABLE data.p1 PARTITION OF data.root (PRIMARY KEY (id))
            FOR VALUES FROM (100) TO (200);
        ALTER TABLE data.p1 OWNER TO test_replica;
        INSERT INTO data.root VALUES (101, 'copied after promotion');
        INSERT INTO pgwrh.replication_group_config_clone
        SELECT replication_group_id, current_version, pgwrh.next_version(current_version)
        FROM pgwrh.replication_group WHERE replication_group_id = 'g1';
    ''')
    previous_version = ha.cluster.master.current_version()
    ha.cluster.deploy(timeout=60)
    assert ha.cluster.master.current_version() != previous_version
    wait_until(lambda: all(replica.query_scalar('SELECT count(*) FROM data.root') == 11
                           for replica in ha.cluster.replicas),
               timeout=60, message='new shard did not finish replication after promotion')
    ha.cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    assert ha.subscriptions() == original


def test_failover_readiness_rejects_missing_slots(controller_ha):
    ha = controller_ha
    deploy(ha)
    slots = ha.wait_for_slots()
    assert slot_readiness(ha.standby, slots + ['missing_required_slot']) == sorted(
        [(slot, True) for slot in slots] + [('missing_required_slot', False)])
