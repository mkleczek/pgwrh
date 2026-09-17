"""Restore a live controller backup and rebuild service using fresh replicas."""
from contextlib import ExitStack
from pathlib import Path

import pytest

from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaHandle, quote_ident, quote_literal, wait_until
from .test_backup_restore import run, snapshot
from .test_credential_protocol import generation_states, rotate, wait_rotation
from .test_local_first_handoff import handoff_cluster, move_to_destination, wait_prepared

ROOT = Path(__file__).resolve().parents[2]
QUERY = 'SELECT id, value FROM data.root ORDER BY id'


@pytest.mark.parametrize('phase', ['committed', 'in_flight', 'credentials_preparing', 'credentials_switching'])
def test_logical_restore_rebuilds_a_working_cluster(handoff_cluster, postgres_node_factory, tmp_path, phase):
    original = handoff_cluster
    source, destination, reader = original.replicas
    with ExitStack() as pauses:
        def pause(replica):
            conn = pauses.enter_context(replica.node.connect())
            conn.execute('SELECT pg_advisory_lock(2895359559)')
            return conn

        if phase == 'in_flight':
            pause(reader)
            move_to_destination(original)
            wait_prepared(original)
        elif phase.startswith('credentials_'):
            delayed = pause(destination)
            pending = rotate(original)
            if phase == 'credentials_switching':
                wait_until(lambda: original.master.query_scalar(f'''SELECT count(*)
                    FROM pgwrh.missing_credential_installation WHERE generation = {quote_literal(pending)}
                    AND member_role <> 'destination' ''') == 0,
                    timeout=60, message='pending credentials were not installed')
                pause(reader)
                delayed.execute('SELECT pg_advisory_unlock(2895359559)')
                wait_until(lambda: generation_states(original).get(pending) == 'active',
                           timeout=60, message='rotation did not reach the switching phase')

        # Freeze every reporter before taking the roles and database backups.
        # Re-acquiring a session advisory lock is safe; closing it releases all.
        # Use distinct sessions only for replicas not already held above.
        held = ({'reader'} if phase == 'in_flight' else
                {'destination', 'reader'} if phase == 'credentials_switching' else
                {'destination'} if phase == 'credentials_preparing' else set())
        for replica in original.replicas:
            if replica.name not in held:
                pause(replica)
        if phase == 'credentials_switching':
            delayed.execute('SELECT pg_advisory_lock(2895359559)')
        before = snapshot(original.master.node)
        expected = original.master.execute(QUERY)
        archive = tmp_path / 'controller.dump'
        run(original.master.node, 'pg_dump', '-d', 'postgres', '-Fc', '-f', str(archive))
        roles = run(original.master.node, 'pg_dumpall', '--roles-only', '--no-role-passwords')
        bootstrap = original.master.query_scalar('SELECT quote_ident(current_user)')
        roles_file = tmp_path / 'roles.sql'
        roles_file.write_text(roles.replace(f'CREATE ROLE {bootstrap};', ''))
        # Fence the old databases while their reporters are still paused. There
        # will never be two live controllers or reuse of an old replica directory.
        for replica in original.replicas:
            replica.node.stop(['-m', 'immediate'])
        original.master.node.stop(['-m', 'immediate'])

    restored = postgres_node_factory('restored_controller', install_extension=False)
    run(restored, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-d', 'postgres', '-f', str(roles_file))
    for section in ('pre-data', 'data', 'post-data'):
        flags = ('--data-only', '--disable-triggers') if section == 'data' else (
            f'--section={section}', '--no-publications', '--no-subscriptions')
        run(restored, 'pg_restore', '-d', 'postgres', '--exit-on-error', '--single-transaction',
            *flags, str(archive))
    assert snapshot(restored) == before
    assert restored.execute(QUERY) == expected
    restored.execute('SELECT pgwrh.sync_publications()')
    restored.execute((ROOT / 'docs/recovery-quarantine.sql').read_text())
    assert restored.execute('''SELECT count(*) FROM pgwrh.replication_group_member
        WHERE credential_generation IS NOT NULL OR users::jsonb <> '[]'::jsonb''') == [(0,)]
    assert restored.execute('SELECT count(*) FROM pg_replication_slots') == [(0,)]
    if phase == 'in_flight':
        with pytest.raises(Exception, match='required'):
            restored.execute("SELECT pgwrh.commit_rollout('g1')")

    rebuilt = PgwrhCluster(MasterHandle(restored), postgres_node_factory)
    for old in original.replicas:
        node = postgres_node_factory('rebuilt_' + old.name)
        replica = ReplicaHandle(old.spec, node, old.username, old.password)
        rebuilt.replicas.append(replica)
        restored.execute(f'ALTER ROLE {quote_ident(old.username)} PASSWORD {quote_literal(old.password)}')
        # Updating endpoints is configuration, not editing readiness reports.
        restored.execute(f'''UPDATE pgwrh.shard_host SET port = {node.port}
            WHERE host_id = {quote_literal(old.name)}''')
    restored.execute('UPDATE pgwrh.shard_host SET online = true')
    for replica in rebuilt.replicas:
        replica.configure_controller(master_port=restored.port)
    rebuilt.master.wait_for_rollout_ready(expected_replicas=3, timeout=90)
    if phase == 'in_flight':
        rebuilt.master.commit_rollout()
    wait_until(lambda: all(r.execute(QUERY) == expected for r in rebuilt.replicas),
               timeout=60, message='rebuilt replicas did not serve the restored controller data')
    if phase.startswith('credentials_'):
        wait_rotation(rebuilt, pending)

    # Exercise new INSERT/UPDATE/DELETE after recovery, then another complete
    # placement/credential lifecycle. Old catalog equality alone is insufficient.
    restored.execute("""INSERT INTO data.root VALUES (33, 'after recovery');
        UPDATE data.root SET value = 'updated after recovery' WHERE id = 1;
        DELETE FROM data.root WHERE id = 2;""")
    expected = restored.execute(QUERY)
    wait_until(lambda: all(r.execute(QUERY) == expected for r in rebuilt.replicas),
               timeout=60, message='new writes did not replicate after recovery')
    target = 'source' if phase == 'in_flight' else 'destination'
    rebuilt.master.execute(f"""SELECT pgwrh.set_replica_weight('g1', 'default', '{target}', 100);
        DELETE FROM pgwrh.shard_host_weight WHERE host_id <> '{target}' AND version <>
            (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1');""")
    rebuilt.deploy(timeout=60)
    wait_rotation(rebuilt, rotate(rebuilt))
    rebuilt.assert_query_results_match(QUERY)
