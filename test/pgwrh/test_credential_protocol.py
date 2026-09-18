from pathlib import Path

import pytest

from .pgwrh_testkit import quote_ident, quote_literal, wait_until
from .test_local_first_handoff import handoff_cluster, move_to_destination


# Test-only provider: production continues to expand shared group credentials.
DISTINCT_CREDENTIALS = """
CREATE OR REPLACE VIEW pgwrh.replica_credentials AS
SELECT m.replication_group_id, m.member_role, c.version,
       'pgwrh_test_' || md5(c.username || ':' || m.member_role) AS username,
       c.password || ':' || m.member_role AS password
FROM pgwrh.replication_group_member m
JOIN pgwrh.replication_group_credentials c USING (replication_group_id)
"""


def test_credential_feeds_are_scoped_to_member_and_group(postgres_node_factory):
    node = postgres_node_factory('credential_feeds')
    node.execute("""
        SELECT pgwrh.create_replica_cluster('one');
        SELECT pgwrh.create_replica_cluster('two');
        CREATE ROLE a; CREATE ROLE b; CREATE ROLE c; CREATE ROLE outsider;
        SELECT pgwrh.add_replica('one', 'a', 'a', 5432);
        SELECT pgwrh.add_replica('one', 'b', 'b', 5432);
        SELECT pgwrh.add_replica('two', 'c', 'c', 5432);
    """)
    # Expanding the destination dimension must not change production generation.
    assert node.execute("""SELECT count(*), count(DISTINCT (username, password))
        FROM pgwrh.replica_credentials WHERE replication_group_id = 'one'""") == [(2, 1)]
    original = node.execute('SELECT * FROM pgwrh.replication_group_credentials ORDER BY 1, 2')
    node.execute(DISTINCT_CREDENTIALS)
    assert node.execute('SELECT * FROM pgwrh.replication_group_credentials ORDER BY 1, 2') == original
    credentials = {member: (username, password) for member, username, password in node.execute(
        'SELECT member_role, username, password FROM pgwrh.replica_credentials')}
    for member, peers in [('a', ['b']), ('b', ['a']), ('c', []), ('outsider', [])]:
        with node.connect() as conn:
            conn.execute(f'SET ROLE {quote_ident(member)}')
            assert conn.execute('SELECT * FROM pgwrh.local_credentials') == (
                [credentials[member]] if member in credentials else [])
            assert conn.execute('SELECT * FROM pgwrh.remote_credentials ORDER BY member_role') == [
                (peer, *credentials[peer]) for peer in peers]


def require_generated_passwords(node):
    # Set this before the replica daemon starts, so no FDW connection can have
    # authenticated under testgres's default trust rules. Control users retain
    # trust; only the test provider's generated logins require SCRAM.
    hba = Path(node.data_dir) / 'pg_hba.conf'
    rule = 'host all /^pgwrh_test_ all scram-sha-256\n'
    if not hba.read_text().startswith(rule):
        hba.write_text(rule + hba.read_text())
        node.reload()


def credentials_for(cluster, version):
    return {member: (username, password) for member, username, password in cluster.master.execute(
        f'''SELECT member_role, username, password FROM pgwrh.replica_credentials
            WHERE version = {quote_literal(version)}''')}


def installed_users(replica):
    return {name for name, in replica.execute('''SELECT u.rolname
        FROM pg_roles u JOIN pg_auth_members a ON a.member = u.oid
        JOIN pg_roles g ON g.oid = a.roleid
        WHERE g.rolname = pgwrh.pgwrh_replica_role_name()''')}


def wait_for_local_credentials(cluster, credentials):
    # Check local membership, not role existence: sibling databases share the
    # PostgreSQL role catalog but must grant access only to their own logins.
    wait_until(lambda: all(installed_users(replica) == {credentials[replica.username][0]}
                           for replica in cluster.replicas), timeout=60,
               message='local credential installation or retirement did not finish')


@pytest.mark.parametrize('handoff_cluster', [
    dict(source='source_db', destination='destination_db', reader='reader_db', shared=shared,
         credentials_sql=DISTINCT_CREDENTIALS, replica_setup=require_generated_passwords)
    for shared in (False, True)
], indirect=True, ids=['separate-servers', 'shared-server'])
def test_distinct_credentials_authenticate_rotate_and_roll_back(handoff_cluster):
    cluster = handoff_cluster
    source, destination, reader = cluster.replicas
    original = credentials_for(cluster, cluster.master.current_version())
    assert len({username for username, _ in original.values()}) == 3
    assert len({password for _, password in original.values()}) == 3
    wait_for_local_credentials(cluster, original)
    for replica in cluster.replicas:
        username, password = original[replica.username]
        with replica.node.connect(username=username, password=password) as authenticated:
            assert authenticated.execute('SELECT current_user') == [(username,)]
        with pytest.raises(Exception, match='password authentication failed'):
            replica.node.connect(username=username, password='incorrect')

    wait_until(lambda: reader.query_scalar('''SELECT count(*) FROM pgwrh.remote_node n
        JOIN pgwrh.reachable_shard r USING (reg_class)
        WHERE n.node_rel_id = ('data','root')::pgwrh.rel_id''') == 1,
        timeout=60, message='reader did not aggregate the two authenticated destinations')
    mappings = reader.execute('''SELECT DISTINCT a.member_role, u.value, p.value
        FROM pgwrh.assignment_target a JOIN pg_foreign_server s ON s.srvname = a.server_name
        JOIN pg_user_mappings m ON m.srvid = s.oid AND m.umuser = 0,
        LATERAL pgwrh.opts(m.umoptions) u, LATERAL pgwrh.opts(m.umoptions) p
        WHERE u.key = 'user' AND p.key = 'password' ORDER BY 1''')
    assert mappings == [(member, *original[member]) for member in ('destination', 'source')]
    reader.execute('''CREATE ROLE application_reader;
        GRANT USAGE ON SCHEMA data TO application_reader;
        GRANT SELECT ON data.root TO application_reader''')
    server = reader.query_scalar("SELECT pgwrh.pgwrh_shard_server('data','root')")
    with reader.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        targets = reader.query_scalar(f'''SELECT shard_server_targets FROM pgwrh.remote_server_route
            WHERE srvname = {quote_literal(server)}''')
        assert set(targets.values()) == {original[member][0] for member in ('source', 'destination')}
        for target in targets:
            reader.execute(f'SELECT pgwrh_fdw_set_members({quote_literal(server)}, ARRAY[{quote_literal(target)}])')
            # A non-superuser must actually authenticate on every virtual target.
            with reader.node.connect() as application:
                application.execute('SET ROLE application_reader')
                assert application.execute('SELECT count(*) FROM data.root') == [(32,)]
        reader.execute(f"SELECT pgwrh_fdw_set_members({quote_literal(server)}, ARRAY[{','.join(map(quote_literal, targets))}])")
        # A valid peer username on the wrong target must not count as ready.
        target, other = targets
        for username, expected_missing in ((targets[other], 2), (targets[target], 0)):
            paused.execute(f'''ALTER USER MAPPING FOR PUBLIC SERVER {quote_ident(target)}
                OPTIONS (SET user {quote_literal(username)})''')
            paused.execute('SELECT pgwrh.report_state()')
            paused.commit()
            assert cluster.master.query_scalar('''SELECT count(*) FROM pgwrh.missing_ready_remote_shard
                WHERE host_id = 'reader' AND version =
                    (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1')''') == expected_missing
        paused.execute('SELECT pg_advisory_unlock(2895359559)')

    with destination.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        move_to_destination(cluster)
        pending = credentials_for(cluster, cluster.master.target_version())
        wait_until(lambda: pending[source.username][0] in installed_users(source), timeout=30,
                   message='source did not install its new login')
        assert pending[destination.username][0] not in installed_users(destination)
        # Another member's new login cannot acknowledge this destination's login.
        assert reader.query_scalar('''SELECT array_agg(DISTINCT shard_server_user ORDER BY shard_server_user)
            FROM pgwrh.assignment_target''') == sorted(original[member][0] for member in ('source', 'destination'))
        with pytest.raises(Exception, match='required remote shards'):
            cluster.master.commit_rollout()
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    with reader.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        cluster.master.rollback_rollout()
        assert cluster.master.query_scalar('SELECT count(*) FROM pgwrh.replication_group_config_lock') == 2
        assert installed_users(destination) == {original[destination.username][0], pending[destination.username][0]}
        # The paused reader still authenticates with the abandoned credentials.
        cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    wait_until(lambda: cluster.master.query_scalar('''SELECT count(*) FROM pgwrh.replication_group_config_lock
        WHERE rollback_unlock IS NOT NULL''') == 0, timeout=60, message='rollback did not finish')
    wait_for_local_credentials(cluster, original)

    move_to_destination(cluster)
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    rotated = credentials_for(cluster, cluster.master.target_version())
    assert all(rotated[member] != original[member] for member in original)
    cluster.master.commit_rollout()
    wait_for_local_credentials(cluster, rotated)
    wait_until(lambda: source.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 0,
               timeout=60, message='source did not hand off after credential rotation')
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
