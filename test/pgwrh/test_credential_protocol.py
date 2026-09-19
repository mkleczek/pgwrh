import pytest

from .pgwrh_testkit import quote_ident, quote_literal, wait_until
from .test_local_first_handoff import handoff_cluster


def credentials_for(cluster, generation=None):
    condition = "g.state = 'active'" if generation is None else f"g.generation = {quote_literal(generation)}"
    return {member: (username, password) for member, username, password in cluster.master.execute(
        f'''SELECT source_role, username, password FROM pgwrh.source_credential c
            JOIN pgwrh.credential_generation g USING (replication_group_id, generation)
            WHERE {condition}''')}


def installed_users(replica):
    return {name for name, in replica.execute('''SELECT u.rolname
        FROM pg_roles u JOIN pg_auth_members a ON a.member = u.oid
        JOIN pg_roles g ON g.oid = a.roleid
        WHERE g.rolname = pgwrh.pgwrh_replica_role_name()''')}


def wait_for_local_credentials(cluster, credentials):
    wait_until(lambda: all(installed_users(replica) == {
        value[0] for member, value in credentials.items() if member != replica.username
    } for replica in cluster.replicas), timeout=60,
        message='local credential installation or retirement did not finish')


def generation_states(cluster):
    return {str(generation): state for generation, state in cluster.master.execute(
        'SELECT generation, state FROM pgwrh.credential_generation')}


def rotate(cluster):
    return str(cluster.master.query_scalar("SELECT pgwrh.rotate_credentials('g1')"))


def wait_rotation(cluster, generation):
    wait_until(lambda: generation_states(cluster) == {generation: 'active'}, timeout=60,
               message='credential rotation did not complete')
    wait_for_local_credentials(cluster, credentials_for(cluster))


def test_credential_feeds_are_scoped_to_source_and_destination(postgres_node_factory):
    node = postgres_node_factory('credential_feeds')
    node.execute("""
        SELECT pgwrh.create_replica_cluster('one');
        SELECT pgwrh.create_replica_cluster('two');
        CREATE ROLE a; CREATE ROLE b; CREATE ROLE c; CREATE ROLE outsider;
        SELECT pgwrh.add_replica('one', 'a', 'a', 5432);
        SELECT pgwrh.add_replica('one', 'b', 'b', 5432);
        SELECT pgwrh.add_replica('two', 'c', 'c', 5432);
    """)
    credentials = {member: (username, password) for member, username, password in node.execute(
        'SELECT source_role, username, password FROM pgwrh.source_credential')}
    assert len(set(credentials.values())) == 3
    for member, peers in [('a', ['b']), ('b', ['a']), ('c', []), ('outsider', [])]:
        local = node.execute(f'''SELECT username, verifier FROM pgwrh.replica_credentials
            WHERE member_role = {quote_literal(member)} ORDER BY username''')
        with node.connect() as conn:
            conn.execute(f'SET ROLE {quote_ident(member)}')
            assert conn.execute('SELECT * FROM pgwrh.local_credentials ORDER BY username') == local
            assert all(password.startswith('SCRAM-SHA-256$') for _, password in local)
            assert conn.execute('SELECT * FROM pgwrh.remote_credentials ORDER BY member_role') == [
                (peer, *credentials[member]) for peer in peers]
            active = conn.execute('SELECT username FROM pgwrh.credential_state')
            assert active == ([(credentials[member][0],)] if member in credentials else [])

    # A generation is created only by an explicit rotation, even with a locked
    # topology. Empty groups complete immediately; normal input errors are clear.
    node.execute("SELECT pgwrh.create_replica_cluster('empty')")
    for _ in range(2):
        generation = node.execute("SELECT pgwrh.rotate_credentials('empty')")[0][0]
        assert node.execute("SELECT generation, state FROM pgwrh.credential_generation WHERE replication_group_id = 'empty'") == [(generation, 'active')]
    node.execute("SELECT pgwrh.rotate_credentials('one')")
    with pytest.raises(Exception, match='already in progress'):
        node.execute("SELECT pgwrh.rotate_credentials('one')")
    with pytest.raises(Exception, match='Unknown replication group'):
        node.execute("SELECT pgwrh.rotate_credentials('missing')")


@pytest.mark.parametrize('handoff_cluster', [
    dict(source='source_db', destination='destination_db', reader='reader_db', shared=shared)
    for shared in (False, True)
], indirect=True, ids=['separate-servers', 'shared-server'])
def test_source_credentials_authenticate_and_rotate_without_topology_change(handoff_cluster):
    cluster = handoff_cluster
    source, destination, reader = cluster.replicas
    topology = cluster.master.execute('SELECT * FROM pgwrh.replication_group_config_lock ORDER BY 1, 2')
    original = credentials_for(cluster)
    original_generation, = generation_states(cluster)
    assert len({username for username, _ in original.values()}) == 3
    assert len({password for _, password in original.values()}) == 3
    wait_for_local_credentials(cluster, original)

    def verify_authentication(credentials):
        for replica in cluster.replicas:
            for member, (username, password) in credentials.items():
                if member == replica.username:
                    continue
                with replica.node.connect(username=username, password=password) as authenticated:
                    assert authenticated.execute('SELECT current_user') == [(username,)]
                    assert authenticated.execute("SELECT has_table_privilege(current_user, 'pgwrh.sync', 'SELECT')") == [(False,)]
                expected = cluster.master.query_scalar(f'''SELECT verifier FROM pgwrh.replica_credentials
                    WHERE state = 'active' AND source_role = {quote_literal(member)}
                        AND member_role = {quote_literal(replica.username)}''')
                assert replica.query_scalar(f'SELECT rolpassword FROM pg_authid WHERE rolname = {quote_literal(username)}') == expected
        # Reusable source passwords are the same on all of that source's mappings;
        # target verifier salts differ across physical PostgreSQL servers.
        verifier_counts = cluster.master.execute('''SELECT source_role, count(DISTINCT verifier)
            FROM pgwrh.replica_credentials WHERE state = 'active' GROUP BY source_role''')
        physical_count = len({replica.node.port for replica in cluster.replicas})
        assert all(count == (1 if physical_count == 1 else 2) for _, count in verifier_counts)

    verify_authentication(original)
    wait_until(lambda: reader.query_scalar('''SELECT count(*) FROM pgwrh.remote_node n
        JOIN pgwrh.reachable_shard r USING (reg_class)
        WHERE n.node_rel_id = ('data','root')::pgwrh.rel_id''') == 1,
        timeout=60, message='reader did not aggregate authenticated destinations')
    mappings = reader.execute('''SELECT DISTINCT a.member_role, u.value, p.value
        FROM pgwrh.assignment_target a JOIN pg_foreign_server s ON s.srvname = a.server_name
        JOIN pg_user_mappings m ON m.srvid = s.oid AND m.umuser = 0,
        LATERAL pgwrh.opts(m.umoptions) u, LATERAL pgwrh.opts(m.umoptions) p
        WHERE u.key = 'user' AND p.key = 'password' ORDER BY 1''')
    assert mappings == [(member, *original['reader']) for member in ('destination', 'source')]
    reader.execute('''CREATE ROLE application_reader;
        GRANT USAGE ON SCHEMA data TO application_reader;
        GRANT SELECT ON data.root TO application_reader''')
    server = reader.query_scalar("SELECT pgwrh.pgwrh_shard_server('data','root')")
    with reader.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        targets = reader.query_scalar(f'''SELECT shard_server_targets FROM pgwrh.remote_server_route
            WHERE srvname = {quote_literal(server)}''')
        assert set(targets.values()) == {original['reader'][0]}
        for target in targets:
            reader.execute(f'SELECT pgwrh_fdw_set_members({quote_literal(server)}, ARRAY[{quote_literal(target)}])')
            with reader.node.connect() as application:
                application.execute('SET ROLE application_reader')
                assert application.execute('SELECT count(*) FROM data.root') == [(32,)]
        reader.execute(f"SELECT pgwrh_fdw_set_members({quote_literal(server)}, ARRAY[{','.join(map(quote_literal, targets))}])")
        paused.execute('SELECT pg_advisory_unlock(2895359559)')

    with destination.node.connect() as delayed, reader.node.connect() as slow_reader:
        delayed.execute('SELECT pg_advisory_lock(2895359559)')
        pending = rotate(cluster)
        new_credentials = credentials_for(cluster, pending)
        wait_until(lambda: cluster.master.query_scalar(f'''SELECT count(*) FROM pgwrh.missing_credential_installation
            WHERE generation = {quote_literal(pending)} AND member_role <> 'destination' ''') == 0,
            timeout=60, message='other destinations did not install pending verifiers')
        assert generation_states(cluster) == {original_generation: 'active', pending: 'preparing'}
        assert credentials_for(cluster) == original
        slow_reader.execute('SELECT pg_advisory_lock(2895359559)')
        delayed.execute('SELECT pg_advisory_unlock(2895359559)')
        wait_until(lambda: generation_states(cluster).get(pending) == 'active', timeout=60,
                   message='new credentials were not activated after installation')
        assert generation_states(cluster)[original_generation] == 'retiring'
        cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
        assert all(original[member][0] in installed_users(destination)
                   for member in ('source', 'reader'))
        slow_reader.execute('SELECT pg_advisory_unlock(2895359559)')
    wait_rotation(cluster, pending)
    assert credentials_for(cluster) == new_credentials
    verify_authentication(new_credentials)
    assert cluster.master.execute('SELECT * FROM pgwrh.replication_group_config_lock ORDER BY 1, 2') == topology
    # Generations never reuse the two topology labels or an earlier secret.
    next_generation = rotate(cluster)
    assert next_generation not in (pending, original_generation)
    wait_rotation(cluster, next_generation)
    assert all(credentials_for(cluster)[member] != original[member] for member in original)
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
