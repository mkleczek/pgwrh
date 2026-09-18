import pytest

from .pgwrh_testkit import quote_literal, wait_until
from .test_local_first_handoff import handoff_cluster, move_to_destination


@pytest.mark.parametrize('handoff_cluster', [
    dict(master="ctl '=\\", source="source,db", destination="dest '=\\", reader='reader db', shared=shared)
    for shared in (False, True)
], indirect=True, ids=['separate-servers', 'shared-server'])
def test_database_names_survive_replication_routing_and_handoff(handoff_cluster):
    cluster = handoff_cluster
    source, destination, reader = cluster.replicas
    controller_db = cluster.master.query_scalar('SELECT current_database()')
    for replica in cluster.replicas:
        assert replica.query_scalar("""SELECT value FROM pg_foreign_server,
            LATERAL pgwrh.opts(srvoptions)
            WHERE srvname = 'replica_controller' AND key = 'dbname'""") == controller_db
        assert replica.query_scalar('SELECT current_database()') == replica.spec.dbname
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    wait_until(lambda: reader.query_scalar("""SELECT count(*) FROM pgwrh.remote_node n
        JOIN pgwrh.reachable_shard r USING (reg_class)
        WHERE n.node_rel_id = ('data','root')::pgwrh.rel_id""") == 1,
        timeout=60, message='reader did not aggregate both database destinations')
    assert reader.execute("""SELECT DISTINCT dbname FROM pgwrh.assignment_target ORDER BY dbname""") == [
        (name,) for name in sorted([source.spec.dbname, destination.spec.dbname])]
    assert len(reader.query_scalar("""SELECT shard_server_targets
        FROM pgwrh.remote_server_route WHERE srvname = pgwrh.pgwrh_shard_server('data','root')""")) == 2
    # Force each actual destination through the same virtual server. The member
    # API uses the production drain protocol; zero weights are not valid.
    server = reader.query_scalar("SELECT pgwrh.pgwrh_shard_server('data','root')")
    with reader.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        targets = reader.query_scalar(f"SELECT shard_server_targets FROM pgwrh.remote_server_route WHERE srvname = {quote_literal(server)}")
        for target in targets:
            reader.execute(f"SELECT pgwrh_fdw_set_members({quote_literal(server)}, ARRAY[{quote_literal(target)}])")
            assert reader.query_scalar('SELECT count(*) FROM data.root') == 32
        reader.execute(f"SELECT pgwrh_fdw_set_members({quote_literal(server)}, ARRAY[{','.join(map(quote_literal, targets))}])")
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    cluster.master.execute("INSERT INTO data.root VALUES (33, 'new write')")
    for replica in (source, destination):
        wait_until(lambda: replica.query_scalar('SELECT count(*) FROM data.root') == 33,
                   timeout=30, message=f'{replica.name} did not receive the write')
    move_to_destination(cluster)
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    cluster.master.commit_rollout()
    wait_until(lambda: source.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 0,
               timeout=60, message='source did not hand off to the other database')
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    # Old credentials can be retired in one database while a sibling still uses
    # them. Every database must eventually report only the current credential.
    wait_until(lambda: cluster.master.query_scalar("""SELECT bool_and(json_array_length(users) = 1)
        FROM pgwrh.replication_group_member"""), timeout=60, message='credential rotation did not finish')


def test_database_registration_defaults_and_endpoint_uniqueness(postgres_node_factory):
    node = postgres_node_factory('database_registration')
    node.execute("""SELECT pgwrh.create_replica_cluster('g');
        CREATE ROLE first; CREATE ROLE second; CREATE ROLE third;
        SELECT pgwrh.add_replica('g', 'first', 'host', 5432);
        SELECT pgwrh.add_replica('g', 'second', 'host', 5432, _dbname := 'another db');""")
    assert node.execute("SELECT dbname = current_database() FROM pgwrh.shard_host WHERE host_id = 'first'") == [(True,)]
    for database, error in (("'another db'", 'duplicate key'), ("''", 'check constraint'), ('NULL', 'not-null constraint')):
        with pytest.raises(Exception, match=error):
            node.execute(f"SELECT pgwrh.add_replica('g', 'third', 'host', 5432, _dbname := {database})")
    assert node.execute('SELECT count(*) FROM pgwrh.replication_group_member') == [(2,)]
    for database in ("''", 'NULL'):
        with pytest.raises(Exception, match='Controller database name must not be empty'):
            node.execute(f"SELECT pgwrh.configure_controller('host','5432','u','p',false, dbname := {database})")
