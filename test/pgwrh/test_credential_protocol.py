from .pgwrh_testkit import quote_ident


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
