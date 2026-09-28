from __future__ import annotations

import pytest

from .pgwrh_testkit import quote_ident


SIGNATURE = 'pgwrh.configure_controller(text,text,text,text,boolean,real,text)'


@pytest.mark.parametrize('controller', [False, True], ids=['fresh-replica', 'controller'])
def test_controller_setup_rejects_untrusted_logins(postgres_node_factory, controller):
    node = postgres_node_factory('controller_privileges')
    if controller:
        node.execute("SELECT pgwrh.create_replica_cluster('g1')")
    peer_group = node.execute('SELECT pgwrh.pgwrh_replica_role_name()')[0][0]
    node.execute(f'''CREATE ROLE ordinary LOGIN;
        CREATE ROLE peer LOGIN IN ROLE {quote_ident(peer_group)}''')
    original_mapping = node.execute('SELECT srvoptions FROM pg_foreign_server')

    for role in ('ordinary', 'peer'):
        assert node.execute(f"SELECT has_function_privilege('{role}', '{SIGNATURE}', 'EXECUTE')") == [(False,)]
        # Omitted defaults and named arguments must resolve to the protected API.
        for args in ("'127.0.0.1', '1', 'attacker', 'secret'",
                     "host := '127.0.0.1', port := '1', username := 'attacker', "
                     "password := 'secret', start_daemon := false, dbname := 'hostile'"):
            with pytest.raises(Exception, match='permission denied for function configure_controller'):
                node.execute(f'SELECT pgwrh.configure_controller({args})', username=role)

    assert node.execute('SELECT srvoptions FROM pg_foreign_server') == original_mapping
    assert node.execute('SELECT subname FROM pg_subscription') == []
    assert node.execute('SELECT subname FROM pgwrh.shard_subscription') == []
    assert node.execute("SELECT pid FROM pg_stat_activity WHERE application_name = 'pgwrh_sync_daemon'") == []


def test_setup_does_not_elevate_an_explicitly_granted_caller(postgres_node_factory):
    node = postgres_node_factory('controller_invoker')
    node.execute('CREATE ROLE delegated LOGIN')
    node.execute(f'GRANT EXECUTE ON FUNCTION {SIGNATURE} TO delegated')
    assert node.execute(f"SELECT prosecdef FROM pg_proc WHERE oid = '{SIGNATURE}'::regprocedure") == [(False,)]
    with pytest.raises(Exception, match='must be owner of foreign server replica_controller'):
        node.execute("SELECT pgwrh.configure_controller('127.0.0.1', '1', 'attacker', 'secret')",
                     username='delegated')
    assert node.execute('SELECT subname FROM pg_subscription') == []
