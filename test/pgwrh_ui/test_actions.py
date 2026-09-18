from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def revision(node):
    return node.execute("SELECT pgwrh_ui.revision('g1')")[0][0]


def mutate(node, operation, expected=None, **fields):
    if expected is None:
        expected = revision(node)
    with node.connect() as conn:
        args = {'group_id': 'g1', 'operation': operation, 'expected': expected, **fields}
        sql = 'SELECT pgwrh_ui.mutate(' + ','.join(k + ' := %s' for k in args) + ')'
        result = conn.execute(sql, *args.values())[0][0]
        conn.commit()
        return result


def report_all_local(node):
    node.execute("""UPDATE pgwrh.replication_group_member m SET
        connected_local_shards=(SELECT json_agg(json_build_object('schema_name',s.schema_name,'table_name',s.table_name))
            FROM pgwrh.shard_assigned_host s WHERE s.replication_group_id=m.replication_group_id
              AND s.availability_zone=m.availability_zone AND s.host_id=m.host_id AND s.version='FLOP')""")


def test_registration_and_pending_weight(configured):
    node = configured
    node.execute('CREATE ROLE replica3 LOGIN REPLICATION')
    html = mutate(node,'add',replica_id='r3',availability_zone='a',host_name='r3.invalid',member_role='replica3',weight=70)
    assert 'Replica registered' in html
    assert node.execute("SELECT weight FROM pgwrh.shard_host_weight WHERE host_id='r3'") == [(70,)]
    assert node.execute("SELECT current_version,target_version FROM pgwrh.replication_group") == [('FLIP','FLIP')]
    html = mutate(node,'weight',replica_id='r3',availability_zone='a',weight=123)
    assert 'Pending weight saved' in html
    assert node.execute("SELECT weight FROM pgwrh.shard_host_weight WHERE host_id='r3'") == [(123,)]
    assert len(node.execute("SELECT * FROM pgwrh_ui.placement_diff('g1')")) == 6


def test_invalid_registration_is_atomic_and_does_not_create_roles(configured):
    node = configured
    before = revision(node)
    assert 'Choose an existing' in mutate(node,'add',replica_id='r3',availability_zone='a',host_name='r3.invalid',member_role='missing')
    assert revision(node) == before
    assert 'already registered' in mutate(node,'add',replica_id='r3',availability_zone='a',host_name='r3.invalid',member_role='replica1')
    assert revision(node) == before
    assert node.execute("SELECT count(*) FROM pg_roles WHERE rolname='missing'") == [(0,)]


def test_stale_forms_and_partition_changes_are_rejected(configured):
    node = configured
    before = revision(node)
    mutate(node,'weight',replica_id='r1',availability_zone='a',weight=110)
    assert 'Configuration changed' in mutate(node,'routing',expected=before,replica_id='r1',availability_zone='a',online=False)
    assert node.execute("SELECT online FROM pgwrh.shard_host WHERE host_id='r1'") == [(True,)]
    before = revision(node)
    node.execute('ALTER TABLE data.root DETACH PARTITION data.two')
    assert 'Configuration changed' in mutate(node,'start',expected=before)
    assert node.execute("SELECT phase FROM pgwrh_ui.group_state('g1')") == [('draft',)]


def test_routing_and_exclusion_have_different_effects(configured):
    node = configured
    mutate(node,'routing',replica_id='r1',availability_zone='a',online=False)
    assert node.execute("SELECT online FROM pgwrh.shard_host WHERE host_id='r1'") == [(False,)]
    assert node.execute("SELECT count(*) FROM pgwrh.shard_host_weight WHERE host_id='r1'") == [(1,)]
    mutate(node,'exclude',replica_id='r1',availability_zone='a')
    assert node.execute("SELECT count(*) FROM pgwrh.shard_host_weight WHERE host_id='r1'") == [(0,)]
    assert node.execute("SELECT count(*) FROM pgwrh.replication_group_member") == [(2,)]
    mutate(node,'weight',replica_id='r1',availability_zone='a',weight=90)
    assert node.execute("SELECT weight FROM pgwrh.shard_host_weight WHERE host_id='r1'") == [(90,)]


def test_rollout_rechecks_readiness_and_requires_commit_confirmation(configured):
    node = configured
    assert 'Rollout started' in mutate(node,'start')
    assert 'Wait for the rollout' in mutate(node,'weight',replica_id='r1',availability_zone='a',weight=110)
    assert 'Confirm that committing' in mutate(node,'commit')
    assert 'Not all hosts confirmed' in mutate(node,'commit',confirm=True)
    assert node.execute("SELECT phase FROM pgwrh_ui.group_state('g1')") == [('rolling_out',)]
    token = revision(node)
    report_all_local(node)
    assert revision(node) == token  # reports do not invalidate the form
    assert 'Rollout committed' in mutate(node,'commit',expected=token,confirm=True)
    assert node.execute("SELECT phase FROM pgwrh_ui.group_state('g1')") == [('stable',)]
    # Excluding a host from a fresh draft must clone all current configuration.
    assert 'excluded from pending' in mutate(node,'exclude',replica_id='r1',availability_zone='a')
    assert node.execute("SELECT version,host_id FROM pgwrh.shard_host_weight ORDER BY 1,2") == [('FLIP','r2'),('FLOP','r1'),('FLOP','r2')]
    assert node.execute("SELECT count(*) FROM pgwrh.sharded_table") == [(2,)]


def test_rollback_keeps_core_acknowledgement_protocol(configured):
    mutate(configured,'start')
    assert 'Confirm rollback' in mutate(configured,'rollback')
    assert 'Rollback requested' in mutate(configured,'rollback',confirm=True)
    assert configured.execute("SELECT phase FROM pgwrh_ui.group_state('g1')") == [('rolling_back',)]
    assert 'Wait for the rollout' in mutate(configured,'weight',replica_id='r1',availability_zone='a',weight=110)


def test_operator_boundary_and_controls(configured):
    configured.psql(filename=str(ROOT / 'pgwrh_ui/readonly.sql'))
    configured.psql(filename=str(ROOT / 'pgwrh_ui/operator.sql'))
    with configured.connect() as conn:
        conn.execute('SET ROLE pgwrh_ui_viewer')
        assert 'Add replica' not in conn.execute("SELECT pgwrh_ui.index('g1','replicas')")[0][0]
        with pytest.raises(Exception, match='permission denied'):
            conn.execute("SELECT pgwrh_ui.mutate('g1','start','irrelevant')")
    with configured.connect() as conn:
        conn.execute('SET ROLE pgwrh_ui_operator')
        assert 'Add replica' in conn.execute("SELECT pgwrh_ui.index('g1','replicas')")[0][0]
        assert conn.execute("SELECT has_table_privilege(current_user,'pgwrh.shard_host_weight','UPDATE')") == [(False,)]


def test_concurrent_submissions_do_not_overwrite_each_other(configured):
    token = revision(configured)
    def save(weight):
        return mutate(configured,'weight',expected=token,replica_id='r1',availability_zone='a',weight=weight)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(save,[201,202]))
    assert sum('Pending weight saved' in html for html in results) == 1
    assert sum('Configuration changed' in html for html in results) == 1


def test_temporary_objects_cannot_shadow_controller_api_relations(configured):
    configured.psql(filename=str(ROOT / 'pgwrh_ui/readonly.sql'))
    configured.psql(filename=str(ROOT / 'pgwrh_ui/operator.sql'))
    token = revision(configured)
    with configured.connect() as conn:
        conn.execute('SET ROLE pgwrh_ui_operator')
        conn.execute('CREATE TEMP TABLE shard_host_weight (payload text)')
        with pytest.raises(Exception, match='fresh database session'):
            conn.execute('SELECT pgwrh_ui.mutate(%s,%s,%s)', 'g1','start',token)
    assert revision(configured) == token
