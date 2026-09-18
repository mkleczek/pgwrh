import pytest


def test_install_is_separate_and_reading_does_not_create_draft(controller):
    controller.execute("SELECT pgwrh.create_replica_cluster('empty')")
    assert controller.execute("SELECT phase, replica_count FROM pgwrh_ui.group_state('empty')") == [('stable', 0)]
    assert controller.execute("SELECT * FROM pgwrh_ui.placement_diff('empty')") == []
    assert controller.execute('SELECT count(*) FROM pgwrh.replication_group_config') == [(1,)]
    assert controller.execute("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'pgwrh_ui' AND c.relkind IN ('r', 'p')") == [(0,)]
    controller.execute('DROP EXTENSION pgwrh_ui')
    assert controller.execute('SELECT replication_group_id FROM pgwrh.replication_group') == [('empty',)]


def test_preview_and_frozen_snapshot(configured):
    node = configured
    assert node.execute("SELECT phase FROM pgwrh_ui.group_state('g1')") == [('draft',)]
    assert node.execute("SELECT change, count(*) FROM pgwrh_ui.placement_diff('g1') GROUP BY change") == [('add', 4)]
    node.execute("SELECT pgwrh.start_rollout('g1')")
    before = node.execute("SELECT * FROM pgwrh_ui.placement_diff('g1') ORDER BY 1, 2, 3, 4")
    assert {row[5] for row in before} == {'Target snapshot'}
    node.execute('ALTER TABLE data.root DETACH PARTITION data.two')
    assert node.execute("SELECT * FROM pgwrh_ui.placement_diff('g1') ORDER BY 1, 2, 3, 4") == before


def test_blockers_and_replica_reports(configured):
    node = configured
    node.execute("SELECT pgwrh.start_rollout('g1')")
    assert node.execute("SELECT kind, count(*) FROM pgwrh_ui.rollout_blockers('g1') GROUP BY kind") == [('subscription', 4)]
    node.execute("""UPDATE pgwrh.replication_group_member SET
        subscribed_local_shards = '[{"schema_name":"data","table_name":"one"}]'
        WHERE host_id = 'r1'""")
    assert node.execute("SELECT kind FROM pgwrh_ui.rollout_blockers('g1') WHERE host_id = 'r1' AND table_name = 'one'") == [('local',)]
    assert node.execute("SELECT current_copies, target_copies, slot_count, confirmed_lag_bytes FROM pgwrh_ui.replica_state('g1')") == [(0, 2, 0, None), (0, 2, 0, None)]
    node.execute("SELECT pgwrh.rollback_rollout('g1')")
    assert node.execute("SELECT phase FROM pgwrh_ui.group_state('g1')") == [('rolling_back',)]


def test_infeasible_preview_does_not_change_configuration(configured):
    node = configured
    node.execute("UPDATE pgwrh.replication_group_config SET min_replica_count_after_az_failure = 2 WHERE version = 'FLOP'")
    with pytest.raises(Exception, match='Cannot place'):
        node.execute("SELECT * FROM pgwrh_ui.placement_diff('g1')")
    assert node.execute("SELECT phase FROM pgwrh_ui.group_state('g1')") == [('draft',)]


def test_helpers_are_not_public(configured):
    configured.execute('CREATE ROLE untrusted')
    assert configured.execute("""SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pgwrh_ui' AND has_function_privilege('untrusted', p.oid, 'EXECUTE')""") == [(0,)]
