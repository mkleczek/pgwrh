"""A reproducible mixed workload across real placement/authentication changes."""
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from .pgwrh_testkit import wait_until
from .test_credential_protocol import generation_states, rotate, wait_rotation
from .test_local_first_handoff import handoff_cluster, move_to_destination


@pytest.mark.parametrize('finish', ['commit', 'rollback'])
@pytest.mark.parametrize('handoff_cluster', [dict(source='source_db', destination='target_db', reader='reader_db')], indirect=True)
def test_mixed_writes_and_reads_during_rollout_and_rotation(handoff_cluster, finish):
    cluster = handoff_cluster
    _, destination, _ = cluster.replicas
    stop = Event()
    committed = []
    reads = []
    baseline = [(n, f'row {n}') for n in range(1, 33)]
    query = 'SELECT id, value FROM data.root ORDER BY id'
    for replica in cluster.replicas:
        assert replica.query_scalar('''SELECT count(*) FROM pgwrh.connected_local_shard s
            JOIN pgwrh.rel r USING (rel_id)
            WHERE NOT EXISTS (SELECT 1 FROM pg_constraint c
                              WHERE c.conrelid = r.reg_class AND c.contype = 'p')''') == 0

    def write():
        with cluster.master.node.connect() as conn:
            conn.execute("SET statement_timeout = '30s'")
            conn.commit()
            sequence = 0
            while not stop.is_set():
                sequence += 1
                ident = 1000 + sequence
                conn.execute('INSERT INTO data.root VALUES (%s, %s)', ident, f'insert:{sequence}')
                conn.execute('UPDATE data.root SET value = %s WHERE id = %s',
                             f'updated:{sequence}', ident - 1)
                conn.execute('DELETE FROM data.root WHERE id = %s', ident - 3)
                conn.commit()
                # Abort all three DML forms in the same persistent session.
                conn.execute('INSERT INTO data.root VALUES (%s, %s)', -sequence, 'aborted')
                conn.execute("UPDATE data.root SET value = 'aborted' WHERE id = 1")
                conn.execute('DELETE FROM data.root WHERE id = 2')
                conn.rollback()
                committed.append(sequence)
                stop.wait(0.02)

    def read():
        while not stop.is_set():
            for replica in cluster.replicas:
                with replica.node.connect(autocommit=True) as conn:
                    conn.execute("SET statement_timeout = '30s'")
                    rows = conn.execute(query)
                assert len({ident for ident, _ in rows}) == len(rows), (replica.name, rows)
                assert [(ident, value) for ident, value in rows if ident <= 32] == baseline
                for ident, value in rows:
                    if ident > 32:
                        assert ident > 1000
                        sequence = ident - 1000
                        assert value in (f'insert:{sequence}', f'updated:{sequence + 1}')
            reads.append(True)

    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(write)
        reader = pool.submit(read)

        def progress():
            writes_before, reads_before = len(committed), len(reads)

            def advanced():
                for task in (writer, reader):
                    if task.done():
                        task.result()  # propagate worker assertions immediately
                        raise AssertionError('workload stopped before the lifecycle finished')
                return len(committed) >= writes_before + 3 and len(reads) >= reads_before + 2

            wait_until(advanced, timeout=30, message='concurrent workload made no progress')

        try:
            progress()
            # This replica already has its local copies. Delaying its sync pass
            # holds credential installation without blocking ordinary replication.
            with destination.node.connect() as delayed:
                delayed.execute('SELECT pg_advisory_lock(2895359559)')
                pending = rotate(cluster)
                move_to_destination(cluster)
                cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
                assert generation_states(cluster)[pending] == 'preparing'
                progress()
                if finish == 'commit':
                    cluster.master.commit_rollout()
                else:
                    cluster.master.rollback_rollout()
                progress()
                assert generation_states(cluster)[pending] == 'preparing'
                delayed.execute('SELECT pg_advisory_unlock(2895359559)')
            wait_rotation(cluster, pending)
            wait_until(lambda: cluster.master.query_scalar('''SELECT count(*)
                FROM pgwrh.replication_group_config_lock WHERE rollback_unlock IS NOT NULL''') == 0,
                timeout=60, message='rollback cleanup did not finish')
            progress()
        finally:
            stop.set()
        writer.result(timeout=30)
        reader.result(timeout=30)

    # During traffic different shards can legitimately lag independently. Once
    # writes stop, demand exact convergence, not merely matching row counts.
    expected = cluster.master.execute(query)
    last = committed[-1]
    assert expected == baseline + [
        (1000 + sequence, f'updated:{sequence + 1}' if sequence < last else f'insert:{sequence}')
        for sequence in range(max(1, last - 2), last + 1)
    ]
    wait_until(lambda: all(replica.execute(query) == expected for replica in cluster.replicas),
               timeout=60, message='replicas did not converge to the committed mixed workload')
