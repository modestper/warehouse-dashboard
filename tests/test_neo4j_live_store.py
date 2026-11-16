"""Direct persistence contracts, with a transactional Neo4j driver double."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from backend.knowledge import KnowledgeError
from backend.neo4j_live_store import Neo4jLiveStore


class Result:
    def __init__(self, row=None):
        self.row = row

    def single(self):
        return self.row

    def consume(self):
        return self


class Transaction:
    def __init__(self, driver, state):
        self.driver, self.state = driver, state

    def run(self, query, **params):
        if query.startswith('CREATE CONSTRAINT'):
            return Result()
        run = params['run_id']
        if query.startswith('MERGE (h:WarehouseLiveCheckpoint'):
            head = self.state['heads'].setdefault(run, {'seq': 0, 'payload': None, 'checksum': None})
            return Result(dict(head))
        if query.startswith('MATCH (c:WarehouseLiveCommit'):
            commit = self.state['commits'].get((run, params['seq']))
            return Result({'checksum': commit['events_checksum']} if commit else None)
        if query.startswith('CREATE (c:WarehouseLiveCommit'):
            self.state['commits'][(run, params['seq'])] = dict(params)
            return Result()
        if query.startswith('MATCH (h:WarehouseLiveCheckpoint'):
            if self.driver.fail_head_update:
                raise RuntimeError('injected Neo4j transaction failure')
            self.state['heads'][run] = {key: params[key] for key in ('seq', 'payload', 'checksum')}
            return Result()
        raise AssertionError(query)


class Session:
    def __init__(self, driver):
        self.driver = driver

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def run(self, query, **params):
        return Transaction(self.driver, self.driver.state).run(query, **params)

    def execute_write(self, callback):
        candidate = deepcopy(self.driver.state)
        result = callback(Transaction(self.driver, candidate))
        self.driver.state = candidate
        return result


class Driver:
    def __init__(self):
        self.state = {'heads': {}, 'commits': {}}
        self.fail_head_update = False
        self.databases = []
        self.closed = False

    def session(self, *, database):
        self.databases.append(database)
        return Session(self)

    def close(self):
        self.closed = True


def checkpoint(seq):
    return {'run_id': 'direct-test', 'knowledge_seq': seq, 'tick': seq - 1,
            'elapsed': seq - 1, 'completed': 0, 'config': {'flow_rate': 0},
            'running': False, 'speed': 1, 'racks': [], 'tasks': {}, 'robots': []}


class DirectStoreTests(unittest.TestCase):
    def setUp(self):
        self.driver = Driver()
        self.store = Neo4jLiveStore(uri='bolt://unused', user='neo4j', password='unused',
                                    database='warehouse', driver=self.driver)
        self.addCleanup(self.store.close)

    def test_checkpoint_and_events_are_committed_to_named_database(self):
        events = [{'kind': 'task_assigned', 'robot_id': 1}]
        self.store.append(checkpoint(1), events)
        self.assertEqual(json.loads(self.driver.state['heads']['direct-test']['payload']), checkpoint(1))
        batch = json.loads(self.driver.state['commits'][('direct-test', 1)]['payload'])
        self.assertEqual(batch['events'], events)
        self.assertEqual(set(self.driver.databases), {'warehouse'})
        status = self.store.status('direct-test')
        self.assertEqual(status['durable_seq'], 1)
        self.assertEqual(status['direct_commit_seq'], 1)
        self.assertEqual(status['mirrored_seq'], 0)  # no legacy A query watermark
        self.assertFalse(status['local_journal'])

    def test_retry_is_idempotent_but_conflicting_payloads_and_sequence_gaps_fail(self):
        self.store.append(checkpoint(1), [])
        self.store.append(checkpoint(1), [])
        self.assertEqual(len(self.driver.state['commits']), 1)
        for cp, events in (({**checkpoint(1), 'completed': 99}, []),
                           (checkpoint(1), [{'kind': 'different'}]), (checkpoint(3), [])):
            with self.subTest(cp=cp, events=events), self.assertRaises(KnowledgeError):
                self.store.append(cp, events)
        self.assertEqual(self.driver.state['heads']['direct-test']['seq'], 1)

    def test_failure_rolls_back_event_batch_and_never_advances_durable_sequence(self):
        self.store.append(checkpoint(1), [])
        self.driver.fail_head_update = True
        with self.assertRaises(KnowledgeError):
            self.store.append(checkpoint(2), [{'kind': 'picked'}])
        self.assertNotIn(('direct-test', 2), self.driver.state['commits'])
        self.assertEqual(self.store.status('direct-test')['durable_seq'], 1)
        self.assertIsNotNone(self.store.status('direct-test')['last_error'])
        self.driver.fail_head_update = False
        self.store.append(checkpoint(2), [{'kind': 'picked'}])
        self.assertEqual(self.store.status('direct-test')['durable_seq'], 2)

    def test_corrupt_checkpoint_is_rejected(self):
        self.store.append(checkpoint(1), [])
        self.driver.state['heads']['direct-test']['checksum'] = 'corrupted'
        with self.assertRaises(KnowledgeError):
            self.store.append(checkpoint(2), [])
        self.assertEqual(self.store.status('direct-test')['durable_seq'], 1)


class LiveNoJournalTests(unittest.TestCase):
    def test_no_journal_requires_complete_graph_configuration(self):
        from ops.warehouse_live_runtime import WarehouseLiveRuntime
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, '--no-journal requires'):
                WarehouseLiveRuntime(source_root=Path(__file__).resolve().parents[1], output_root=folder,
                    datasets={}, dataset_id='unused', no_journal=True)

    def test_real_live_control_commits_without_creating_a_sqlite_journal(self):
        from ops.warehouse_live_runtime import WarehouseLiveRuntime
        from backend.warehouse.composition import physical_classes
        root = Path(__file__).resolve().parents[1]
        simulation, config = physical_classes('wheeled')
        driver = Driver()
        # Exercise the real owner/control/persistence path; actor execution and
        # Neo4j transport are doubles, so no Ray cluster or real graph is touched.
        def fake_simulation(*args, **kwargs):
            for key in ('robot_graph', 'graph_state_interval', 'kg_state_source', 'global_writer_url',
                        'robot_graph_async', 'robot_graph_backlog', 'robot_context_interval'):
                kwargs.pop(key, None)
            sim = simulation(*args, **kwargs)
            sim.global_writer_url = 'test-owned-per-step-writer'
            return sim
        datasets = {'medium19': {
            'scene': str(root / 'data/layouts/expansion-candidates/medium/layout.json'),
            'dataset': str(root / 'data/examples/wheeled-medium-sample19'),
            'config_json': str(root / 'config/wheeled-medium-100.json')}}
        store = Neo4jLiveStore(uri='bolt://unused', user='neo4j', password='unused',
                               database='warehouse', driver=driver)
        with tempfile.TemporaryDirectory() as folder, \
                patch.dict('os.environ', {'WAREHOUSE_NEO4J_URI': 'bolt://unused'}), \
                patch('backend.warehouse.composition.physical_classes', return_value=(fake_simulation, config)), \
                patch.object(Neo4jLiveStore, 'from_env', return_value=store):
            runtime = WarehouseLiveRuntime(source_root=root, output_root=folder, datasets=datasets,
                dataset_id='medium19', run_id='no-journal-live', knowledge_backend='neo4j', no_journal=True,
                mobility='wheeled', robot_processes='ray', robot_graph=True, global_writer_url='http://unused')
            try:
                self.assertEqual(runtime.command('step')['elapsed'], 1)
                self.assertEqual(runtime.latest()['knowledge']['backend'], 'neo4j-direct')
                self.assertFalse(runtime.latest()['metadata']['local_journal'])
                self.assertEqual(list(Path(folder).rglob('journal.sqlite*')), [])
                self.assertIn(('no-journal-live', 2), driver.state['commits'])
            finally:
                runtime.shutdown()
            self.assertTrue(driver.closed)


if __name__ == '__main__':
    unittest.main()
