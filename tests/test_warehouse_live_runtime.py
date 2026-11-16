from contextlib import closing
import importlib.util
import json
import math
from pathlib import Path
from types import SimpleNamespace
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('owned_live_runtime', ROOT / 'ops/warehouse_live_runtime.py')
MOD = importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(MOD)
TEMP = ROOT / 'data/local-check'
TEMP.mkdir(parents=True, exist_ok=True)
REGISTRY = {'sample19': {'scene': str(ROOT / 'data/layouts/warehouse100-wide-v2.json'),
                          'dataset': str(ROOT / 'data/examples/warehouse-v5'),
                          'config': {'charging_policy': 'critical-only'}}}


class ValidationTests(unittest.TestCase):
    def test_wall_speed_is_bounded_finite_and_not_bool(self):
        for invalid in (True, False, 0, -.1, .01, 121, math.nan, math.inf, '2'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                MOD.positive_speed(invalid)
        for value in (.1, .5, 1, 20, 60, 120):
            self.assertEqual(MOD.positive_speed(value), float(value))

    def test_run_id_is_single_safe_directory(self):
        for invalid in ('', '..', '../old', 'a/b', 'a\\b', 'x:y', '.hidden'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                MOD.run_name(invalid)
        self.assertEqual(MOD.run_name('wh-live-100'), 'wh-live-100')

    def test_required_journal_mode_rejected_before_start(self):
        with self.assertRaisesRegex(ValueError, 'needs Neo4j'):
            MOD.WarehouseLiveRuntime(source_root=ROOT, output_root=TEMP / 'must-not-create',
                datasets=REGISTRY, dataset_id='sample19', knowledge_mode='required')

    def test_disturbance_registration_rejects_invalid_selector_and_values(self):
        good = {'at': 250, 'kind': 'soc', 'robot': 'carrying_moving', 'soc': 24, 'selection_timeout': 600}
        self.assertEqual(MOD.validate_disturbances([good]), [good])
        for changed in ({'robot': 'carried'}, {'robot': True}, {'soc': math.nan}, {'soc': True},
                        {'at': -1}, {'selection_timeout': -1}, {'kind': 'unknown'}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                MOD.validate_disturbances([{**good, **changed}])


class RunIdAvailabilityTests(unittest.TestCase):
    def owner(self, folder):
        owner = MOD.WarehouseLiveRuntime.__new__(MOD.WarehouseLiveRuntime)
        owner.output_root = Path(folder)
        owner.knowledge_backend = 'neo4j'
        owner._state = 'paused'
        owner._sim = SimpleNamespace(run_id='current-run')
        owner._create_run = Mock()
        return owner

    def test_existing_graph_id_rejected_before_creating_or_mutating_run(self):
        for label_index in range(3):
            with self.subTest(label=label_index), tempfile.TemporaryDirectory(dir=TEMP) as folder, \
                    patch('neo4j.GraphDatabase.driver') as factory:
                owner = self.owner(folder)
                driver = factory.return_value.__enter__.return_value
                session = driver.session.return_value.__enter__.return_value
                session.run.return_value.single.side_effect = [None] * label_index + [{'existing':'used-id'}]
                with self.assertRaisesRegex(ValueError, 'runId 已存在于知识图谱'):
                    owner._control('new_run', {'dataset_id':'sample19','run_id':'used-id'}, 'current-run')
                owner._create_run.assert_not_called()
                self.assertEqual(owner._sim.run_id, 'current-run')
                self.assertEqual(owner._state, 'paused')
                self.assertEqual(driver.session.call_args.kwargs['default_access_mode'], 'READ')

    def test_graph_lookup_failure_preserves_run_and_never_proceeds(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder, patch('neo4j.GraphDatabase.driver') as factory:
            owner = self.owner(folder)
            factory.side_effect = RuntimeError('database unavailable')
            with self.assertRaisesRegex(ValueError, '暂时无法检查'):
                owner._control('new_run', {'dataset_id':'sample19','run_id':'new-name'}, 'current-run')
            owner._create_run.assert_not_called()
            self.assertEqual(owner._state, 'paused')

    def test_unused_graph_id_only_performs_reads(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder, patch('neo4j.GraphDatabase.driver') as factory:
            owner = self.owner(folder)
            driver = factory.return_value.__enter__.return_value
            session = driver.session.return_value.__enter__.return_value
            session.run.return_value.single.return_value = None
            owner._check_new_run_id('new-name')
            self.assertEqual(session.run.call_count, 3)
            for call in session.run.call_args_list:
                self.assertEqual(call.kwargs, {'run_id':'new-name'})
                self.assertTrue(call.args[0].text.startswith('MATCH '))


class LiveStateTests(unittest.TestCase):
    def test_max_ticks_stops_exactly_persists_and_cannot_be_bypassed(self):
        from backend.checkpoint import read_checkpoint
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            runtime=MOD.WarehouseLiveRuntime(source_root=ROOT,output_root=folder,
                datasets=REGISTRY,dataset_id='sample19',run_id='before-limit',wall_interval=.001,
                initial_speed=120,commit_seconds=5)
            try:
                initial=runtime.command('new_run',{'dataset_id':'sample19','run_id':'limited','max_ticks':3})
                self.assertEqual(initial['maxTicks'],3)
                self.assertFalse(initial['tickLimitReached'])
                order=[]
                runtime._sim.flush_robot_graph=Mock(side_effect=lambda:order.append('flush'))
                persist=runtime._persist
                def save():
                    if runtime._sim.tick==3:order.append('persist')
                    persist()
                runtime._persist=save
                runtime.command('start')
                deadline=time.monotonic()+5
                while not runtime.latest()['tickLimitReached'] and time.monotonic()<deadline:
                    time.sleep(.01)
                capped=runtime.latest()
                self.assertEqual(capped['tick'],3)
                self.assertEqual(capped['status'],'paused')
                self.assertFalse(capped['running'])
                self.assertEqual(capped['stopReason'],'max_ticks')
                self.assertEqual(capped['knowledge']['committed_sim_time'],3)
                self.assertEqual(order,['flush','persist'])
                for action in ('start','resume','step'):
                    with self.assertRaisesRegex(ValueError,'已达到最大 tick'):
                        runtime.command(action)
                time.sleep(.03)
                self.assertEqual(runtime.latest()['tick'],3)
                cp=read_checkpoint(Path(folder)/'limited/checkpoint.json')
                self.assertEqual(cp['tick'],3)
                self.assertEqual(cp['run_metadata']['max_ticks'],3)
                result=json.loads((Path(folder)/'limited/results.json').read_text())
                self.assertEqual(result['status'],'paused')
                self.assertEqual(result['stop_reason'],'max_ticks')
                self.assertFalse(result['acceptance']['all_input_completed'])
            finally:runtime.shutdown()
            restored=MOD.WarehouseLiveRuntime(source_root=ROOT,output_root=folder,
                datasets=REGISTRY,dataset_id='sample19',resume_run_id='limited')
            try:
                self.assertEqual(restored.latest()['maxTicks'],3)
                self.assertTrue(restored.latest()['tickLimitReached'])
                with self.assertRaisesRegex(ValueError,'已达到最大 tick'):restored.command('step')
                unlimited=restored.command('new_run',{'dataset_id':'sample19','run_id':'unlimited'})
                self.assertIsNone(unlimited['maxTicks'])
                self.assertFalse(unlimited['tickLimitReached'])
                for _ in range(4):restored.command('step')
                self.assertEqual(restored.latest()['tick'],4)
                self.assertEqual(read_checkpoint(Path(folder)/'limited/checkpoint.json')['tick'],3)
            finally:restored.shutdown()

    def test_http_tick_limit_reaches_real_run_and_single_step_stops_at_one(self):
        from fastapi.testclient import TestClient
        from ops.warehouse_live import create_app
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            runtime=MOD.WarehouseLiveRuntime(source_root=ROOT,output_root=folder,
                datasets=REGISTRY,dataset_id='sample19',run_id='http-original')
            try:
                with TestClient(create_app(runtime,REGISTRY,ROOT/'dist',own_runtime=False)) as client:
                    created=client.post('/api/control',json={'action':'new_run','dataset_id':'sample19',
                        'run_id':'http-limited','max_ticks':1,'expected_run_id':'http-original'})
                    self.assertEqual(created.status_code,200)
                    self.assertEqual(created.json()['snapshot']['maxTicks'],1)
                    manifest=json.loads((Path(folder)/'http-limited/live-manifest.json').read_text())
                    self.assertEqual(manifest['max_ticks'],1)
                    stepped=client.post('/api/control',json={'action':'step','expected_run_id':'http-limited'})
                    self.assertEqual(stepped.status_code,200)
                    self.assertEqual(stepped.json()['snapshot']['tick'],1)
                    self.assertTrue(stepped.json()['snapshot']['tickLimitReached'])
                    for action in ('step','resume'):
                        rejected=client.post('/api/control',json={'action':action,'expected_run_id':'http-limited'})
                        self.assertEqual(rejected.status_code,400)
                    for limit in (0,True,-1,1.2,'10'):
                        with self.assertRaisesRegex(ValueError,'最大 tick'):
                            runtime.command('new_run',{'dataset_id':'sample19','run_id':'invalid','max_ticks':limit})
                        self.assertFalse((Path(folder)/'invalid').exists())
                    self.assertEqual(runtime.latest()['runId'],'http-limited')
                    self.assertEqual(runtime.latest()['tick'],1)
            finally:runtime.shutdown()

    def test_http_custom_id_creates_named_checkpoint_and_duplicate_preserves_current(self):
        try:
            from fastapi.testclient import TestClient
        except ImportError:
            self.skipTest('optional FastAPI HTTP integration requires requirements.txt')
        from ops.warehouse_live import create_app
        from backend.checkpoint import read_checkpoint
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            runtime = MOD.WarehouseLiveRuntime(source_root=ROOT, output_root=folder,
                datasets=REGISTRY, dataset_id='sample19', run_id='original-named-run')
            try:
                old_checkpoint = Path(folder) / 'original-named-run/checkpoint.json'
                checksum = MOD.file_sha(old_checkpoint)
                with TestClient(create_app(runtime, REGISTRY, ROOT / 'dist', own_runtime=False)) as client:
                    response = client.post('/api/control', json={'action':'new_run','dataset_id':'sample19',
                        'run_id':'page-named-run','expected_run_id':'original-named-run'})
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json()['snapshot']['runId'], 'page-named-run')
                    checkpoint = read_checkpoint(Path(folder) / 'page-named-run/checkpoint.json')
                    self.assertEqual(checkpoint['run_id'], 'page-named-run')
                    self.assertEqual(checkpoint['elapsed'], 0)
                    for name in ('page-named-run', 'original-named-run'):
                        rejected = client.post('/api/control', json={'action':'new_run','dataset_id':'sample19','run_id':name})
                        self.assertEqual(rejected.status_code, 400)
                        self.assertEqual(runtime.latest()['runId'], 'page-named-run')
                        self.assertEqual(runtime.latest()['status'], 'ready')
                    automatic = client.post('/api/control', json={'action':'new_run','dataset_id':'sample19','run_id':''})
                    self.assertEqual(automatic.status_code, 200)
                    self.assertRegex(automatic.json()['snapshot']['runId'], r'^wh-live-')
                    self.assertEqual(MOD.file_sha(old_checkpoint), checksum)
            finally:
                runtime.shutdown()

    def test_actual_state_control_new_run_and_owned_resume(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            options = dict(source_root=ROOT, output_root=folder, datasets=REGISTRY,
                           dataset_id='sample19', run_id='live-test-first')
            runtime = MOD.WarehouseLiveRuntime(**options)
            try:
                initial = runtime.latest()
                self.assertEqual(initial['status'], 'ready')
                self.assertEqual(initial['sourceKind'], 'live_simulation_memory')
                self.assertEqual(len(initial['robots']), 100)
                self.assertEqual(initial['knowledge']['durable_seq'], 1)
                self.assertFalse(initial['running'])
                instance_id = initial['runtimeInstanceId']
                first = runtime.command('step', expected_run_id=initial['runId'])
                self.assertEqual(first['elapsed'], 1)
                self.assertEqual(first['status'], 'paused')
                self.assertEqual(first['knowledge']['committed_sim_time'], 1)
                self.assertEqual(first['knowledge']['durable_seq'], 2)
                with closing(sqlite3.connect(Path(folder) / 'live-test-first/journal.sqlite')) as db:
                    self.assertEqual(db.execute('SELECT seq FROM heads').fetchone()[0], 2)
                with self.assertRaisesRegex(ValueError, 'stale run'):
                    runtime.command('step', expected_run_id='old-run')
                for action in ('reset', 'set_flow_rate', 'inject_task', 'config'):
                    with self.subTest(action=action), self.assertRaises(ValueError):
                        runtime.command(action, 100)
                for speed in (True, math.nan, 0, 121):
                    with self.assertRaises(ValueError):
                        runtime.command('speed', speed)
                changed = runtime.command('speed', 120)
                self.assertEqual(changed['speed'], 120)
                from backend.checkpoint import read_checkpoint
                cp = read_checkpoint(Path(folder) / 'live-test-first/checkpoint.json')
                self.assertEqual(cp['config']['speed'], .8)
                started = runtime.command('start')
                self.assertTrue(started['running'])
                with self.assertRaisesRegex(ValueError, 'pause'):
                    runtime.command('new_run', {'dataset_id': 'sample19'})
                deadline = time.monotonic() + 5
                while runtime.latest()['elapsed'] <= 1 and time.monotonic() < deadline:
                    time.sleep(.01)
                paused = runtime.command('pause')
                self.assertGreater(paused['elapsed'], 1)
                time.sleep(.03)
                self.assertEqual(runtime.latest()['elapsed'], paused['elapsed'])
                old_checkpoint = Path(folder) / 'live-test-first/checkpoint.json'
                checksum = MOD.file_sha(old_checkpoint)
                with self.assertRaisesRegex(ValueError, 'unknown'):
                    runtime.command('new_run', {'dataset_id': 'unregistered'})
                self.assertEqual(runtime.latest()['runId'], 'live-test-first')
                second = runtime.command('new_run', {'dataset_id': 'sample19', 'run_id': 'live-test-second'})
                self.assertEqual(second['runId'], 'live-test-second')
                self.assertEqual(second['elapsed'], 0)
                self.assertEqual(second['runtimeInstanceId'], instance_id)
                self.assertGreater(second['liveRevision'], paused['liveRevision'])
                self.assertEqual(MOD.file_sha(old_checkpoint), checksum)
                runtime.command('step')
            finally:
                runtime.shutdown()
            restored = MOD.WarehouseLiveRuntime(source_root=ROOT, output_root=folder,
                datasets=REGISTRY, dataset_id='sample19', resume_run_id='live-test-second')
            try:
                self.assertEqual(restored.latest()['elapsed'], 1)
                self.assertEqual(restored.latest()['status'], 'paused')
                self.assertNotEqual(restored.latest()['runtimeInstanceId'], instance_id)
                self.assertEqual(restored.command('step')['elapsed'], 2)
            finally:
                restored.shutdown()

    def test_failed_step_is_frozen_and_requires_explicit_new_run(self):
        class FailingRuntime(MOD.WarehouseLiveRuntime):
            def _advance_one(self, force_persist=False):
                self._sim.robots[1]['x'] += 1000
                raise ValueError('diagnostic partial step failure')
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            runtime = FailingRuntime(source_root=ROOT, output_root=folder, datasets=REGISTRY,
                                     dataset_id='sample19', run_id='live-failed')
            before = runtime.latest()
            try:
                with self.assertRaisesRegex(RuntimeError, 'partial step'):
                    runtime.command('step')
                after = runtime.latest()
                self.assertEqual(after['status'], 'failed')
                self.assertEqual(after['robots'], before['robots'])
                self.assertEqual(after['elapsed'], 0)
                self.assertTrue(list((Path(folder) / 'live-failed').glob('failure-*.json')))
                with self.assertRaises(ValueError):
                    runtime.command('resume')
                with self.assertRaises(ValueError):
                    runtime.command('speed', 2)
                new = runtime.command('new_run', {'dataset_id': 'sample19'})
                self.assertEqual(new['status'], 'ready')
                self.assertNotEqual(new['runId'], 'live-failed')
            finally:
                runtime.shutdown()
            with self.assertRaisesRegex(ValueError, 'reconciliation'):
                MOD.WarehouseLiveRuntime(source_root=ROOT, output_root=folder, datasets=REGISTRY,
                    dataset_id='sample19', resume_run_id='live-failed')


if __name__ == '__main__':
    unittest.main()
