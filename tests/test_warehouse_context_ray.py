"""Ray scheduler/affinity tests; optional real Actors use a stub graph only."""
from dataclasses import replace
from functools import partial
import io
import json
import os
from pathlib import Path
import signal
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from backend.robot_operation_context import ContextOptions
from scripts import warehouse_llm_context_queries_ray as runner
from tests.test_warehouse_context_multiprocess import make_stub_driver


def benchmark_args(output, *, workers=2, repeat=3):
    return SimpleNamespace(uri='bolt://stub.invalid', user='neo4j', password='TEST_PRIVATE_VALUE',
        database='neo4j', transaction_timeout=5.0, max_connection_pool_size=1,
        max_workers=workers, repeat=repeat, cpu_ids=None, startup_timeout=30.0,
        output_dir=output, print_context=False, progress_every=0)


class FakeRef:
    def __init__(self, kind, actor, payload=None):
        self.kind, self.actor, self.payload = kind, actor, payload


class FakeMethod:
    def __init__(self, actor, kind):
        self.actor, self.kind = actor, kind

    def remote(self, *args):
        if self.kind == 'execute':
            assert self.actor.ray.all_ready, 'A query started before all actors were ready'
            assert not self.actor.pending, 'Multiple jobs queued for one actor'
            self.actor.pending = True
            self.actor.ray.submissions.append((self.actor.index, args[1]))
        return FakeRef(self.kind, self.actor, args)


class FakeActor:
    def __init__(self, ray, index, cpu):
        self.ray, self.index, self.cpu = ray, index, cpu
        self.pending, self.closed = False, False
        self.ready, self.execute, self.close = (FakeMethod(self, kind) for kind in ('ready', 'execute', 'close'))


class FakeRay:
    def __init__(self, *, initialized=False, fail_ready=False, fail_jobs=()):
        self.initialized, self.fail_ready = initialized, fail_ready
        self.fail_jobs = set(fail_jobs)
        self.all_ready = False
        self.actors, self.submissions, self.killed = [], [], []
        self.shutdown_calls = 0

    def is_initialized(self):
        return self.initialized

    def init(self, **kwargs):
        assert kwargs['address'] == 'local'
        self.init_kwargs, self.initialized = kwargs, True

    def remote(self, **options):
        self.actor_options = options
        ray = self

        class ActorBuilder:
            def remote(self, index, cpu, connection, factory):
                actor = FakeActor(ray, index, cpu)
                ray.actors.append(actor)
                return actor

        return lambda cls: ActorBuilder()

    def get(self, refs, timeout=None):
        if isinstance(refs, list):
            if refs and all(ref.kind == 'ready' for ref in refs):
                if self.fail_ready:
                    raise RuntimeError('startup failed')
                result = [self.get(ref) for ref in refs]
                self.all_ready = True
                return result
            return [self.get(ref) for ref in refs]
        actor = refs.actor
        if refs.kind == 'ready':
            return dict(worker_index=actor.index, worker_pid=1000+actor.index,
                cpu_id=actor.cpu, affinity_cpus=[actor.cpu], physical_socket=0,
                physical_core=actor.cpu, ray_assigned_resources={'CPU': 1}, pinned_thread_count=4)
        if refs.kind == 'close':
            actor.closed = True
            return None
        options, index, repeat, submitted = refs.payload
        actor.pending = False
        if index in self.fail_jobs:
            raise RuntimeError('actor lost')
        context = dict(context_index=index, run_id=options.run_id, robot_id=options.robot_id,
            tick=10, dataset_id='dataset:stub', repeat_index=repeat, status='ok', queries={},
            elapsed_ms=1.0, worker_pid=1000+actor.index, elapsed_measured=True)
        record = dict(query_id='w0', name='basis', context_index=index, run_id=options.run_id,
            robot_id=options.robot_id, status='ok', elapsed_ms=1.0, row_count=1,
            worker_pid=1000+actor.index, repeat_index=repeat)
        return context, [record]

    def wait(self, refs, num_returns=1):
        return refs[:num_returns], refs[num_returns:]

    def kill(self, actor, no_restart):
        assert no_restart
        self.killed.append(actor.index)

    def shutdown(self):
        self.initialized = False
        self.shutdown_calls += 1

    def get_runtime_context(self):
        return SimpleNamespace(get_assigned_resources=lambda: {'CPU': 1})


class RayTopologyTests(unittest.TestCase):
    def test_cpu_selection_rejects_smt_siblings_and_disallowed_cpus(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for cpu, core in [(0, 0), (1, 1), (8, 0)]:
                path = root / f'cpu{cpu}' / 'topology'
                path.mkdir(parents=True)
                (path / 'physical_package_id').write_text('0')
                (path / 'core_id').write_text(str(core))
            self.assertEqual(runner.choose_cpu_ids(2, allowed_cpus={0, 1, 8}, topology_root=root), [0, 1])
            for raw in ['0,8', '0,99', '0', '0,0']:
                with self.subTest(raw=raw), self.assertRaises(ValueError):
                    runner.choose_cpu_ids(2, raw, allowed_cpus={0, 1, 8}, topology_root=root)
            with self.assertRaises(ValueError):
                runner.choose_cpu_ids(3, allowed_cpus={0, 1, 8}, topology_root=root)

    def test_cpu_parser_rejects_bad_and_extreme_ranges(self):
        self.assertEqual(runner.parse_cpu_ids('0-2,8'), [0, 1, 2, 8])
        for raw in ['-1', '3-1', '0-1000000000', '', '0,']:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                runner.parse_cpu_ids(raw)

    def test_thread_pinning_targets_all_existing_threads_without_changing_test_affinity(self):
        with patch.object(runner.os, 'sched_setaffinity') as set_mask, \
             patch.object(runner.os, 'sched_getaffinity', return_value={3}):
            count = runner.pin_actor_threads(3)
        self.assertGreater(count, 0)
        self.assertGreaterEqual(set_mask.call_count, count)
        self.assertTrue(all(call.args[1] == {3} for call in set_mask.call_args_list))


class RayActorTests(unittest.TestCase):
    def test_actor_reuses_own_driver_auto_commits_and_closes(self):
        connection = runner.ProcessConnection('bolt://stub', 'neo4j', 'TEST_PRIVATE_VALUE')
        driver = make_stub_driver(connection)
        driver.close = Mock(wraps=driver.close)
        factory = Mock(return_value=driver)
        with patch.object(runner, 'pin_actor_threads', return_value=4), \
             patch.object(runner, 'physical_core', return_value=(0, 3)), \
             patch.object(runner.os, 'sched_getaffinity', return_value={3}), \
             patch.object(runner, 'load_ray', return_value=FakeRay()):
            actor = runner.RayContextWorker(0, 3, connection, factory)
            self.assertEqual(actor.ready()['affinity_cpus'], [3])
            for index in range(2):
                context, records = actor.execute(ContextOptions('r'), index, index, time.perf_counter())
                self.assertEqual(context['status'], 'ok')
                self.assertEqual(context['worker_pid'], os.getpid())
                self.assertEqual(context['cpu_id'], 3)
                self.assertEqual([r['status'] for r in records], ['ok', 'ok'])
                self.assertTrue(all(r['transaction_mode'] == 'auto_commit_per_query' for r in records))
            actor.close()
            actor.close()
        factory.assert_called_once_with(connection)
        driver.close.assert_called_once()

    def test_failed_connectivity_closes_the_actor_driver(self):
        driver = Mock()
        driver.verify_connectivity.side_effect = RuntimeError('unreachable')
        with patch.object(runner, 'pin_actor_threads', return_value=1), self.assertRaises(RuntimeError):
            runner.RayContextWorker(0, 3, runner.ProcessConnection('bolt://stub', 'n', 'p'), Mock(return_value=driver))
        driver.close.assert_called_once()


class RaySchedulerTests(unittest.TestCase):
    def run_fake(self, root, ray):
        with patch.object(runner, 'choose_cpu_ids', return_value=[0, 1]):
            return runner.run_benchmark(ContextOptions('r'), [1, 2], benchmark_args(root / 'output'), ray_module=ray)

    def test_all_ready_before_queries_one_inflight_per_actor_repeat_and_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            root, ray = Path(folder), FakeRay()
            summary = self.run_fake(root, ray)
            self.assertEqual(summary['status'], 'ok')
            self.assertEqual(summary['contexts'], 6)
            self.assertEqual(summary['started_worker_count'], 2)
            self.assertEqual(summary['observed_worker_count'], 2)
            self.assertEqual(summary['total_queries'], 6)
            self.assertEqual(summary['success_count'], 6)
            self.assertEqual(summary['by_query']['w0']['avg_rows'], 1.0)
            self.assertEqual(ray.actor_options, dict(num_cpus=1, max_concurrency=1, max_restarts=0, max_task_retries=0))
            self.assertEqual(len(ray.submissions), 6)
            self.assertTrue(all(actor.closed for actor in ray.actors))
            self.assertEqual(ray.killed, [0, 1])
            self.assertEqual(ray.shutdown_calls, 1)
            contexts = [json.loads(line) for line in (root / 'output/contexts.jsonl').read_text().splitlines()]
            self.assertEqual({(c['robot_id'], c['repeat_index']) for c in contexts}, {(r, n) for r in [1, 2] for n in range(3)})
            for name in ['contexts.jsonl', 'queries.jsonl', 'summary.json']:
                self.assertNotIn('TEST_PRIVATE_VALUE', (root / 'output' / name).read_text())
            saved = json.loads((root / 'output/summary.json').read_text())
            self.assertEqual(saved['total_queries'], 6)
            self.assertIn('by_category', saved)

    def test_failed_actor_is_retired_and_other_actor_finishes_remaining_jobs(self):
        with tempfile.TemporaryDirectory() as folder:
            root, ray = Path(folder), FakeRay(fail_jobs={1})
            summary = self.run_fake(root, ray)
            self.assertEqual(summary['contexts'], 6)
            self.assertEqual(summary['context_statuses'], dict(ok=5, error=1))
            self.assertEqual(summary['measured_contexts'], 5)
            self.assertEqual(summary['status'], 'error')
            failed_actor_submissions = [index for actor, index in ray.submissions if actor == 1]
            self.assertEqual(failed_actor_submissions, [1])

    def test_all_lost_actors_mark_unassigned_jobs_failed_without_fake_timings(self):
        with tempfile.TemporaryDirectory() as folder:
            ray = FakeRay(fail_jobs={0, 1})
            summary = self.run_fake(Path(folder), ray)
            self.assertEqual(summary['contexts'], 6)
            self.assertEqual(summary['context_statuses'], dict(error=6))
            self.assertEqual(summary['measured_contexts'], 0)
            self.assertIsNone(summary['context_p50_ms'])
            self.assertEqual(summary['run_error']['error_type'], 'NoAvailableActors')
            self.assertEqual(len(ray.submissions), 2)
            self.assertEqual(ray.shutdown_calls, 1)

    def test_startup_failure_still_closes_actors_and_writes_failure_summary(self):
        with tempfile.TemporaryDirectory() as folder:
            root, ray = Path(folder), FakeRay(fail_ready=True)
            summary = self.run_fake(root, ray)
            self.assertEqual(summary['status'], 'error')
            self.assertIn('startup failed', summary['run_error']['error'])
            self.assertEqual(ray.submissions, [])
            self.assertTrue(all(actor.closed for actor in ray.actors))
            self.assertEqual(ray.shutdown_calls, 1)
            self.assertTrue((root / 'output/summary.json').exists())

    def test_existing_runtime_is_not_used_or_stopped(self):
        ray = FakeRay(initialized=True)
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(RuntimeError):
            self.run_fake(Path(folder), ray)
        self.assertEqual(ray.shutdown_calls, 0)
        self.assertEqual(ray.killed, [])

    def test_cli_dry_run_needs_no_ray_or_password(self):
        stream = io.StringIO()
        with patch.object(runner, 'load_ray', side_effect=AssertionError('offline')), patch('sys.stdout', stream):
            self.assertEqual(runner.main(['--run-id', 'r', '--queries', 'all', '--repeat', '5',
                                         '--password', 'TEST_PRIVATE_VALUE', '--dry-run']), 0)
        plan = json.loads(stream.getvalue())
        self.assertEqual(plan['planned_contexts'], 500)
        self.assertEqual(plan['max_workers'], 100)
        self.assertEqual(len(plan['queries']), 14)
        self.assertNotIn('TEST_PRIVATE_VALUE', stream.getvalue())


class FinalOutputTests(unittest.TestCase):
    def sample(self):
        records = [
            dict(query_id='w0', status='ok', elapsed_ms=10.0, row_count=2),
            dict(query_id='w0', status='ok', elapsed_ms=30.0, row_count=4),
            dict(query_id='w0', status='error', elapsed_ms=900.0, row_count=0),
            dict(query_id='w0', status='skipped', elapsed_ms=0.0, row_count=0),
            dict(query_id='w10', status='skipped', elapsed_ms=0.0, row_count=0)]
        contexts = [dict(elapsed_ms=100.0, elapsed_measured=True),
                    dict(elapsed_ms=300.0, elapsed_measured=True),
                    dict(elapsed_ms=0.0, elapsed_measured=False)]
        return runner.final_output_metrics(records, contexts, 2000.0)

    def test_statistics_match_lade_success_latency_scope_and_exclude_skips(self):
        stats = self.sample()
        self.assertEqual(stats['total_queries'], 3)
        self.assertEqual(stats['success_count'], 2)
        self.assertEqual(stats['failure_count'], 1)
        self.assertEqual(stats['skipped_count'], 2)
        self.assertEqual(stats['throughput_qps'], 1.5)
        self.assertEqual(stats['query_avg_ms'], 20.0)
        self.assertEqual(stats['query_p95_ms'], 29.0)
        self.assertEqual(stats['query_p99_ms'], 29.8)
        self.assertEqual(stats['context_avg_ms'], 200.0)
        self.assertEqual(stats['context_p95_ms'], 290.0)
        self.assertEqual(stats['by_query']['w0']['avg_rows'], 3.0)
        self.assertEqual(stats['by_query']['w10']['total_queries'], 0)
        self.assertIsNone(stats['by_query']['w10']['avg_ms'])
        self.assertEqual(stats['by_category']['self']['failure_count'], 1)

    def test_main_prints_lade_style_human_summary_and_log_paths(self):
        summary = dict(self.sample(), status='error', catalog_version='test-catalog',
            run_id='run-display', database='neo4j', planned_contexts=3,
            started_worker_count=2, observed_worker_count=2, wall_ms=2000.0,
            context_statuses={'ok': 2, 'error': 1}, mode='read',
            ray_startup_ms=10.0, query_phase_wall_ms=1980.0, ray_cleanup_ms=10.0,
            output_dir='/tmp/warehouse-ray-display')
        stream = io.StringIO()
        with patch.object(runner, 'run_benchmark', return_value=summary), patch('sys.stdout', stream):
            code = runner.main(['--run-id', 'run-display', '--max-workers', '2'])
        text = stream.getvalue()
        self.assertEqual(code, 1)
        self.assertIn('Warehouse LLM 前置上下文查询结果', text)
        self.assertIn('总查询数: 3', text)
        self.assertIn('跳过: 2', text)
        self.assertIn('单查询 avg/p95/p99: 20.000 / 29.000 / 29.800 ms', text)
        self.assertIn('rows_avg=3.000', text)
        self.assertLess(text.index('w0:'), text.index('w10:'))
        self.assertIn('/tmp/warehouse-ray-display/summary.json', text)
        self.assertNotIn('"worker_info"', text)
        self.assertNotIn(runner.DEFAULT_PASSWORD, text)

    def test_empty_success_samples_and_startup_errors_print_na_without_crashing(self):
        stats = runner.final_output_metrics([
            dict(query_id='w0', status='error', elapsed_ms=5.0, row_count=0)], [], 0.0)
        self.assertIsNone(stats['query_p99_ms'])
        self.assertIsNone(stats['throughput_qps'])
        self.assertIsNone(stats['context_avg_ms'])
        stream = io.StringIO()
        with patch('sys.stdout', stream):
            runner.print_summary(dict(stats, status='error', wall_ms=0.0,
                run_error=dict(error_type='StartupError', error='actor not ready')))
        self.assertIn('N/A', stream.getvalue())
        self.assertIn('StartupError: actor not ready', stream.getvalue())


@unittest.skipUnless(os.environ.get('WAREHOUSE_CONTEXT_RAY_TEST') == '1', '设置 WAREHOUSE_CONTEXT_RAY_TEST=1 运行真实双Actor测试')
class RealRayTests(unittest.TestCase):
    def test_two_real_actors_have_distinct_pids_cores_and_close_drivers(self):
        def timed_out(*_):
            raise TimeoutError('isolated Ray test exceeded 60 seconds')

        previous = signal.signal(signal.SIGALRM, timed_out)
        signal.alarm(60)
        try:
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                close_dir = root / 'closed'
                close_dir.mkdir()
                summary = runner.run_benchmark(ContextOptions('stub-run'), [1, 2],
                    benchmark_args(root / 'output'), driver_factory=partial(make_stub_driver, close_dir=str(close_dir)))
                self.assertEqual(summary['status'], 'ok', summary)
                self.assertEqual(summary['contexts'], 6)
                self.assertEqual(summary['query_statuses'], {'ok': 12})
                self.assertEqual(summary['started_worker_count'], 2)
                self.assertEqual(summary['observed_worker_count'], 2)
                self.assertNotIn(os.getpid(), summary['worker_pids'])
                self.assertEqual(len({(w['physical_socket'], w['physical_core']) for w in summary['worker_info']}), 2)
                self.assertTrue(all(w['affinity_cpus'] == [w['cpu_id']] for w in summary['worker_info']))
                self.assertTrue(all(w['ray_assigned_resources']['CPU'] == 1 for w in summary['worker_info']))
                self.assertEqual(len(list(close_dir.iterdir())), 2)
                self.assertEqual(summary['cleanup_errors'], [])
                self.assertFalse(runner.load_ray().is_initialized())
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, previous)


if __name__ == '__main__':
    unittest.main()
