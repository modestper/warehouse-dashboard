"""New process runner: real spawn isolation with a stub graph, no Neo4j writes."""
from concurrent.futures import Future
from functools import partial
import io
import json
import multiprocessing as mp
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

from backend.robot_operation_context import ContextOptions
from scripts import warehouse_llm_context_queries_multiprocess as runner


class StubResult:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def consume(self):
        return SimpleNamespace(result_available_after=1, result_consumed_after=1)


class StubSession:
    def __init__(self, driver, options):
        self.driver = driver
        self.options = options

    def __enter__(self):
        assert self.options['default_access_mode'] == 'READ'
        return self

    def __exit__(self, *_):
        pass

    def begin_transaction(self, **_):
        raise AssertionError('Each process must retain auto-commit semantics')

    def run(self, query, **params):
        assert self.driver.pid == os.getpid(), 'Driver crossed process boundary'
        if str(query).startswith('EXPLAIN '):
            return StubResult([])
        if 'at_tick' in params:
            tick = params['at_tick'] if params['at_tick'] is not None else 10
            rid = params['robot_id']
            return StubResult([dict(run_id=params['run_id'], t=tick, published_tick=tick, step_count=1,
                step=dict(tick=tick, complete=True, expected_robot_count=2, received_robot_count=2, elapsed=tick),
                datasets=[dict(id='dataset:stub')], robot_type='X3',
                self_state=dict(tick=tick, stage='idle', battery=80, x=rid, y=0, task_id=None, cargo_id=None),
                state_age_ticks=0, config_json='{"charge_request_soc":35}', observation_ready=True,
                stub_driver_id=self.driver.instance_id)])
        if 'from_tick' in params:
            return StubResult([dict(states=[
                dict(tick=1, stage='idle', battery=90, x=0, y=0),
                dict(tick=params['t'], stage='idle', battery=80, x=params['robot_id'], y=0)], events=[])])
        raise AssertionError('Unexpected stub query')


class StubDriver:
    def __init__(self, *, barrier=None, close_dir=None, fail=False):
        self.pid = os.getpid()
        self.instance_id = f'{self.pid}-{uuid.uuid4().hex}'
        self.barrier, self.close_dir, self.fail = barrier, close_dir, fail

    def verify_connectivity(self):
        assert self.pid == os.getpid()
        if self.fail:
            raise RuntimeError('stub connection unavailable')
        if self.barrier is not None:
            self.barrier.wait(timeout=20)

    def session(self, **options):
        assert self.pid == os.getpid()
        return StubSession(self, options)

    def close(self):
        assert self.pid == os.getpid()
        if self.close_dir:
            (Path(self.close_dir) / ('closed-' + self.instance_id)).write_text('closed', encoding='utf-8')


def make_stub_driver(connection, *, barrier=None, close_dir=None, fail=False):
    assert connection.max_connection_pool_size == 1
    return StubDriver(barrier=barrier, close_dir=close_dir, fail=fail)


def benchmark_args(output, *, workers=2, repeat=3):
    return SimpleNamespace(uri='bolt://stub.invalid', user='neo4j', password='TEST_PRIVATE_VALUE',
        database='neo4j', transaction_timeout=5.0, max_connection_pool_size=1,
        max_workers=workers, repeat=repeat, output_dir=output, print_context=False)


class ProcessRunnerUnitTests(unittest.TestCase):
    def tearDown(self):
        runner._close_process_driver()
        if runner._FINALIZER is not None:
            runner._FINALIZER.cancel()

    def test_dry_run_exposes_100_process_plan_without_connecting_or_starting_pool(self):
        stream = io.StringIO()
        with patch.object(runner, 'ProcessPoolExecutor', side_effect=AssertionError('must stay offline')), \
             patch('sys.stdout', stream):
            code = runner.main(['--run-id', 'r', '--robot-ids', '1-100', '--repeat', '5',
                                '--queries', 'all', '--password', 'TEST_PRIVATE_VALUE', '--dry-run'])
        self.assertEqual(code, 0)
        plan = json.loads(stream.getvalue())
        self.assertEqual(plan['max_workers'], 100)
        self.assertEqual(plan['effective_process_limit'], 100)
        self.assertEqual(plan['planned_contexts'], 500)
        self.assertEqual(plan['connection_pool_size_per_process'], 1)
        self.assertEqual(len(plan['queries']), 14)
        self.assertNotIn('TEST_PRIVATE_VALUE', stream.getvalue())

    def test_invalid_scope_override_is_rejected_before_starting_pool(self):
        with patch('sys.stderr', io.StringIO()), self.assertRaises(SystemExit):
            runner.parse_args(['--run-id', 'r', '--param', 't=999'])

    def test_missing_password_fails_in_parent_without_starting_workers(self):
        with patch.object(runner, 'ProcessPoolExecutor', side_effect=AssertionError('no pool')), \
             patch('sys.stdin.isatty', return_value=False), patch('sys.stderr', io.StringIO()):
            self.assertEqual(runner.main(['--run-id', 'r', '--password', '']), 2)

    def test_worker_driver_is_created_once_and_closed_by_its_owner(self):
        driver = Mock()
        connection = runner.ProcessConnection('bolt://stub', 'neo4j', 'TEST_PRIVATE_VALUE')
        self.assertNotIn('TEST_PRIVATE_VALUE', repr(connection))
        with patch('neo4j.GraphDatabase.driver', return_value=driver) as create:
            runner._initialize_process(connection)
            self.assertIs(runner._get_process_driver(), driver)
            self.assertIs(runner._get_process_driver(), driver)
        create.assert_called_once()
        self.assertEqual(create.call_args.kwargs['max_connection_pool_size'], 1)
        driver.verify_connectivity.assert_called_once()
        runner._close_process_driver()
        driver.close.assert_called_once()

    def test_connectivity_failure_closes_driver_and_does_not_cache_it(self):
        driver = Mock()
        driver.verify_connectivity.side_effect = RuntimeError('unreachable')
        with patch.object(runner, '_make_driver', return_value=driver):
            runner._initialize_process(runner.ProcessConnection('bolt://stub', 'neo4j', 'secret'))
            with self.assertRaisesRegex(RuntimeError, 'unreachable'):
                runner._get_process_driver()
        self.assertIsNone(runner._PROCESS_DRIVER)
        driver.close.assert_called_once()

    def test_existing_output_is_rejected_before_creating_process_pool(self):
        with tempfile.TemporaryDirectory() as folder, \
             patch.object(runner, 'ProcessPoolExecutor') as pool:
            with self.assertRaises(FileExistsError):
                runner.run_benchmark(ContextOptions('r'), [1], benchmark_args(Path(folder)))
            pool.assert_not_called()

    def test_lost_worker_and_submit_failures_are_logged_without_inventing_a_pid(self):
        for fail_during_submit in (False, True):
            with self.subTest(fail_during_submit=fail_during_submit), tempfile.TemporaryDirectory() as folder:
                class StubPool:
                    def __init__(self, **kwargs):
                        self.kwargs = kwargs
                        self.assert_spawn = kwargs['mp_context'].get_start_method() == 'spawn'

                    def __enter__(self):
                        return self

                    def __exit__(self, *_):
                        pass

                    def submit(self, fn, options, index, repeat_index, submitted):
                        future = Future()
                        if index == 0:
                            if fail_during_submit:
                                raise RuntimeError('pool unavailable')
                            future.set_exception(RuntimeError('worker disappeared'))
                        else:
                            context = dict(run_id=options.run_id, robot_id=options.robot_id,
                                context_index=index, repeat_index=repeat_index, status='ok', queries={},
                                elapsed_ms=7.0, worker_pid=12345, elapsed_measured=True, driver_initialized=True)
                            future.set_result((context, []))
                        return future

                output = Path(folder) / 'new'
                with patch.object(runner, 'ProcessPoolExecutor', StubPool):
                    summary = runner.run_benchmark(ContextOptions('r'), [1, 2], benchmark_args(output, repeat=1))
                self.assertEqual(summary['contexts'], 2)
                self.assertEqual(summary['context_statuses'], dict(error=1, ok=1))
                self.assertEqual(summary['measured_contexts'], 1)
                self.assertEqual(summary['context_p50_ms'], 7.0)
                contexts = [json.loads(line) for line in (output / 'contexts.jsonl').read_text().splitlines()]
                failed = next(row for row in contexts if row['status'] == 'error')
                self.assertIsNone(failed['worker_pid'])
                self.assertFalse(failed['elapsed_measured'])


class ProcessRunnerSpawnTests(unittest.TestCase):
    def test_real_spawn_isolates_drivers_repeats_logs_and_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            close_dir = root / 'closed'
            close_dir.mkdir()
            barrier = mp.get_context('spawn').Barrier(2)
            factory = partial(make_stub_driver, barrier=barrier, close_dir=str(close_dir))
            output = root / 'results'
            summary = runner.run_benchmark(ContextOptions('stub-run', selection='core'), [1, 2],
                                          benchmark_args(output), driver_factory=factory)
            self.assertEqual(summary['contexts'], 6)
            self.assertEqual(summary['context_statuses'], dict(ok=6))
            self.assertEqual(summary['process_count'], 2)
            self.assertNotIn(os.getpid(), summary['worker_pids'])
            self.assertEqual(summary['query_statuses'], dict(ok=12))
            contexts = [json.loads(line) for line in (output / 'contexts.jsonl').read_text().splitlines()]
            queries = [json.loads(line) for line in (output / 'queries.jsonl').read_text().splitlines()]
            self.assertEqual({(c['robot_id'], c['repeat_index']) for c in contexts},
                             {(r, n) for r in [1, 2] for n in range(3)})
            self.assertEqual(len({c['context_index'] for c in contexts}), 6)
            for pid in summary['worker_pids']:
                assigned = [c for c in contexts if c['worker_pid'] == pid]
                self.assertEqual(len({c['queries']['w0']['rows'][0]['stub_driver_id'] for c in assigned}), 1)
                self.assertEqual(sum(c['driver_initialized'] for c in assigned), 1)
                self.assertEqual(summary['processes'][str(pid)]['driver_initializations'], 1)
            self.assertTrue(all(q['worker_pid'] in summary['worker_pids'] for q in queries))
            self.assertTrue(all(q['transaction_mode'] == 'auto_commit_per_query' for q in queries))
            self.assertEqual(len(list(close_dir.iterdir())), 2)
            for name in ['contexts.jsonl', 'queries.jsonl', 'summary.json']:
                self.assertNotIn('TEST_PRIVATE_VALUE', (output / name).read_text())

    def test_child_connection_failures_are_recorded_per_context(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            close_dir = root / 'closed'
            close_dir.mkdir()
            output = root / 'results'
            summary = runner.run_benchmark(ContextOptions('stub-run'), [1],
                benchmark_args(output, workers=1, repeat=2),
                driver_factory=partial(make_stub_driver, fail=True, close_dir=str(close_dir)))
            self.assertEqual(summary['contexts'], 2)
            self.assertEqual(summary['context_statuses'], dict(error=2))
            self.assertEqual(summary['queries'], {})
            self.assertEqual(summary['process_count'], 1)
            self.assertEqual(len(list(close_dir.iterdir())), 2)
            contexts = [json.loads(line) for line in (output / 'contexts.jsonl').read_text().splitlines()]
            self.assertTrue(all('stub connection unavailable' in c['error'] for c in contexts))
            self.assertTrue(all(not c['driver_initialized'] for c in contexts))


if __name__ == '__main__':
    unittest.main()
