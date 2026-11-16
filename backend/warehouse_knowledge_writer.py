"""Acknowledged write routing inspired by LaDe's knowledge_writer.

Only plain payloads cross process/Ray boundaries. Drivers are constructed in
the write-owning process. Robot execution remains on Ray in every write mode.
"""
from copy import deepcopy
import multiprocessing
import os
from pathlib import Path
import time

from ops.warehouse_kg_config import WRITE_MODES


class WriteExecutor:
    def __init__(self, connection):
        self.connection = dict(connection)
        self.robot_writer = self.global_writer = self.store = None

    def apply(self, method, *args):
        if method == 'bootstrap':
            from .warehouse_runtime_graph import Neo4jRuntimeGraphWriter
            config, run, context_queries = args
            if self.robot_writer is not None:
                self.robot_writer.close()
            self.robot_writer = Neo4jRuntimeGraphWriter(**config)
            self.robot_writer.ensure_schema()
            self.robot_writer.ensure_run(run)
            if context_queries:
                self.robot_writer.ensure_context_schema()
            return None
        if method == 'robots':
            snapshots, = args
            if self.robot_writer is None:
                raise RuntimeError('robot writer is not bootstrapped')
            ticks = {snapshot['tick'] for snapshot in snapshots.values()}
            if len(ticks) != 1:
                raise ValueError('robot frame must contain one tick')
            for snapshot in snapshots.values():
                self.robot_writer.record_robot_snapshot(snapshot)
                self.robot_writer.record_robot_current_state(snapshot, snapshot['current_robot'])
            return {'tick': ticks.pop(), 'robots': sorted(snapshots)}
        if method == 'global':
            from .global_writer import GlobalWriter
            if self.global_writer is None:
                self.global_writer = GlobalWriter(**self.connection)
            self.global_writer.write(args[0])
            return None
        if method == 'append':
            from .neo4j_live_store import Neo4jLiveStore
            if self.store is None:
                self.store = Neo4jLiveStore(**self.connection)
            self.store.append(*args)
            return self.store.status(args[0]['run_id'])
        if method == 'close':
            self.close()
            return None
        raise ValueError('unsupported knowledge write: ' + method)

    def close(self):
        errors = []
        for name in ('robot_writer', 'global_writer', 'store'):
            writer = getattr(self, name)
            if writer is not None:
                try:
                    writer.close()
                except Exception as exc:
                    errors.append(exc)
                finally:
                    setattr(self, name, None)
        if errors:
            raise errors[0]


def process_worker(pipe, connection, executor_class):
    executor = executor_class(connection)
    try:
        pipe.send({'ok': True, 'pid': os.getpid()})
        while True:
            try:
                method, args = pipe.recv()
            except EOFError:
                break
            try:
                result = executor.apply(method, *args)
                pipe.send({'ok': True, 'pid': os.getpid(), 'result': result})
            except Exception as exc:
                pipe.send({'ok': False, 'pid': os.getpid(), 'error': f'{type(exc).__name__}: {exc}'})
            if method == 'close':
                break
    finally:
        executor.close()
        pipe.close()


class RayWriteActor:
    def __init__(self, connection, executor_class=WriteExecutor):
        self.executor = executor_class(connection)

    def apply(self, method, args):
        return {'pid': os.getpid(), 'result': self.executor.apply(method, *args)}


class WarehouseKnowledgeWriter:
    """The live store and the per-step writer share one write-owning route.

    Main/process routes own robot and global writes. In ray mode each robot
    actor writes itself; this dedicated actor owns global facts and commits.
    Calls are acknowledged, bounded to one in flight, and failures propagate.
    """
    mirror = None

    def __init__(self, mode, connection, *, ray_address=None, ray_num_cpus=None,
                 timeout=120., executor_class=WriteExecutor):
        if mode not in WRITE_MODES:
            raise ValueError('KG_WRITE_MODE must be main, ray or process')
        self.mode, self.connection = mode, dict(connection)
        self.ray_address, self.ray_num_cpus = ray_address, ray_num_cpus
        self.timeout, self.executor_class = timeout, executor_class
        self.executor = self.process = self.pipe = self.actor = self.ray = None
        self.pid = None
        self.closed = False
        self.failure = None
        self.last_error = None
        self._statuses = {}
        self.timings = {'journal_seconds': 0., 'mirror_seconds': 0., 'commits': 0}

    def _receive(self):
        if not self.pipe.poll(self.timeout):
            self.failure = RuntimeError('knowledge worker acknowledgement timed out')
            raise self.failure
        try:
            response = self.pipe.recv()
        except (EOFError, OSError) as exc:
            self.failure = RuntimeError('knowledge worker exited before acknowledgement')
            raise self.failure from exc
        self.pid = response['pid']
        if not response['ok']:
            raise RuntimeError(response['error'])
        return response.get('result')

    def _ensure(self):
        if self.closed:
            raise RuntimeError('knowledge writer is closed')
        if self.failure is not None:
            raise self.failure
        if self.pid is not None:
            return
        if self.mode == 'main':
            self.executor = self.executor_class(self.connection)
            self.pid = os.getpid()
        elif self.mode == 'process':
            context = multiprocessing.get_context('spawn')
            self.pipe, child_pipe = context.Pipe()
            self.process = context.Process(target=process_worker,
                args=(child_pipe, self.connection, self.executor_class), name='warehouse-knowledge-writer')
            try:
                self.process.start()
            except BaseException:
                self.pipe.close()
                child_pipe.close()
                self.process = None
                raise
            child_pipe.close()
            self._receive()
        else:
            import ray
            self.ray = ray
            if not ray.is_initialized():
                options = {'address': self.ray_address, 'namespace': 'warehouse-robot-actors',
                           'include_dashboard': False, 'log_to_driver': False,
                           'runtime_env': {'env_vars': {'PYTHONPATH': str(Path(__file__).resolve().parents[1])}}}
                if self.ray_num_cpus is not None:
                    options['num_cpus'] = self.ray_num_cpus
                ray.init(**options)
            self.actor = ray.remote(num_cpus=0)(RayWriteActor).remote(self.connection, self.executor_class)
            # apply('close') is not used for handshaking: the first real write
            # returns the writer PID, without constructing a parent-side driver.

    def call(self, method, *args):
        self._ensure()
        payload = deepcopy(args)
        if self.mode == 'main':
            return self.executor.apply(method, *payload)
        if self.mode == 'process':
            self.pipe.send((method, payload))
            return self._receive()
        try:
            response = self.ray.get(self.actor.apply.remote(method, payload), timeout=self.timeout)
        except Exception as exc:
            self.failure = exc
            raise
        self.pid = response['pid']
        return response['result']

    def bootstrap(self, config, run, context_queries=False):
        return self.call('bootstrap', config, run, context_queries)

    def write_robots(self, snapshots):
        return self.call('robots', snapshots)

    def write_global(self, record):
        return self.call('global', record)

    def append(self, checkpoint, events):
        began = time.perf_counter()
        try:
            status = self.call('append', checkpoint, events)
        except Exception as exc:
            self.last_error = f'knowledge {self.mode} commit failed: {type(exc).__name__}'
            raise
        self._statuses[checkpoint['run_id']] = status
        self.last_error = None
        self.timings['mirror_seconds'] += time.perf_counter() - began
        self.timings['commits'] += 1

    def status(self, run_id):
        return {**self._statuses.get(run_id, {'backend': 'neo4j-direct', 'durable_seq': 0,
                 'direct_commit_seq': 0, 'mirrored_seq': 0, 'lag': 0}),
                'run_id': run_id, 'local_journal': False, 'last_error': self.last_error,
                'write_mode': self.mode, 'writer_pid': self.pid}

    def close(self):
        if self.closed:
            return
        try:
            if self.pid is not None and self.failure is None:
                self.call('close')
        finally:
            self.closed = True
            if self.pipe is not None:
                self.pipe.close()
            if self.process is not None:
                self.process.join(timeout=5)
                if self.process.is_alive():
                    self.process.terminate()
                    self.process.join(timeout=5)
            if self.actor is not None:
                self.ray.kill(self.actor, no_restart=True)
