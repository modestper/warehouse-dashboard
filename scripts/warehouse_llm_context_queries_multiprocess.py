"""仓储上下文多进程查询；每个子进程独立连接 Neo4j，逐条自动提交。"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from datetime import datetime
import getpass
import multiprocessing as mp
from multiprocessing.util import Finalize
import os
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.robot_operation_context import (
    CATALOG_VERSION, DEFAULT_PARAMS, ContextOptions, read_context,
    select_queries, validate_options,
)
from ops.warehouse_kg_config import Neo4jConnection
from scripts.warehouse_llm_context_queries import (
    json_text, nonnegative, parse_param, percentile, positive, positive_float,
    print_queries, robot_ids, summary_from,
)


@dataclass(frozen=True)
class ProcessConnection:
    uri: str
    user: str
    password: str = field(repr=False)
    database: str = 'neo4j'
    transaction_timeout: float = 30.0
    max_connection_pool_size: int = 1


_CONNECTION = None
_PROCESS_DRIVER = None
_DRIVER_FACTORY = None
_FINALIZER = None


def _close_process_driver():
    global _PROCESS_DRIVER
    driver, _PROCESS_DRIVER = _PROCESS_DRIVER, None
    if driver is not None:
        driver.close()


def _initialize_process(connection, driver_factory=None):
    """Only plain configuration crosses the process boundary, never a driver."""
    global _CONNECTION, _DRIVER_FACTORY, _FINALIZER
    _close_process_driver()
    if _FINALIZER is not None:
        _FINALIZER.cancel()
    _CONNECTION, _DRIVER_FACTORY = connection, driver_factory
    _FINALIZER = Finalize(None, _close_process_driver, exitpriority=10)


def _make_driver(connection):
    from neo4j import GraphDatabase

    return GraphDatabase.driver(connection.uri, auth=(connection.user, connection.password),
        max_connection_pool_size=connection.max_connection_pool_size,
        connection_timeout=5.0, connection_acquisition_timeout=10.0,
        max_transaction_retry_time=0.0)


def _get_process_driver():
    global _PROCESS_DRIVER
    if _PROCESS_DRIVER is None:
        if _CONNECTION is None:
            raise RuntimeError('查询子进程尚未初始化')
        driver = (_DRIVER_FACTORY or _make_driver)(_CONNECTION)
        try:
            driver.verify_connectivity()
        except Exception:
            driver.close()
            raise
        _PROCESS_DRIVER = driver
    return _PROCESS_DRIVER


def _error_context(options, context_index, exc):
    return dict(catalog_version=CATALOG_VERSION, context_index=context_index,
                run_id=options.run_id, robot_id=options.robot_id, requested_tick=options.tick,
                mode='explain' if options.explain else 'read', status='error', queries={},
                error_type=type(exc).__name__, error=str(exc), elapsed_ms=0.0,
                transaction_mode='auto_commit_per_query', action_authorized=False)


def _process_worker(options, context_index, repeat_index, submitted):
    began = time.perf_counter()
    driver_needed = _PROCESS_DRIVER is None
    driver_setup_ms = 0.0
    try:
        setup_started = time.perf_counter()
        driver = _get_process_driver()
        if driver_needed:
            driver_setup_ms = (time.perf_counter() - setup_started) * 1000
        context, records = read_context(driver, options, database=_CONNECTION.database,
            timeout=_CONNECTION.transaction_timeout, context_index=context_index)
    except Exception as exc:
        if driver_needed and not driver_setup_ms:
            driver_setup_ms = (time.perf_counter() - began) * 1000
        context, records = _error_context(options, context_index, exc), []
        context['elapsed_ms'] = (time.perf_counter() - began) * 1000
    context.update(execution_backend='multiprocess', worker_pid=os.getpid(),
                   parent_pid=os.getppid(), repeat_index=repeat_index,
                   queue_ms=(began - submitted) * 1000,
                   worker_elapsed_ms=(time.perf_counter() - began) * 1000,
                   driver_setup_ms=driver_setup_ms,
                   driver_initialized=driver_needed and _PROCESS_DRIVER is not None,
                   elapsed_measured=True)
    for record in records:
        record.update(worker_pid=os.getpid(), repeat_index=repeat_index,
                      tick=context.get('tick'), dataset_id=context.get('dataset_id'),
                      execution_backend='multiprocess')
    return context, records


def parse_args(argv=None):
    connection = Neo4jConnection.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--uri', default=connection.uri)
    parser.add_argument('--user', default=connection.user)
    parser.add_argument('--password', default=connection.password, help='也可使用 NEO4J_PASSWORD；仅在主进程提示输入')
    parser.add_argument('--database', default=connection.database)
    parser.add_argument('--run-id', default=os.environ.get('KG_RUN_ID'))
    parser.add_argument('--queries', default='core', help='查询包/名称/类别/编号范围，例如 all、w6-w13')
    parser.add_argument('--list-queries', action='store_true')
    parser.add_argument('--robot-id', action='append', type=nonnegative, default=[])
    parser.add_argument('--robot-ids', help='逗号或范围，例如 1-100')
    parser.add_argument('--tick', type=nonnegative, help='省略则每个 context 固定一次最新完成步')
    parser.add_argument('--task-id', action='append', default=[])
    parser.add_argument('--task-ids', help='逗号分隔，省略则自动发现任务')
    parser.add_argument('--max-tasks', type=positive, default=8)
    parser.add_argument('--history-ticks', type=positive, default=60)
    parser.add_argument('--param', action='append', type=parse_param, default=[])
    parser.add_argument('--repeat', type=positive, default=1, help='每台机器人生成多少次 context')
    parser.add_argument('--max-workers', type=positive, help='查询子进程数上限，默认等于机器人数量')
    parser.add_argument('--max-connection-pool-size', type=positive, default=1,
                        help='每个子进程的连接池上限，默认1')
    parser.add_argument('--transaction-timeout', type=positive_float, default=30.0,
                        help='每条查询的自动提交事务超时秒数')
    parser.add_argument('--output-dir', type=Path, help='全新输出目录')
    parser.add_argument('--include-cypher', action='store_true')
    parser.add_argument('--print-context', action='store_true')
    parser.add_argument('--dry-run', action='store_true', help='打印查询计划，不启动子进程或连接数据库')
    parser.add_argument('--explain', action='store_true', help='只执行 EXPLAIN，不读取业务结果')
    args = parser.parse_args(argv)
    if args.list_queries:
        return args, None, None
    try:
        if len(dict(args.param)) != len(args.param):
            raise ValueError('--param 包含重复参数')
        tasks = args.task_id + [item.strip() for item in (args.task_ids or '').split(',') if item.strip()]
        options = ContextOptions(run_id=args.run_id, tick=args.tick, selection=args.queries,
            task_ids=tuple(dict.fromkeys(tasks)), max_tasks=args.max_tasks,
            history_ticks=args.history_ticks, params=dict(args.param),
            include_cypher=args.include_cypher, explain=args.explain)
        validate_options(options)
        ids = robot_ids(args.robot_ids, args.robot_id)
        args.max_workers = args.max_workers or len(ids)
    except (ValueError, argparse.ArgumentTypeError) as exc:
        parser.error(str(exc))
    return args, options, ids


def dry_run(options, ids, args):
    queries, automatic = select_queries(options.selection, explicit_tasks=bool(options.task_ids))
    print(json_text(dict(catalog_version=CATALOG_VERSION, mode='dry_run',
        execution_backend='multiprocess', start_method='spawn', transaction_mode='auto_commit_per_query',
        run_id=options.run_id, robot_ids=ids, requested_tick=options.tick,
        repeat=args.repeat, planned_contexts=len(ids) * args.repeat,
        max_workers=args.max_workers, effective_process_limit=min(args.max_workers, len(ids) * args.repeat),
        connection_pool_size_per_process=args.max_connection_pool_size,
        task_ids=options.task_ids, effective_defaults={**DEFAULT_PARAMS, **options.params},
        parameter_resolution=dict(t='w0.t 或显式 --tick', dataset_id='w0.datasets[0].id',
            task_ids='显式任务；否则本机任务及已观察候选', from_tick='max(0,t-history_ticks) 或显式参数'),
        queries=[dict(id=q.id, name=q.name, automatic=q.id in automatic, requires_tasks=q.requires_tasks,
                      required_params=q.required_params, cypher=q.cypher) for q in queries]), pretty=True))


def run_benchmark(options, ids, args, *, driver_factory=None):
    """Schedule contexts across spawn workers; the parent owns all output files.

    driver_factory is a test seam: it must be a spawn-picklable factory. The CLI
    always constructs real Neo4j drivers inside each child process.
    """
    connection = ProcessConnection(args.uri, args.user, args.password, args.database,
                                   args.transaction_timeout, args.max_connection_pool_size)
    output = args.output_dir or Path('data/warehouse_context_queries_multiprocess') / (
        datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:8])
    output.mkdir(parents=True, exist_ok=False)
    effective_workers = min(args.max_workers, len(ids) * args.repeat)
    context_stats, query_records = [], []
    wall_started = time.perf_counter()
    with (output / 'contexts.jsonl').open('w', encoding='utf-8') as context_file, \
         (output / 'queries.jsonl').open('w', encoding='utf-8') as query_file:

        def save_result(context, records):
            context_file.write(json_text(context) + '\n')
            context_file.flush()
            for record in records:
                query_file.write(json_text(record) + '\n')
            query_file.flush()
            context_stats.append({key: context.get(key) for key in
                ('status', 'elapsed_ms', 'worker_pid', 'driver_initialized', 'elapsed_measured')})
            query_records.extend(records)
            if args.print_context:
                print(json_text(context, pretty=True))

        def save_worker_error(job, exc):
            job_options, index, repeat_index, submitted = job
            context = _error_context(job_options, index, exc)
            context.update(execution_backend='multiprocess', worker_pid=None, repeat_index=repeat_index,
                           elapsed_measured=False, driver_initialized=False,
                           submit_to_failure_ms=(time.perf_counter() - submitted) * 1000)
            save_result(context, [])

        with ProcessPoolExecutor(max_workers=effective_workers, mp_context=mp.get_context('spawn'),
                initializer=_initialize_process, initargs=(connection, driver_factory)) as pool:
            futures = {}
            for repeat_index in range(args.repeat):
                for offset, robot_id in enumerate(ids):
                    job = (replace(options, robot_id=robot_id), repeat_index * len(ids) + offset,
                           repeat_index, time.perf_counter())
                    try:
                        futures[pool.submit(_process_worker, *job)] = job
                    except Exception as exc:
                        save_worker_error(job, exc)
            for future in as_completed(futures):
                try:
                    context, records = future.result()
                except Exception as exc:
                    save_worker_error(futures[future], exc)
                else:
                    save_result(context, records)
    summary = summary_from(query_records, context_stats, (time.perf_counter() - wall_started) * 1000)
    measured = [row['elapsed_ms'] for row in context_stats if row['elapsed_measured']]
    processes = defaultdict(list)
    for row in context_stats:
        if row['worker_pid'] is not None:
            processes[row['worker_pid']].append(row)
    summary.update(execution_backend='multiprocess', start_method='spawn',
        transaction_mode='auto_commit_per_query', run_id=options.run_id, database=args.database,
        requested_tick=options.tick, robot_ids=ids, repeat=args.repeat,
        max_workers=args.max_workers, effective_process_limit=effective_workers,
        connection_pool_size_per_process=args.max_connection_pool_size,
        process_count=len(processes), worker_pids=sorted(processes),
        measured_contexts=len(measured), context_p50_ms=percentile(measured, .50),
        context_p95_ms=percentile(measured, .95), mode='explain' if options.explain else 'read',
        processes={str(pid): dict(contexts=len(rows), statuses=dict(Counter(row['status'] for row in rows)),
            driver_initializations=sum(bool(row['driver_initialized']) for row in rows))
            for pid, rows in sorted(processes.items())}, output_dir=str(output.resolve()))
    (output / 'summary.json').write_text(json_text(summary, pretty=True) + '\n', encoding='utf-8')
    return summary


def main(argv=None):
    args, options, ids = parse_args(argv)
    if args.list_queries:
        print_queries()
        return 0
    if args.dry_run:
        dry_run(options, ids, args)
        return 0
    if not args.password and sys.stdin.isatty():
        args.password = getpass.getpass('Neo4j 密码: ')
    if not args.password:
        print('未配置 Neo4j 密码：请设置 NEO4J_PASSWORD / WAREHOUSE_NEO4J_PASSWORD，或使用 --password。', file=sys.stderr)
        return 2
    try:
        summary = run_benchmark(options, ids, args)
    except Exception as exc:
        print(json_text(dict(status='error', error_type=type(exc).__name__, error=str(exc))), file=sys.stderr)
        return 1
    print(json_text(summary, pretty=True))
    statuses = summary['context_statuses']
    return 1 if statuses.get('error') or statuses.get('unavailable') else 0


if __name__ == '__main__':
    raise SystemExit(main())
