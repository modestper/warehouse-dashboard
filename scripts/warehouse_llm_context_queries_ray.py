"""单机 Ray 并发查询：每个 Actor 独立连接，绑定不同物理核，逐条自动提交。"""
from __future__ import annotations

import argparse
from dataclasses import replace
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.robot_operation_context import (
    CATALOG_VERSION, DEFAULT_PARAMS, QUERIES, ContextOptions, read_context,
    select_queries, validate_options,
)
from ops.warehouse_kg_config import Neo4jConnection
from scripts.warehouse_llm_context_queries import (
    json_text, nonnegative, parse_param, percentile, positive, positive_float,
    print_queries, robot_ids, summary_from,
)
from scripts.warehouse_llm_context_queries_multiprocess import ProcessConnection, _make_driver, _error_context


CPU_TOPOLOGY_ROOT = Path('/sys/devices/system/cpu')
DEFAULT_WORKERS = 100
DEFAULT_PASSWORD = 'neo4j123456'


def physical_core(cpu_id, topology_root=CPU_TOPOLOGY_ROOT):
    try:
        root = Path(topology_root) / f'cpu{cpu_id}' / 'topology'
        return (int((root / 'physical_package_id').read_text()), int((root / 'core_id').read_text()))
    except (OSError, ValueError) as exc:
        raise ValueError(f'无法读取 CPU {cpu_id} 的物理核心拓扑：{exc}') from exc


def parse_cpu_ids(raw):
    ids = []
    try:
        for part in raw.split(','):
            bounds = part.strip().split('-')
            if len(bounds) == 1:
                ids.append(int(bounds[0]))
            elif len(bounds) == 2:
                first, last = map(int, bounds)
                if first < 0 or last < first or last - first > 65535:
                    raise ValueError('无效或过大的 CPU 范围')
                ids.extend(range(first, last + 1))
            else:
                raise ValueError('无效范围')
        if not ids or any(cpu < 0 for cpu in ids) or len(set(ids)) != len(ids):
            raise ValueError('CPU 编号为空、为负或重复')
    except ValueError as exc:
        raise ValueError('--cpu-ids 应为互不重复的 CPU 编号或递增范围，例如 0-3,8,10') from exc
    return ids


def choose_cpu_ids(workers, raw=None, *, allowed_cpus=None, topology_root=CPU_TOPOLOGY_ROOT):
    if not hasattr(os, 'sched_setaffinity') or not Path('/proc/self/task').is_dir():
        raise ValueError('该入口需要支持 CPU affinity 和 /proc 的 Linux 环境')
    allowed = set(os.sched_getaffinity(0) if allowed_cpus is None else allowed_cpus)
    if raw is not None:
        chosen = parse_cpu_ids(raw)
        if len(chosen) != workers:
            raise ValueError(f'--cpu-ids 需要恰好 {workers} 个 CPU 编号，实际 {len(chosen)} 个')
        if set(chosen) - allowed:
            raise ValueError(f'指定 CPU 不在当前允许范围内：{sorted(set(chosen) - allowed)}')
        if len({physical_core(cpu, topology_root) for cpu in chosen}) != workers:
            raise ValueError('指定 CPU 包含同一物理核心的超线程，请选择不同物理核心')
        return chosen
    chosen, seen = [], set()
    for cpu in sorted(allowed):
        core = physical_core(cpu, topology_root)
        if core not in seen:
            chosen.append(cpu)
            seen.add(core)
    if len(chosen) < workers:
        raise ValueError(f'需要 {workers} 个不同物理核心，当前允许范围内只有 {len(chosen)} 个；请减小 --max-workers')
    return chosen[:workers]


def pin_actor_threads(cpu_id):
    """Pin all existing threads; new threads inherit the creator's affinity."""
    expected = {cpu_id}
    for _ in range(8):
        for path in Path('/proc/self/task').iterdir():
            try:
                os.sched_setaffinity(int(path.name), expected)
            except ProcessLookupError:
                pass
        masks = []
        for path in Path('/proc/self/task').iterdir():
            try:
                masks.append(os.sched_getaffinity(int(path.name)))
            except ProcessLookupError:
                pass
        if masks and all(mask == expected for mask in masks):
            return len(masks)
    raise RuntimeError(f'Actor 线程未能全部绑定到 CPU {cpu_id}')


def load_ray():
    try:
        import ray
    except ImportError as exc:
        raise RuntimeError('当前 Python 环境未安装 Ray，请使用项目的 Ray 环境') from exc
    return ray


class RayContextWorker:
    def __init__(self, worker_index, cpu_id, connection, driver_factory=None):
        self.worker_index, self.cpu_id = worker_index, cpu_id
        self.pid, self.connection = os.getpid(), connection
        self.thread_count = pin_actor_threads(cpu_id)
        self.driver = (driver_factory or _make_driver)(connection)
        try:
            self.driver.verify_connectivity()
        except Exception:
            self.driver.close()
            self.driver = None
            raise

    def ready(self):
        self.thread_count = pin_actor_threads(self.cpu_id)
        socket, core = physical_core(self.cpu_id)
        return dict(worker_index=self.worker_index, worker_pid=self.pid, cpu_id=self.cpu_id,
            affinity_cpus=sorted(os.sched_getaffinity(0)), physical_socket=socket, physical_core=core,
            ray_assigned_resources=load_ray().get_runtime_context().get_assigned_resources(),
            pinned_thread_count=self.thread_count)

    def execute(self, options, context_index, repeat_index, submitted):
        began = time.perf_counter()
        pin_actor_threads(self.cpu_id)
        try:
            context, records = read_context(self.driver, options, database=self.connection.database,
                timeout=self.connection.transaction_timeout, context_index=context_index)
        except Exception as exc:
            context, records = _error_context(options, context_index, exc), []
            context['elapsed_ms'] = (time.perf_counter() - began) * 1000
        metadata = dict(execution_backend='ray', concurrency_mode='ray', worker_pid=self.pid,
            worker_index=self.worker_index, cpu_id=self.cpu_id,
            affinity_cpus=sorted(os.sched_getaffinity(0)), repeat_index=repeat_index)
        if metadata['affinity_cpus'] != [self.cpu_id]:
            raise RuntimeError('Actor 执行线程的 CPU 绑定发生变化')
        context.update(metadata, queue_ms=(began - submitted) * 1000,
                       worker_elapsed_ms=(time.perf_counter() - began) * 1000, elapsed_measured=True)
        for record in records:
            record.update(metadata, tick=context.get('tick'), dataset_id=context.get('dataset_id'))
        return context, records

    def close(self):
        if self.driver is not None:
            self.driver.close()
            self.driver = None


def parse_args(argv=None):
    connection = Neo4jConnection.from_env()
    parser = argparse.ArgumentParser(description=__doc__,
        epilog='num_cpus=1 预留 Ray 逻辑资源；affinity 约束执行位置，不排除其他系统进程。')
    parser.add_argument('--uri', default=connection.uri)
    parser.add_argument('--user', default=connection.user)
    parser.add_argument('--password', default=connection.password or DEFAULT_PASSWORD)
    parser.add_argument('--database', default=connection.database)
    parser.add_argument('--run-id', default=os.environ.get('KG_RUN_ID'))
    parser.add_argument('--queries', default='core', help='查询包/名称/类别/编号范围，例如 all、w6-w13')
    parser.add_argument('--list-queries', action='store_true')
    parser.add_argument('--robot-id', action='append', type=nonnegative, default=[])
    parser.add_argument('--robot-ids', help='逗号或范围；未指定时使用1到Actor数')
    parser.add_argument('--tick', type=nonnegative)
    parser.add_argument('--task-id', action='append', default=[])
    parser.add_argument('--task-ids')
    parser.add_argument('--max-tasks', type=positive, default=8)
    parser.add_argument('--history-ticks', type=positive, default=60)
    parser.add_argument('--param', action='append', type=parse_param, default=[])
    parser.add_argument('--repeat', type=positive, default=1)
    parser.add_argument('--max-workers', type=positive, default=DEFAULT_WORKERS, help='Actor数，默认100')
    parser.add_argument('--cpu-ids', help='逻辑CPU编号，例如0-99；数量等于Actor数且属于不同物理核')
    parser.add_argument('--startup-timeout', type=positive_float, default=180.0, help='全部Actor就绪超时秒数')
    parser.add_argument('--max-connection-pool-size', type=positive, default=1, help='每个Actor的连接池上限')
    parser.add_argument('--transaction-timeout', type=positive_float, default=30.0, help='每条自动提交查询超时秒数')
    parser.add_argument('--output-dir', type=Path, help='全新输出目录')
    parser.add_argument('--include-cypher', action='store_true')
    parser.add_argument('--print-context', action='store_true')
    parser.add_argument('--progress-every', type=nonnegative, default=20)
    parser.add_argument('--dry-run', action='store_true', help='打印查询计划，不启动Ray或连接数据库')
    parser.add_argument('--explain', action='store_true')
    args = parser.parse_args(argv)
    if args.list_queries:
        return args, None, None
    try:
        if len(dict(args.param)) != len(args.param):
            raise ValueError('--param 包含重复参数')
        tasks = args.task_id + [item.strip() for item in (args.task_ids or '').split(',') if item.strip()]
        options = ContextOptions(run_id=args.run_id, selection=args.queries, tick=args.tick,
            task_ids=tuple(dict.fromkeys(tasks)), max_tasks=args.max_tasks, history_ticks=args.history_ticks,
            params=dict(args.param), include_cypher=args.include_cypher, explain=args.explain)
        validate_options(options)
        ids = robot_ids(args.robot_ids, args.robot_id) if args.robot_ids or args.robot_id else list(range(1, args.max_workers + 1))
    except (ValueError, argparse.ArgumentTypeError) as exc:
        parser.error(str(exc))
    return args, options, ids


def dry_run(options, ids, args):
    queries, automatic = select_queries(options.selection, explicit_tasks=bool(options.task_ids))
    print(json_text(dict(catalog_version=CATALOG_VERSION, mode='dry_run', execution_backend='ray',
        transaction_mode='auto_commit_per_query', run_id=options.run_id, requested_tick=options.tick,
        robot_ids=ids, repeat=args.repeat, planned_contexts=len(ids) * args.repeat,
        max_workers=args.max_workers, ray_cpus_per_actor=1, cpu_binding='distinct_physical_cores',
        cpu_ids_requested=args.cpu_ids, system_cpu_exclusivity=False,
        connection_pool_size_per_process=args.max_connection_pool_size,
        effective_defaults={**DEFAULT_PARAMS, **options.params},
        queries=[dict(id=q.id, name=q.name, automatic=q.id in automatic, requires_tasks=q.requires_tasks,
                      required_params=q.required_params, cypher=q.cypher) for q in queries]), pretty=True))


def _query_group_metrics(records):
    executed = [record for record in records if record['status'] != 'skipped']
    successes = [record for record in executed if record['status'] == 'ok']
    times = [record['elapsed_ms'] for record in successes]

    def rounded(value):
        return round(value, 3) if value is not None else None

    return dict(total_queries=len(executed), success_count=len(successes),
        failure_count=len(executed)-len(successes), skipped_count=len(records)-len(executed),
        avg_ms=rounded(sum(times)/len(times)) if times else None,
        p50_ms=rounded(percentile(times, .50)), p95_ms=rounded(percentile(times, .95)),
        p99_ms=rounded(percentile(times, .99)), max_ms=rounded(max(times)) if times else None,
        avg_rows=rounded(sum(record['row_count'] for record in successes)/len(successes)) if successes else None)


def final_output_metrics(records, context_stats, wall_ms):
    """LaDe-style metrics; skips and unmeasured contexts are not executions."""
    overall = _query_group_metrics(records)
    by_query, by_category = {}, {}
    for record in records:
        qid = record['query_id']
        by_query.setdefault(qid, []).append(record)
        by_category.setdefault(QUERIES[qid].category, []).append(record)
    times = [row['elapsed_ms'] for row in context_stats if row['elapsed_measured']]
    return dict(context_count=len(context_stats), total_queries=overall['total_queries'],
        success_count=overall['success_count'], failure_count=overall['failure_count'],
        skipped_count=overall['skipped_count'],
        throughput_qps=round(overall['total_queries']*1000/wall_ms, 3) if wall_ms > 0 else None,
        query_avg_ms=overall['avg_ms'], query_p50_ms=overall['p50_ms'],
        query_p95_ms=overall['p95_ms'], query_p99_ms=overall['p99_ms'],
        context_avg_ms=round(sum(times)/len(times), 3) if times else None,
        context_p95_ms=round(percentile(times, .95), 3) if times else None,
        latency_scope='successful_queries_only', throughput_scope='executed_queries_over_total_wall',
        by_query={qid: dict(name=QUERIES[qid].name, **_query_group_metrics(by_query[qid]))
                  for qid in QUERIES if qid in by_query},
        by_category={category: _query_group_metrics(rows) for category, rows in sorted(by_category.items())})


def print_summary(summary):
    def metric(value):
        return f'{value:.3f}' if value is not None else 'N/A'

    print(f"已启动 Actor: {summary.get('started_worker_count', 0)}，有查询返回的 Actor: {summary.get('observed_worker_count', 0)}")
    print(f"查询目录版本: {summary.get('catalog_version', CATALOG_VERSION)}")
    print('\nWarehouse LLM 前置上下文查询结果')
    print('========================================')
    print(f"运行状态: {summary.get('status', 'unknown')}")
    print(f"数据库: {summary.get('database', 'N/A')}")
    print(f"run_id: {summary.get('run_id', 'N/A')}")
    print(f"执行模式: {summary.get('mode', 'read')}")
    print(f"context 数: {summary.get('context_count', 0)}（计划 {summary.get('planned_contexts', 0)}）")
    statuses = summary.get('context_statuses') or {}
    if statuses:
        print('context 状态: ' + ' '.join(f'{status}={count}' for status, count in sorted(statuses.items())))
    print(f"总查询数: {summary.get('total_queries', 0)}")
    print(f"成功: {summary.get('success_count', 0)}")
    print(f"失败: {summary.get('failure_count', 0)}")
    print(f"跳过: {summary.get('skipped_count', 0)}（不计入总查询数和吞吐）")
    print(f"总耗时: {metric(summary.get('wall_ms'))} ms")
    print(f"吞吐: {metric(summary.get('throughput_qps'))} qps")
    print('单查询 avg/p95/p99: ' + ' / '.join(metric(summary.get(key)) for key in
          ('query_avg_ms', 'query_p95_ms', 'query_p99_ms')) + ' ms')
    print('单 context avg/p95: ' + ' / '.join(metric(summary.get(key)) for key in
          ('context_avg_ms', 'context_p95_ms')) + ' ms')
    print('Ray 启动/查询/清理: ' + ' / '.join(metric(summary.get(key)) for key in
          ('ray_startup_ms', 'query_phase_wall_ms', 'ray_cleanup_ms')) + ' ms')
    print('\n按查询统计（延时仅统计成功查询）')
    for qid, item in summary.get('by_query', {}).items():
        print(f"{qid}: total={item['total_queries']} success={item['success_count']} "
              f"failed={item['failure_count']} skipped={item['skipped_count']} "
              f"avg={metric(item['avg_ms'])}ms p95={metric(item['p95_ms'])}ms "
              f"rows_avg={metric(item['avg_rows'])}")
    if summary.get('run_error'):
        error = summary['run_error']
        print(f"\n运行错误: {error.get('error_type', 'Error')}: {error.get('error', '')}")
    if summary.get('cleanup_errors'):
        print('清理错误: ' + ', '.join(summary['cleanup_errors']))
    if summary.get('output_dir'):
        output = Path(summary['output_dir'])
        print(f"\n查询日志: {output / 'queries.jsonl'}")
        print(f"上下文日志: {output / 'contexts.jsonl'}")
        print(f"汇总日志: {output / 'summary.json'}")


def run_benchmark(options, ids, args, *, driver_factory=None, ray_module=None):
    validate_options(options)
    ray = ray_module or load_ray()
    if ray.is_initialized():
        raise RuntimeError('当前 Python 已连接 Ray；请用独立进程运行此入口，不复用仿真集群')
    cpu_ids = choose_cpu_ids(args.max_workers, args.cpu_ids)
    connection = ProcessConnection(args.uri, args.user, args.password, args.database,
                                   args.transaction_timeout, args.max_connection_pool_size)
    output = args.output_dir or Path('data/warehouse_context_queries_ray') / (
        datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:8])
    output.mkdir(parents=True, exist_ok=False)
    ray_temp_dir = tempfile.mkdtemp(prefix='whq-ray-', dir='/tmp')
    actors, worker_info, context_stats, records = [], [], [], []
    cleanup_errors, run_error = [], None
    jobs = iter((replace(options, robot_id=rid), repeat * len(ids) + offset, repeat)
                for repeat in range(args.repeat) for offset, rid in enumerate(ids))
    wall_started = time.perf_counter()
    query_started = query_ended = None
    with (output / 'contexts.jsonl').open('w', encoding='utf-8') as context_file, \
         (output / 'queries.jsonl').open('w', encoding='utf-8') as query_file:

        def save_result(context, query_records):
            context_file.write(json_text(context) + '\n')
            context_file.flush()
            for row in query_records:
                query_file.write(json_text(row) + '\n')
            query_file.flush()
            context_stats.append({key: context.get(key) for key in ('status', 'elapsed_ms', 'worker_pid', 'elapsed_measured')})
            records.extend(query_records)
            if args.print_context:
                print(json_text(context, pretty=True))
            if args.progress_every and len(context_stats) % args.progress_every == 0:
                print(f'进度: {len(context_stats)}/{len(ids)*args.repeat} contexts')

        def save_failure(job, exc, info=None):
            job_options, index, repeat_index = job
            context = _error_context(job_options, index, exc)
            context.update(execution_backend='ray', concurrency_mode='ray', repeat_index=repeat_index,
                elapsed_measured=False, worker_pid=None, assigned_worker_pid=(info or {}).get('worker_pid'),
                worker_index=(info or {}).get('worker_index'), cpu_id=(info or {}).get('cpu_id'))
            save_result(context, [])

        pending = {}

        def submit(actor, info, job):
            try:
                ref = actor.execute.remote(*job, time.perf_counter())
                pending[ref] = actor, info, job
            except Exception as exc:
                save_failure(job, exc, info)

        try:
            pythonpath = os.pathsep.join(filter(None, (str(ROOT), os.environ.get('PYTHONPATH'))))
            ray.init(address='local', num_cpus=args.max_workers, include_dashboard=False,
                object_store_memory=128*1024*1024, _temp_dir=ray_temp_dir,
                _node_ip_address='127.0.0.1', log_to_driver=False,
                runtime_env={'env_vars': {'PYTHONPATH': pythonpath, 'PYTHONDONTWRITEBYTECODE': '1',
                    'OMP_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1'}})
            actor_class = ray.remote(num_cpus=1, max_concurrency=1, max_restarts=0, max_task_retries=0)(RayContextWorker)
            for index, cpu in enumerate(cpu_ids):
                actors.append(actor_class.remote(index, cpu, connection, driver_factory))
            worker_info = ray.get([actor.ready.remote() for actor in actors], timeout=args.startup_timeout)
            if len({info['worker_pid'] for info in worker_info}) != args.max_workers:
                raise RuntimeError('Ray 未创建预期数量的独立 Actor 进程')
            for cpu, info in zip(cpu_ids, worker_info):
                if info['affinity_cpus'] != [cpu]:
                    raise RuntimeError('Actor CPU 绑定校验失败')
            query_started = time.perf_counter()
            for actor, info in zip(actors, worker_info):
                job = next(jobs, None)
                if job is not None:
                    submit(actor, info, job)
            while pending:
                ready, _ = ray.wait(list(pending), num_returns=1)
                ref = ready[0]
                actor, info, job = pending.pop(ref)
                try:
                    context, query_records = ray.get(ref)
                except Exception as exc:
                    save_failure(job, exc, info)
                    # A failed RPC may mean the Actor died; retire it without retries.
                else:
                    save_result(context, query_records)
                    following = next(jobs, None)
                    if following is not None:
                        submit(actor, info, following)
            for job in jobs:
                run_error = dict(error_type='NoAvailableActors', error='所有查询Actor已失效，未分配任务标记失败')
                save_failure(job, RuntimeError(run_error['error']))
            query_ended = time.perf_counter()
        except Exception as exc:
            run_error = dict(error_type=type(exc).__name__, error=str(exc))
        finally:
            cleanup_started = time.perf_counter()
            if query_started is not None and query_ended is None:
                query_ended = cleanup_started
            close_refs = []
            for actor in actors:
                try:
                    close_refs.append(actor.close.remote())
                except Exception as exc:
                    cleanup_errors.append(type(exc).__name__)
            if close_refs:
                try:
                    ray.get(close_refs, timeout=10)
                except Exception as exc:
                    cleanup_errors.append(type(exc).__name__)
            for actor in actors:
                try:
                    ray.kill(actor, no_restart=True)
                except Exception as exc:
                    cleanup_errors.append(type(exc).__name__)
            try:
                ray.shutdown()
            except Exception as exc:
                cleanup_errors.append(type(exc).__name__)
    ended = time.perf_counter()
    summary = summary_from(records, context_stats, (ended - wall_started) * 1000)
    measured = [row['elapsed_ms'] for row in context_stats if row['elapsed_measured']]
    pids = sorted({row['worker_pid'] for row in context_stats if row['worker_pid'] is not None})
    summary.update(execution_backend='ray', concurrency_mode='ray', transaction_mode='auto_commit_per_query',
        status='error' if run_error or cleanup_errors or any(c['status'] in {'error', 'unavailable'} for c in context_stats) else 'ok',
        run_error=run_error, cleanup_errors=cleanup_errors, run_id=options.run_id, database=args.database,
        requested_tick=options.tick, robot_ids=ids, repeat=args.repeat, planned_contexts=len(ids)*args.repeat,
        max_workers=args.max_workers, ray_cpus_per_actor=1, created_actor_handles=len(actors),
        started_worker_count=len(worker_info), worker_info=worker_info,
        observed_worker_count=len(pids), worker_pids=pids, cpu_ids=cpu_ids,
        cpu_binding='distinct_physical_cores', system_cpu_exclusivity=False,
        connection_pool_size_per_process=args.max_connection_pool_size,
        measured_contexts=len(measured), context_p50_ms=percentile(measured, .50),
        context_p95_ms=percentile(measured, .95),
        ray_startup_ms=((query_started or cleanup_started)-wall_started)*1000,
        query_phase_wall_ms=(query_ended-query_started)*1000 if query_started is not None else 0.0,
        ray_cleanup_ms=(ended-cleanup_started)*1000, ray_temp_dir=ray_temp_dir,
        mode='explain' if options.explain else 'read', output_dir=str(output.resolve()))
    summary.update(final_output_metrics(records, context_stats, summary['wall_ms']))
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
    args.password = args.password or DEFAULT_PASSWORD
    try:
        summary = run_benchmark(options, ids, args)
    except Exception as exc:
        print(json_text(dict(status='error', error_type=type(exc).__name__, error=str(exc))), file=sys.stderr)
        return 1
    print_summary(summary)
    return 1 if summary['status'] == 'error' else 0


if __name__ == '__main__':
    raise SystemExit(main())
