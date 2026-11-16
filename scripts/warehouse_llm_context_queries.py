"""仓储机器人操作上下文查询；只读 Neo4j，支持单机/并发、结果和耗时日志。"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime
import getpass
import json
import math
import os
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.robot_operation_context import (
    CATALOG_VERSION, DEFAULT_PARAMS, PACKS, QUERIES, ContextOptions,
    json_safe, read_context, select_queries, validate_options,
)
from ops.warehouse_kg_config import Neo4jConnection


def json_text(value, *, pretty=False):
    return json.dumps(json_safe(value), ensure_ascii=False, allow_nan=False,
                      indent=2 if pretty else None)


def nonnegative(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError('必须为非负整数')
    return number


def positive(value):
    number = nonnegative(value)
    if number == 0:
        raise argparse.ArgumentTypeError('必须为正整数')
    return number


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError('必须为有限正数')
    return number


def parse_param(value):
    key, sep, raw = value.partition('=')
    if not sep or not key:
        raise argparse.ArgumentTypeError('--param 格式应为 key=value')
    try:
        return key, json.loads(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('--param 的值应是合法 JSON，例如 max_state_age_ticks=4') from exc


def robot_ids(raw, singles=()):
    result = list(singles)
    for item in (raw or '').split(','):
        if not item.strip():
            continue
        parts = item.strip().split('-')
        if len(parts) == 1:
            result.append(nonnegative(parts[0]))
        elif len(parts) == 2:
            first, last = map(nonnegative, parts)
            if first > last or last - first > 9999:
                raise ValueError('机器人范围应递增，且一次范围不超过10000个')
            result.extend(range(first, last + 1))
        else:
            raise ValueError(f'无效机器人编号：{item}')
    return list(dict.fromkeys(result or [1]))


def parse_args(argv=None):
    connection = Neo4jConnection.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--uri', default=connection.uri)
    parser.add_argument('--user', default=connection.user)
    parser.add_argument('--password', default=connection.password, help='也可用 NEO4J_PASSWORD；终端缺省时交互输入')
    parser.add_argument('--database', default=connection.database)
    parser.add_argument('--run-id', default=os.environ.get('KG_RUN_ID'))
    parser.add_argument('--queries', default='core', help='查询包/名称/类别/编号/范围，例如 task_decision、w2、w0-w13')
    parser.add_argument('--list-queries', action='store_true')
    parser.add_argument('--robot-id', action='append', type=nonnegative, default=[])
    parser.add_argument('--robot-ids', help='逗号或范围，例如 1,2,5-10')
    parser.add_argument('--tick', type=nonnegative, help='省略则每个 context 固定一次最新完成步')
    parser.add_argument('--task-id', action='append', default=[])
    parser.add_argument('--task-ids', help='逗号分隔；省略则发现本机任务及候选队列')
    parser.add_argument('--max-tasks', type=positive, default=8, help='单个 context 最多查询的任务数，最大32')
    parser.add_argument('--history-ticks', type=positive, default=60)
    parser.add_argument('--param', action='append', type=parse_param, default=[],
                        help='阈值/窗口/展示上限参数；不允许覆盖 run_id/robot_id/t/dataset_id')
    parser.add_argument('--repeat', type=positive, default=1)
    parser.add_argument('--max-workers', type=positive, default=1, help='并发 context 数，默认1')
    parser.add_argument('--max-connection-pool-size', type=positive)
    parser.add_argument('--transaction-timeout', type=positive_float, default=30.0, help='每条查询的自动提交事务超时秒数')
    parser.add_argument('--output-dir', type=Path, help='全新输出目录；默认 data/warehouse_context_queries/时间戳-随机后缀')
    parser.add_argument('--include-cypher', action='store_true', help='在逐条日志中记录完整语句')
    parser.add_argument('--print-context', action='store_true')
    parser.add_argument('--dry-run', action='store_true', help='打印模板和参数解析规则，不连接数据库')
    parser.add_argument('--explain', action='store_true', help='只执行 EXPLAIN 编译所选语句，不读取业务结果')
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
    except (ValueError, argparse.ArgumentTypeError) as exc:
        parser.error(str(exc))
    return args, options, ids


def print_queries():
    for query in QUERIES.values():
        print(f'{query.id:3} {query.name:16} [{query.category}] {query.description}')
    print('\n查询包：')
    for name, ids in PACKS.items():
        print(f'  {name}: {",".join(ids)}')


def dry_run(options, ids):
    queries, automatic = select_queries(options.selection, explicit_tasks=bool(options.task_ids))
    plan = dict(catalog_version=CATALOG_VERSION, mode='dry_run', run_id=options.run_id,
                robot_ids=ids, requested_tick=options.tick, task_ids=options.task_ids,
                effective_defaults={**DEFAULT_PARAMS, **options.params},
                parameter_resolution=dict(t='w0.t 或显式 --tick', dataset_id='w0.datasets[0].id',
                    task_ids='显式任务；否则本机任务优先，再取 w1 中已观察候选，最多 max_tasks',
                    from_tick='max(0,t-history_ticks)，或 --param from_tick=N'),
                queries=[dict(id=q.id, name=q.name, automatic=q.id in automatic, requires_tasks=q.requires_tasks,
                              required_params=q.required_params, cypher=q.cypher) for q in queries])
    print(json_text(plan, pretty=True))


def percentile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * fraction
    low = math.floor(position)
    high = math.ceil(position)
    return values[low] + (values[high] - values[low]) * (position - low)


def summary_from(records, context_stats, wall_ms):
    by_query = defaultdict(list)
    for record in records:
        by_query[record['query_id']].append(record)
    summary = dict(catalog_version=CATALOG_VERSION, contexts=len(context_stats),
                   context_statuses=dict(Counter(c['status'] for c in context_stats)),
                   query_statuses=dict(Counter(r['status'] for r in records)), wall_ms=wall_ms,
                   context_p50_ms=percentile([c['elapsed_ms'] for c in context_stats], .50),
                   context_p95_ms=percentile([c['elapsed_ms'] for c in context_stats], .95),
                   queries={})
    for qid, group in by_query.items():
        executed = [r for r in group if r['status'] != 'skipped']
        times = [r['elapsed_ms'] for r in executed]
        summary['queries'][qid] = dict(name=QUERIES[qid].name, executed=len(executed),
            statuses=dict(Counter(r['status'] for r in group)), rows=sum(r['row_count'] for r in group),
            mean_ms=sum(times) / len(times) if times else None,
            p50_ms=percentile(times, .50), p95_ms=percentile(times, .95), max_ms=max(times, default=None))
    return summary


def _worker(driver, options, args, context_index, submitted):
    began = time.perf_counter()
    try:
        context, records = read_context(driver, options, database=args.database,
                                       timeout=args.transaction_timeout, context_index=context_index)
    except Exception as exc:
        # A connection/session/normalization failure is distinct from an empty query result.
        context = dict(catalog_version=CATALOG_VERSION, context_index=context_index,
                       run_id=options.run_id, robot_id=options.robot_id, requested_tick=options.tick,
                       status='error', queries={}, error_type=type(exc).__name__, error=str(exc),
                       elapsed_ms=(time.perf_counter() - began) * 1000, action_authorized=False)
        records = []
    context['queue_ms'] = (began - submitted) * 1000
    context['worker_elapsed_ms'] = (time.perf_counter() - began) * 1000
    for record in records:
        record['tick'] = context.get('tick')
        record['dataset_id'] = context.get('dataset_id')
    return context, records


def run_benchmark(driver, options, ids, args):
    output = args.output_dir or Path('data/warehouse_context_queries') / (
        datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:8])
    output.mkdir(parents=True, exist_ok=False)
    context_stats, query_records = [], []
    wall_start = time.perf_counter()
    with (output / 'contexts.jsonl').open('w', encoding='utf-8') as contexts, \
         (output / 'queries.jsonl').open('w', encoding='utf-8') as queries, \
         ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = []
        for repeat in range(args.repeat):
            for offset, rid in enumerate(ids):
                index = repeat * len(ids) + offset
                futures.append(pool.submit(_worker, driver, replace(options, robot_id=rid),
                                           args, index, time.perf_counter()))
        for future in as_completed(futures):
            context, records = future.result()
            contexts.write(json_text(context) + '\n')
            contexts.flush()
            for record in records:
                queries.write(json_text(record) + '\n')
            queries.flush()
            context_stats.append({key: context[key] for key in ('status', 'elapsed_ms')})
            query_records.extend(records)
            if args.print_context:
                print(json_text(context, pretty=True))
    summary = summary_from(query_records, context_stats, (time.perf_counter() - wall_start) * 1000)
    summary.update(run_id=options.run_id, database=args.database, requested_tick=options.tick,
                   robot_ids=ids, repeat=args.repeat, max_workers=args.max_workers,
                   mode='explain' if options.explain else 'read', output_dir=str(output.resolve()))
    (output / 'summary.json').write_text(json_text(summary, pretty=True) + '\n', encoding='utf-8')
    return summary


def main(argv=None):
    args, options, ids = parse_args(argv)
    if args.list_queries:
        print_queries()
        return 0
    if args.dry_run:
        dry_run(options, ids)
        return 0
    password = args.password
    if not password and sys.stdin.isatty():
        password = getpass.getpass('Neo4j 密码: ')
    if not password:
        print('未配置 Neo4j 密码：请设置 NEO4J_PASSWORD / WAREHOUSE_NEO4J_PASSWORD，或使用 --password。', file=sys.stderr)
        return 2
    try:
        from neo4j import GraphDatabase
        pool_size = args.max_connection_pool_size or args.max_workers
        driver = GraphDatabase.driver(args.uri, auth=(args.user, password),
            max_connection_pool_size=pool_size, connection_timeout=5.0,
            connection_acquisition_timeout=10.0, max_transaction_retry_time=0.0)
        try:
            driver.verify_connectivity()
            summary = run_benchmark(driver, options, ids, args)
        finally:
            driver.close()
    except Exception as exc:
        print(json_text(dict(status='error', error_type=type(exc).__name__, error=str(exc))), file=sys.stderr)
        return 1
    print(json_text(summary, pretty=True))
    statuses = summary['context_statuses']
    return 1 if statuses.get('error') or statuses.get('unavailable') else 0


if __name__ == '__main__':
    raise SystemExit(main())
