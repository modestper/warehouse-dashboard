"""Start the complete Neo4j live stack without a local SQLite journal.

Run via ./run_in_lade.sh python ops/start_neo4j_live.py. Database credentials
come from NEO4J_* (warehouse aliases are accepted); a missing password is
prompted without echo. KG_WRITE_MODE selects main/ray/process write ownership.
"""
import argparse
import getpass
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / 'config/live-datasets.json'
sys.path.insert(0, str(ROOT))
from ops.warehouse_kg_config import Neo4jConnection, WRITE_MODES, env_bool, knowledge_backend, state_source


class DatabaseSelectionError(RuntimeError):
    """The connection works, but its explicitly selected database is absent."""


def verify_database(driver, database, run_id=None):
    from neo4j.exceptions import Neo4jError
    try:
        with driver.session(database=database) as session:
            session.run('RETURN 1 AS connected').consume()
            if run_id:
                for query in ('MATCH (r:Run {id:$run_id}) RETURN r.id AS existing',
                              'MATCH (h:WarehouseLiveCheckpoint {run_id:$run_id}) RETURN h.run_id AS existing'):
                    if session.run(query, run_id=run_id).single() is not None:
                        raise ValueError(f'KG_RUN_ID 已存在：{run_id}；请使用新的运行编号')
    except Neo4jError as exc:
        if exc.code != 'Neo.ClientError.Database.DatabaseNotFound':
            raise
        names = []
        try:
            with driver.session(database='system', default_access_mode='READ') as session:
                names = [row['name'] for row in session.run('SHOW DATABASES YIELD name RETURN name').data()]
        except Exception:
            pass  # Listing privileges are optional; preserve the original diagnosis.
        available = '；现有数据库：' + '、'.join(names) if names else ''
        raise DatabaseSelectionError(
            f'已连接 Neo4j，但数据库“{database}”不存在{available}。'
            'NEO4J_DATABASE 只选择数据库，不会创建它。'
            '请先创建目标库；首次 Docker 初始化需在新数据卷中设置 '
            f'NEO4J_initial_dbms_default__database={database}。'
            '已有数据卷不会因为修改该参数而自动改名。'
        ) from exc


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    connection = Neo4jConnection.from_env(default_database='warehouse')
    p.add_argument('--database', default=connection.database)
    p.add_argument('--uri', default=connection.uri)
    p.add_argument('--user', default=connection.user)
    p.add_argument('--kg-backend', type=knowledge_backend, default=knowledge_backend(os.environ.get('KG_BACKEND', 'neo4j')))
    p.add_argument('--kg-write-mode', choices=WRITE_MODES, default=os.environ.get('KG_WRITE_MODE', 'main'))
    p.add_argument('--state-source', '--kg-state-source', type=state_source,
                   default=state_source(os.environ.get('KG_STATE_SOURCE', 'ray')))
    p.add_argument('--run-id', default=os.environ.get('KG_RUN_ID'))
    p.add_argument('--record-agent-state', action=argparse.BooleanOptionalAction,
                   default=env_bool('KG_RECORD_AGENT_STATE'))
    p.add_argument('--registry', type=Path, default=DEFAULT_REGISTRY)
    p.add_argument('--dataset-id', default='medium1728', help='registered alias; use --list-datasets to see available inputs')
    p.add_argument('--list-datasets', action='store_true', help='list registered inputs offline without connecting or starting services')
    p.add_argument('--port', type=int, default=14583)
    p.add_argument('--host', choices=('127.0.0.1', 'localhost'), default='127.0.0.1')
    p.add_argument('--backend', choices=('ray',), default='ray', help='robot execution backend, independent of KG_WRITE_MODE')
    p.add_argument('--ray-address', default=os.environ.get('RAY_ADDRESS') or 'local', help='local starts a private Ray cluster; or supply an existing head')
    p.add_argument('--ray-num-cpus', type=int)
    p.add_argument('--ray-actor-cpus', type=float)
    p.add_argument('--graph-state-interval', type=int, default=1 if os.environ.get('KG_RECORD_AGENT_STATE')=='1' else 4)
    p.add_argument('--speed', type=float, default=1.)
    p.add_argument('--output-root', type=Path, default=ROOT / 'data/neo4j-runs')
    p.add_argument('--skip-import', action='store_true', help='use static data already imported into this database')
    p.add_argument('--check-only', action='store_true', help='check credentials/database/dependencies/run ID without importing or starting services')
    return p


def selected_dataset(args):
    from ops.warehouse_live import dataset_descriptions, load_registry
    datasets, _ = load_registry(args.registry)
    if args.dataset_id not in datasets:
        raise ValueError(f'未知 dataset-id：{args.dataset_id}；可选：' + ', '.join(datasets))
    description = next(row for row in dataset_descriptions(datasets)['datasets'] if row['id'] == args.dataset_id)
    if not description['available']:
        raise ValueError(f'数据集 {args.dataset_id} 的输入或配置文件不存在')
    spec = datasets[args.dataset_id]
    if not spec.get('locations_csv') or not spec.get('graph_dataset_id'):
        raise ValueError('Neo4j 启动器要求注册项提供 locations_csv 和独立 graph_dataset_id')
    return spec


def prepare_environment(args):
    spec = selected_dataset(args)
    from backend.warehouse_graph import load_graph_package
    load_graph_package(spec['dataset'], spec['scene'], spec['locations_csv'], dataset_id=spec['graph_dataset_id'])
    from neo4j import GraphDatabase
    import ray  # noqa: F401 -- fail before launching anything if unavailable
    import fastapi  # noqa: F401
    import httpx  # noqa: F401
    env = os.environ.copy()
    password = Neo4jConnection.from_env(env).password
    if not password:
        password = getpass.getpass('Neo4j 密码：')
    if not password:
        raise ValueError('数据库密码不能为空')
    connection = Neo4jConnection(args.uri, args.user, password, args.database)
    env = connection.environment(env)
    env.update(KG_BACKEND=args.kg_backend, KG_WRITE_MODE=args.kg_write_mode,
               KG_STATE_SOURCE='ray' if args.state_source=='memory' else args.state_source,
               KG_RECORD_AGENT_STATE='1' if args.record_agent_state else '0',
               WAREHOUSE_NEO4J_DATASET_ID=spec['graph_dataset_id'])
    if args.run_id:
        env['KG_RUN_ID'] = args.run_id
    with GraphDatabase.driver(args.uri, auth=(args.user, password), connection_timeout=10) as driver:
        verify_database(driver, args.database, args.run_id)
    print(f'已连接 {args.uri}，数据库 {args.database}；数据集 {args.dataset_id}；'
          f'写入 {args.kg_write_mode}；状态 {args.state_source}；runId {args.run_id or "自动生成"}', flush=True)
    return env, spec


def ensure_port_free(port):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', port))


def live_command(args):
    spec = selected_dataset(args)
    actor_cpus = args.ray_actor_cpus
    if actor_cpus is None:
        actor_cpus = min(.1, args.ray_num_cpus/100) if args.ray_num_cpus is not None else .1
    command = [sys.executable, str(ROOT / 'ops/warehouse_live.py'),
            '--registry', str(args.registry.resolve()),
            '--dataset-id', args.dataset_id, '--output-root', str(args.output_root),
            '--mode', 'rules', '--knowledge-backend', 'neo4j', '--no-journal',
            '--mobility', spec.get('mobility', 'legacy'), '--robot-processes', 'ray', '--ray-address', args.ray_address,
            '--ray-actor-cpus', str(actor_cpus), '--robot-graph', '--kg-write-mode', args.kg_write_mode,
            '--kg-state-source', args.state_source, '--graph-state-interval', str(args.graph_state_interval),
            '--record-agent-state' if args.record_agent_state else '--no-record-agent-state',
            '--initial-speed', str(args.speed), '--autostart', '--host', args.host, '--port', str(args.port)]
    if args.run_id is not None:
        command += ['--run-id', args.run_id]
    if args.ray_num_cpus is not None:
        command += ['--ray-num-cpus', str(args.ray_num_cpus)]
    return command


def stop_owned_process(process):
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGINT)
        process.wait(timeout=180)


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    if args.list_datasets:
        from ops.warehouse_live import dataset_descriptions, load_registry
        datasets, _ = load_registry(args.registry)
        print(json.dumps(dataset_descriptions(datasets, args.dataset_id), ensure_ascii=False, indent=2))
        return 0
    if not 1 <= args.port <= 65535:
        p.error('页面需要有效端口')
    if args.kg_backend != 'neo4j' or args.kg_write_mode not in WRITE_MODES:
        p.error('无 journal 启动器要求 KG_BACKEND=neo4j，KG_WRITE_MODE=main/ray/process')
    if args.run_id is not None:
        from ops.warehouse_live_runtime import run_name
        run_name(args.run_id)
        if (args.output_root / args.run_id).exists():
            p.error('运行输出已存在，请更换 KG_RUN_ID')
    if args.ray_num_cpus is not None and (args.ray_num_cpus < 1 or args.ray_address != 'local'):
        p.error('ray-num-cpus 只用于本次新建的 local 集群，且必须为正整数')
    if not 1 <= args.graph_state_interval <= 300:
        p.error('graph-state-interval 必须在 1～300 之间')
    if not .1 <= args.speed <= 120 or (args.ray_actor_cpus is not None and not 0 < args.ray_actor_cpus <= 1):
        p.error('speed 必须在 0.1～120 之间，ray-actor-cpus 必须在 (0,1] 之间')
    try:
        selected_dataset(args)
    except ValueError as exc:
        p.error(str(exc))
    try:
        env, spec = prepare_environment(args)
    except DatabaseSelectionError as exc:
        p.error(str(exc))
    if args.check_only:
        return 0
    ensure_port_free(args.port)
    if not args.skip_import:
        subprocess.run([sys.executable, str(ROOT / 'scripts/import_warehouse_neo4j.py'),
                        '--package-dir', spec['dataset'], '--layout', spec['scene'],
                        '--locations', spec['locations_csv'], '--graph-dataset-id', spec['graph_dataset_id'],
                        '--apply'], cwd=ROOT, env=env, check=True)
    live = None
    try:
        print(f'仿真页面：http://127.0.0.1:{args.port}/；无本地 SQLite journal；Ctrl+C 结束本次服务', flush=True)
        live = subprocess.Popen(live_command(args), cwd=ROOT, env=env, start_new_session=True)
        while live.poll() is None:
            time.sleep(.2)
        return live.returncode
    except KeyboardInterrupt:
        return 130
    finally:
        # The live process closes its acknowledged writer before exiting.
        stop_owned_process(live)


if __name__ == '__main__':
    raise SystemExit(main())
