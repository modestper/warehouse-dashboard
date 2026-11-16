"""Copy one warehouse run's induced subgraph to an EMPTY Neo4j database.

Source access is read-only. The target is never cleared or overwritten.
By default only a read-only plan is printed; --apply performs the copy.
Neo4j native property values pass directly through Bolt without JSON conversion.
"""
import argparse
from collections import Counter, defaultdict
from contextlib import ExitStack
import getpass
import json
import os
from pathlib import Path
import time

LABEL = '__WarehouseRunCopy'
KEY = '__warehouse_run_copy_id'
CONSTRAINT = '__warehouse_run_copy_identity'


def scope(alias):
    return f'(coalesce({alias}.run_id=$run,false) OR ({alias}:Run AND coalesce({alias}.id=$run,false)))'


def ident(value):
    if not isinstance(value, str) or not value:
        raise ValueError('Empty graph identifier')
    return '`' + value.replace('`', '``') + '`'


def batches(records, size):
    rows = []
    for record in records:
        rows.append(dict(record))
        if len(rows) == size:
            yield rows
            rows = []
    if rows:
        yield rows


def run_counts(session, run):
    nodes = session.run('MATCH (n) WHERE ' + scope('n') +
                        ' RETURN count(n) AS n', run=run).single()['n']
    edges = session.run('MATCH (a)-[r]->(b) WHERE ' + scope('a') + ' AND ' + scope('b') +
                        ' RETURN count(r) AS n', run=run).single()['n']
    crossing = session.run('MATCH (a)-[r]->(b) WHERE (' + scope('a') + ') <> (' + scope('b') +
                           ') RETURN type(r) AS type,count(r) AS n ORDER BY type', run=run).data()
    # Include only head metadata, never entire checkpoint payloads in reports.
    heads = session.run('MATCH (n) WHERE (n:Run AND n.id=$run) OR '
                        '(n:WarehouseLiveCheckpoint AND n.run_id=$run) OR '
                        '(n:WHRun AND n.run_id=$run) '
                        'RETURN elementId(n) AS id, n.latest_step AS latest_step,'
                        'n.updated_tick AS updated_tick,n.seq AS seq,n.tick AS tick,'
                        'n.committed_seq AS committed_seq ORDER BY id', run=run).data()
    return {'nodes': nodes, 'relationships': edges, 'cross_boundary_relationships': crossing,
            'heads': heads}


def empty_target(session):
    if session.run('MATCH (n) RETURN n LIMIT 1').single() is not None:
        raise ValueError('目标库不是空库，拒绝覆盖或合并；请使用全新的容器和数据卷')
    if session.run('SHOW CONSTRAINTS YIELD name WHERE name=$name RETURN name',
                   name=CONSTRAINT).single() is not None:
        raise ValueError('目标存在上次复制标记，请使用全新的空库')


def write_nodes(tx, rows):
    groups = defaultdict(list)
    for row in rows:
        if LABEL in row['labels'] or KEY in row['props']:
            raise ValueError('源节点使用了复制工具的保留标签/属性，拒绝覆盖')
        groups[tuple(sorted(row['labels']))].append(row)
    written = 0
    for labels, group in groups.items():
        label_text = ''.join(':' + ident(label) for label in (*labels, LABEL))
        result = tx.run('UNWIND $rows AS row CREATE (n' + label_text + ') '
                        'SET n=row.props SET n.' + ident(KEY) + '=row.id '
                        'RETURN count(n) AS n', rows=group).single()
        if result['n'] != len(group):
            raise RuntimeError('Node batch incomplete')
        written += result['n']
    return written


def write_edges(tx, rows):
    groups = defaultdict(list)
    for row in rows:
        groups[row['type']].append(row)
    written = 0
    for kind, group in groups.items():
        query = ('UNWIND $rows AS row '
                 f'MATCH (a:{ident(LABEL)} {{{ident(KEY)}:row.start}}) '
                 f'MATCH (b:{ident(LABEL)} {{{ident(KEY)}:row.end}}) '
                 f'CREATE (a)-[r:{ident(kind)}]->(b) SET r=row.props RETURN count(r) AS n')
        result = tx.run(query, rows=group).single()
        if result['n'] != len(group):
            raise RuntimeError('Relationship endpoints missing; target copy is incomplete')
        written += result['n']
    return written


def schema_for(session, labels, types):
    clauses = [
        'SHOW CONSTRAINTS YIELD name,entityType,labelsOrTypes,createStatement '
        'RETURN name,entityType,labelsOrTypes,createStatement',
        'SHOW INDEXES YIELD name,type,entityType,labelsOrTypes,owningConstraint,createStatement '
        "WHERE owningConstraint IS NULL AND type <> 'LOOKUP' "
        'RETURN name,entityType,labelsOrTypes,createStatement',
    ]
    result = []
    for query in clauses:
        for row in session.run(query):
            selected = types if row['entityType'] == 'RELATIONSHIP' else labels
            if selected.intersection(row['labelsOrTypes'] or []):
                if row['name'] == CONSTRAINT:
                    raise ValueError('源 schema 使用了复制工具保留名称')
                result.append(dict(row))
    return result


def copy_run(source, target, run, *, size=500, copy_schema=True, progress=None):
    empty_target(target)
    before = run_counts(source, run)
    if not before['nodes']:
        raise ValueError('源库中没有找到指定 runId 的节点')
    target.run(f'CREATE CONSTRAINT {ident(CONSTRAINT)} FOR (n:{ident(LABEL)}) '
               f'REQUIRE n.{ident(KEY)} IS UNIQUE').consume()
    target.run('CALL db.awaitIndexes(300)').consume()
    copied_nodes = copied_edges = 0
    label_counts, type_counts = Counter(), Counter()
    records = source.run('MATCH (n) WHERE ' + scope('n') +
                         ' RETURN elementId(n) AS id,labels(n) AS labels,properties(n) AS props', run=run)
    for rows in batches(records, size):
        copied_nodes += target.execute_write(write_nodes, rows)
        for row in rows:
            label_counts.update(row['labels'])
        if progress:
            progress('nodes', copied_nodes)
    records = source.run('MATCH (a)-[r]->(b) WHERE ' + scope('a') + ' AND ' + scope('b') +
                         ' RETURN elementId(a) AS start,elementId(b) AS end,'
                         'type(r) AS type,properties(r) AS props', run=run)
    for rows in batches(records, size):
        copied_edges += target.execute_write(write_edges, rows)
        type_counts.update(row['type'] for row in rows)
        if progress:
            progress('relationships', copied_edges)
    after = run_counts(source, run)
    if before != after or copied_nodes != before['nodes'] or copied_edges != before['relationships']:
        raise RuntimeError('复制期间源 run 发生变化或计数不一致；目标保留为不完整副本，请停止源 run 写入后用新空库重试')
    node_count = target.run('MATCH (n) RETURN count(n) AS n').single()['n']
    edge_count = target.run('MATCH ()-[r]->() RETURN count(r) AS n').single()['n']
    if (node_count, edge_count) != (copied_nodes, copied_edges):
        raise RuntimeError('目标节点或关系总数不一致')
    target_labels = {r['label']: r['n'] for r in target.run(
        'MATCH (n) UNWIND labels(n) AS label RETURN label,count(*) AS n') if r['label'] != LABEL}
    target_types = {r['type']: r['n'] for r in target.run(
        'MATCH ()-[r]->() RETURN type(r) AS type,count(*) AS n')}
    if target_labels != dict(label_counts) or target_types != dict(type_counts):
        raise RuntimeError('目标标签/关系类型统计不一致')
    definitions = schema_for(source, set(label_counts), set(type_counts)) if copy_schema else []
    for row in definitions:
        target.run(row['createStatement']).consume()
    target.run('CALL db.awaitIndexes(600)').consume()
    # Keep the temporary mapping until verification and schema creation succeed.
    while True:
        result = target.run(f'MATCH (n:{ident(LABEL)}) WITH n LIMIT $batch '
                            f'REMOVE n:{ident(LABEL)} REMOVE n.{ident(KEY)} RETURN count(n) AS n',
                            batch=size).single()['n']
        if not result:
            break
    target.run('DROP CONSTRAINT ' + ident(CONSTRAINT)).consume()
    return {'status': 'complete', 'run_id': run, 'nodes': copied_nodes,
            'relationships': copied_edges, 'labels': dict(label_counts),
            'relationship_types': dict(type_counts),
            'schema_created': [row['name'] for row in definitions],
            'excluded_cross_boundary_relationships': before['cross_boundary_relationships'],
            'source_unchanged_by_counts_and_heads': before == after,
            'consistency_requirement': 'source run must remain quiescent for the entire copy; read-committed is not a snapshot'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--source-uri', default='bolt://127.0.0.1:47687')
    parser.add_argument('--source-database', default='neo4j')
    parser.add_argument('--source-user', default='neo4j')
    parser.add_argument('--target-uri', default='bolt://127.0.0.1:48687')
    parser.add_argument('--target-database', default='neo4j')
    parser.add_argument('--target-user', default='neo4j')
    parser.add_argument('--batch-size', type=int, default=500)
    parser.add_argument('--copy-schema', action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if not args.run_id.strip() or not 1 <= args.batch_size <= 10000:
        parser.error('run-id must be nonempty; batch-size must be 1..10000')
    args.run_id = args.run_id.strip()
    from neo4j import GraphDatabase, READ_ACCESS
    source_password = os.environ.get('SOURCE_NEO4J_PASSWORD') or getpass.getpass('源 Neo4j 密码：')
    target_password = os.environ.get('TARGET_NEO4J_PASSWORD') or getpass.getpass('目标 Neo4j 密码：')
    report = {'status': 'starting', 'run_id': args.run_id, 'source_uri': args.source_uri,
              'source_database': args.source_database, 'target_uri': args.target_uri,
              'target_database': args.target_database, 'mode': 'apply' if args.apply else 'plan'}
    started, last_log = time.monotonic(), time.monotonic()
    def progress(kind, count):
        nonlocal last_log
        if time.monotonic() - last_log >= 10:
            print(json.dumps({'phase':kind,'copied':count}), flush=True)
            last_log = time.monotonic()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    # Reports never silently overwrite earlier experiment records.
    with args.report.open('x', encoding='utf-8') as output:
        try:
            with ExitStack() as stack:
                src_driver = stack.enter_context(GraphDatabase.driver(args.source_uri,
                    auth=(args.source_user, source_password), connection_timeout=10))
                dst_driver = stack.enter_context(GraphDatabase.driver(args.target_uri,
                    auth=(args.target_user, target_password), connection_timeout=10))
                src = stack.enter_context(src_driver.session(database=args.source_database,
                    default_access_mode=READ_ACCESS, fetch_size=args.batch_size))
                dst = stack.enter_context(dst_driver.session(database=args.target_database))
                if args.apply:
                    report.update(copy_run(src, dst, args.run_id, size=args.batch_size,
                                           copy_schema=args.copy_schema, progress=progress))
                else:
                    empty_target(dst)
                    report.update(status='plan', source=run_counts(src,args.run_id))
                print(json.dumps(report, ensure_ascii=False), flush=True)
        except Exception as exc:
            report.update(status='failed', error_type=type(exc).__name__,
                          target_may_be_partial=args.apply)
            # Exception messages from a driver can include server data. Reports
            # contain only exception type; traceback stays in the local terminal.
            raise
        finally:
            report['wall_seconds'] = time.monotonic() - started
            json.dump(report, output, ensure_ascii=False, indent=2)
            output.write('\n')


if __name__ == '__main__':
    main()
