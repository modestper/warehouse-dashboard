"""Read-only operation contexts for the current Warehouse100 graph.

The Cypher statements live in config/warehouse-context-queries.json. This
module has no Neo4j import or connection side effect. Production uses one
session per context and one auto-commit transaction per query. Observations
and potential helpers never authorize an action.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import math
from pathlib import Path
import re
import time

from .wide.capabilities import access_reason, profile, slot_requirement


CATALOG_PATH = Path(__file__).resolve().parents[1] / 'config/warehouse-context-queries.json'
DEFAULT_PARAMS = dict(max_state_age_ticks=4, max_peer_age_ticks=4,
                      max_snapshot_age_ticks=5, binding_fetch_limit=17,
                      evidence_fetch_limit=17, state_fetch_limit=65,
                      event_fetch_limit=33, result_fetch_limit=17)
PACKS = {
    'core': ('w0', 'w5'),
    'task_decision': ('w0', 'w1', 'w2', 'w3', 'w6', 'w12'),
    'task_execution': ('w0', 'w2', 'w3', 'w6', 'w7', 'w10'),
    'request_help': ('w0', 'w2', 'w3', 'w4', 'w7', 'w11'),
    'provide_help': ('w0', 'w2', 'w3', 'w4', 'w7', 'w11'),
    'energy_safety': ('w0', 'w5', 'w8', 'w10'),
    'recovery': ('w0', 'w3', 'w7', 'w8', 'w10'),
    'task_progress': ('w0', 'w3', 'w6', 'w8'),
    'memory_review': ('w0', 'w9', 'w11'),
    'local_awareness': ('w0', 'w4', 'w12', 'w13'),
    'data_readiness': ('w0', 'w8'),
}


@dataclass(frozen=True)
class ContextQuery:
    id: str
    name: str
    category: str
    description: str
    temporal_scope: str
    cypher: str
    requires_tasks: bool = False

    @property
    def required_params(self):
        return tuple(sorted(set(re.findall(r'\$([A-Za-z_][A-Za-z_0-9]*)', self.cypher))))


def load_catalog(path=CATALOG_PATH):
    raw = json.loads(Path(path).read_text(encoding='utf-8'))
    queries = {}
    for row in raw['queries']:
        query = ContextQuery(**row)
        # This is a built-in catalog sanity check, not a Cypher parser.
        if query.id in queries or re.search(r'\b(CREATE|MERGE|SET|DELETE|REMOVE|DROP|LOAD|FOREACH)\b', query.cypher):
            raise ValueError(f'查询目录包含重复编号或写操作：{query.id}')
        queries[query.id] = query
        if query.requires_tasks != ('task_ids' in query.required_params):
            raise ValueError(f'查询目录任务依赖声明不一致：{query.id}')
    if len(queries) < 6 or tuple(queries) != tuple(f'w{i}' for i in range(len(queries))):
        raise ValueError('查询目录应保留 w0 至 w5 并使用连续编号')
    return raw['catalog_version'], queries


CATALOG_VERSION, QUERIES = load_catalog()
PACKS['all'] = tuple(QUERIES)


def select_queries(selection='core', *, explicit_tasks=False):
    wanted = set()
    aliases = {query.name: query.id for query in QUERIES.values()}
    for item in selection.split(','):
        item = item.strip()
        if item in PACKS:
            wanted.update(PACKS[item])
        elif item in QUERIES or item in aliases:
            wanted.add(aliases.get(item, item))
        elif re.fullmatch(r'w\d+-w\d+', item):
            first, last = (int(part[1:]) for part in item.split('-'))
            if first >= len(QUERIES) or last >= len(QUERIES):
                raise ValueError(f'不存在的查询范围：{item}')
            ids = {f'w{i}' for i in range(min(first, last), max(first, last) + 1)}
            if not ids <= QUERIES.keys():
                raise ValueError(f'不存在的查询范围：{item}')
            wanted.update(ids)
        else:
            category = {q.id for q in QUERIES.values() if q.category == item}
            if not category:
                raise ValueError(f'未知查询、查询包或类别：{item}')
            wanted.update(category)
    automatic = {'w0'} - wanted
    if any(QUERIES[qid].requires_tasks for qid in wanted) and not explicit_tasks and 'w1' not in wanted:
        automatic.add('w1')
    return [q for q in QUERIES.values() if q.id in wanted | automatic], automatic


@dataclass(frozen=True)
class ContextOptions:
    run_id: str
    robot_id: int = 1
    tick: int | None = None
    selection: str = 'core'
    task_ids: tuple[str, ...] = ()
    max_tasks: int = 8
    history_ticks: int = 60
    params: dict = field(default_factory=dict)
    include_cypher: bool = False
    explain: bool = False


def integer(value, name, minimum=0, maximum=None):
    if type(value) is not int or value < minimum or maximum is not None and value > maximum:
        raise ValueError(f'{name} 必须是 >= {minimum}' + (f' 且 <= {maximum}' if maximum is not None else '') + ' 的整数')
    return value


def validate_options(options):
    if not isinstance(options.run_id, str) or not options.run_id.strip():
        raise ValueError('需要非空 --run-id 或 KG_RUN_ID')
    integer(options.robot_id, 'robot_id')
    if options.tick is not None:
        integer(options.tick, 'tick')
    integer(options.max_tasks, 'max_tasks', 1, 32)
    integer(options.history_ticks, 'history_ticks', 1)
    if any(not isinstance(t, str) or not t.strip() for t in options.task_ids):
        raise ValueError('task_ids 只能包含非空字符串')
    if len(set(options.task_ids)) > options.max_tasks:
        raise ValueError('显式任务数量超过 --max-tasks，请减少任务或提高上限（最大32）')
    unknown = set(options.params) - DEFAULT_PARAMS.keys() - {'from_tick'}
    if unknown:
        raise ValueError('不支持或不允许覆盖的参数：' + ', '.join(sorted(unknown)))
    for key, value in {**DEFAULT_PARAMS, **options.params}.items():
        integer(value, key, 2 if key.endswith('_fetch_limit') else 0,
                1001 if key.endswith('_fetch_limit') else None)
    select_queries(options.selection, explicit_tasks=bool(options.task_ids))


def json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if hasattr(value, 'iso_format'):
        return value.iso_format()
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    return str(value)


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def _json(value, expected, quality, name):
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
        if not isinstance(parsed, expected):
            raise ValueError('unexpected JSON type')
        return parsed
    except (ValueError, TypeError):
        quality['missing_fields'].append(name)
        return None


def _trim(row, key, fetch_limit, quality):
    values = row.get(key)
    if not isinstance(values, list):
        quality['missing_fields'].append(key)
        return []
    truncated = len(values) >= fetch_limit
    row.setdefault('truncated', {})[key] = truncated
    quality['truncated'][key] = quality['truncated'].get(key, False) or truncated
    row[key] = values[:fetch_limit - 1]
    return row[key]


def _job_tasks(jobs):
    result = set()
    for job in jobs or []:
        if isinstance(job, dict):
            if job.get('task_id'):
                result.add(job['task_id'])
            result.update(t for t in job.get('task_ids', []) if isinstance(t, str))
    return result


def _dataset_scope(row):
    """Optional static namespace; it never controls runtime observation reads."""
    linked = {item['id'] for item in row.get('datasets') or []
              if isinstance(item, dict) and isinstance(item.get('id'), str) and item['id']}
    declared = row.get('declared_dataset_id')
    declared = declared if isinstance(declared, str) and declared else None
    if len(linked) > 1 or declared and linked and declared not in linked:
        return None, 'ambiguous'
    return declared or next(iter(linked), None), 'declared' if declared else 'linked' if linked else 'absent'


def pool_candidates(pool, elapsed):
    if not pool or not pool.get('snapshot_fresh') or not finite(elapsed):
        return []
    busy = _job_tasks(pool.get('active_jobs'))
    return [task for task in pool.get('pending') or []
            if isinstance(task, dict) and isinstance(task.get('task_id'), str)
            and task.get('status') in {'waiting', 'accepted'}
            and finite(task.get('release_second')) and task['release_second'] <= elapsed
            and task['task_id'] not in busy]


def _capability(row, robot_type, *, remaining=False):
    endpoints = ['target'] if remaining else ['source', 'target']
    requirements, missing = [], []
    try:
        robot_profile = profile(robot_type)
    except (KeyError, ValueError):
        return dict(result='unknown', missing=['robot_type'])
    for endpoint in endpoints:
        location = row.get(endpoint)
        if not location:
            missing.append(endpoint)
            continue
        if location.get('kind') in {'inbound', 'outbound', 'logical-boundary'}:
            continue
        if location.get('kind') != 'slot':
            missing.append(endpoint + '_kind')
            continue
        if type(location.get('level')) is int and location['level'] > robot_profile['maxLevel']:
            return dict(result='rejected', reason='capability_level')
        rack = row.get(endpoint + '_rack')
        if not rack:
            missing.append(endpoint + '_rack')
            continue
        try:
            scene = dict(spec=row['warehouse_spec'], racks=[{**rack, 'id': location['rack_id']}])
            requirements.append(slot_requirement(location, scene))
        except (ValueError, TypeError, KeyError):
            missing.append(endpoint + '_requirements')
    if missing:
        return dict(result='unknown', missing=missing)
    reason = access_reason({'robot_type_key': robot_type}, requirements)
    return dict(result='rejected' if reason else 'passed', reason=reason)


def _resolve_geometry(row, basis, quality):
    bindings = {}
    for event in row.get('boundary_bindings', []):
        detail = _json(event.get('details_json') or event.get('payload'), dict, quality, 'boundary_event_json')
        if not detail or detail.get('endpoint') not in {'source', 'target'}:
            continue
        endpoint = detail['endpoint']
        candidate = dict(actual_id=detail.get('actual_id'), tick=event.get('tick'))
        previous = bindings.get(endpoint)
        if previous is None:
            bindings[endpoint] = candidate
        elif previous['tick'] == candidate['tick'] and previous['actual_id'] != candidate['actual_id']:
            previous['conflict'] = True
    ports = {port.get('location_id'): port for port in row.get('physical_ports') or []}
    resolved = {}
    for endpoint in ('source', 'target'):
        location = row.get(endpoint)
        if location and location.get('kind') == 'logical-boundary':
            binding = bindings.get(endpoint)
            expected_kind = 'inbound' if endpoint == 'source' else 'outbound'
            complete_bindings = not row.get('truncated', {}).get('boundary_bindings')
            location = ports.get(binding.get('actual_id')) if binding and complete_bindings and not binding.get('conflict') else None
            if location and location.get('kind') != expected_kind:
                location = None
            if location is None:
                quality['missing_fields'].append(endpoint + '_physical_port')
        resolved[endpoint] = location if location and finite(location.get('x')) and finite(location.get('y')) else None
    row['resolved_endpoints'] = resolved
    row['capability'] = dict(
        complete_task=_capability(row, basis.get('robot_type')),
        remaining_delivery=_capability(row, basis.get('robot_type'), remaining=True))
    row['deadline_slack_seconds'] = None
    try:
        task = row['task']
        due = datetime.fromisoformat(task['due_time'])
        start = datetime.fromisoformat(row['dataset_start'])
        elapsed = (basis.get('step') or {}).get('elapsed')
        if finite(elapsed):
            row['deadline_slack_seconds'] = (due - start).total_seconds() - elapsed
    except (ValueError, TypeError, KeyError):
        quality['missing_fields'].append('deadline_time')


def normalize_rows(query, rows, params, basis=None, pool=None):
    """Preserve absent facts; add bounded, explicitly observational summaries."""
    quality = dict(missing_fields=[], warnings=[], truncated={})
    basis = basis or {}
    if not rows:
        quality['missing_fields'].append('result_not_observed')
        if query.id == 'w10':
            quality['recovery_evidence'] = 'not_observed'
        return quality
    if query.id == 'w0':
        row = rows[0]
        row['static_dataset_id'], row['static_dataset_scope'] = _dataset_scope(row)
        if row['static_dataset_scope'] == 'ambiguous':
            quality['warnings'].append('static_dataset_scope_ambiguous')
        if not row.get('observation_ready'):
            quality['warnings'].append('basis_observation_not_ready')
        row['config'] = _json(row.get('config_json'), dict, quality, 'config_json')
        state = row.get('self_state') or {}
        for key in ('stage', 'battery', 'x', 'y'):
            if state.get(key) is None or key in {'battery', 'x', 'y'} and not finite(state.get(key)):
                quality['missing_fields'].append('self_state.' + key)
        if finite(state.get('battery')) and not 0 <= state['battery'] <= 100:
            quality['missing_fields'].append('self_state.battery_out_of_range')
    elif query.id == 'w1':
        row = rows[0]
        if not row.get('has_snapshot'):
            quality['missing_fields'].append('dispatch_snapshot')
        if not row.get('snapshot_fresh'):
            quality['warnings'].append('dispatch_snapshot_not_fresh')
        for key, expected in [('pending', list), ('active_jobs', list), ('tasks', dict), ('ports', dict)]:
            row[key] = _json(row.get(key + '_json'), expected, quality, key + '_json')
        quality['truncated']['pending_possible'] = len(row.get('pending') or []) >= 200
        row['candidate_tasks'] = pool_candidates(row, (basis.get('step') or {}).get('elapsed'))
        row['candidate_scope'] = 'observed_pending_not_claimable_tasks'
    elif query.id == 'w2':
        for row in rows:
            _trim(row, 'boundary_bindings', params['binding_fetch_limit'], quality)
            if not row.get('task_exists'):
                quality['missing_fields'].append('task_definition:' + row['requested_task_id'])
            _resolve_geometry(row, basis, quality)
            for endpoint in ('source', 'target'):
                if (row.get(endpoint) or {}).get('kind') == 'slot' and not row.get(endpoint + '_rack'):
                    quality['missing_fields'].append(endpoint + '_rack:' + row['requested_task_id'])
    elif query.id == 'w3':
        for row in rows:
            for key in ('ownership_changes', 'handoffs', 'operation_events'):
                _trim(row, key, params['evidence_fetch_limit'], quality)
            sample = row.get('execution_sample')
            row['sample_age_ticks'] = params['t'] - sample['tick'] if sample and type(sample.get('tick')) is int else None
            row['ownership_interpretation'] = 'historical_evidence_not_current_authorization'
            if not row.get('task_observed', row.get('task_exists')):
                quality['missing_fields'].append('task_evidence:' + row['requested_task_id'])
    elif query.id == 'w4':
        own = basis.get('self_state') or {}
        threshold = (basis.get('config') or {}).get('charge_request_soc')
        valid_threshold = finite(threshold) and 0 <= threshold <= 100
        busy = {job.get('robot_id') for job in (pool or {}).get('active_jobs') or [] if isinstance(job, dict)}
        for row in rows:
            state = row.get('state') or {}
            row['distance_kind'] = 'euclidean_sort_only'
            row['distance_m'] = math.hypot(state['x'] - own['x'], state['y'] - own['y']) if all(
                finite(v) for v in (state.get('x'), state.get('y'), own.get('x'), own.get('y'))) else None
            row['potential_receiver'] = bool(row.get('state_fresh') and row['distance_m'] is not None
                and row.get('robot_type') and state.get('stage') == 'idle'
                and state.get('task_id') is None and state.get('cargo_id') is None
                and finite(state.get('battery')) and valid_threshold
                and 0 <= state['battery'] <= 100 and state['battery'] > threshold and row['robot_id'] not in busy)
            row['requires_confirmation'] = ['fault_free', 'physically_empty', 'no_job_or_help_commitment',
                                             'target_capability', 'route_energy_and_meeting_reservation']
            if not row.get('state_fresh'):
                quality['missing_fields'].append('fresh_peer_state:' + str(row['robot_id']))
        if not valid_threshold:
            quality['missing_fields'].append('config.charge_request_soc')
        expected = (basis.get('step') or {}).get('expected_robot_count')
        if type(expected) is int and len(rows) != expected - 1:
            quality['warnings'].append('peer_count_differs_from_step')
    elif query.id == 'w5':
        row = rows[0]
        states = _trim(row, 'states', params['state_fetch_limit'], quality)
        _trim(row, 'events', params['event_fetch_limit'], quality)
        ordered = sorted(states, key=lambda s: s['tick'])
        row['observed_soc_delta'] = (ordered[-1]['battery'] - ordered[0]['battery']) if len(ordered) >= 2 and all(
            finite(s.get('battery')) for s in (ordered[0], ordered[-1])) else None
        row['observed_displacement_m'] = math.hypot(ordered[-1]['x'] - ordered[0]['x'], ordered[-1]['y'] - ordered[0]['y']) if len(ordered) >= 2 and all(
            finite(s.get(k)) for s in (ordered[0], ordered[-1]) for k in ('x', 'y')) else None
        row['max_observed_gap_ticks'] = max((b['tick'] - a['tick'] for a, b in zip(ordered, ordered[1:])), default=None)
        if not states:
            quality['missing_fields'].append('history_states_not_observed')
    else:
        _normalize_insight(query.id, rows, params, quality)
    quality['missing_fields'] = sorted(set(quality['missing_fields']))
    return quality


def _normalize_insight(qid, rows, params, quality):
    if 'result_fetch_limit' in params:
        limit = params['result_fetch_limit']
        quality['truncated']['rows'] = len(rows) >= limit
        del rows[limit - 1:]
    if qid == 'w6':
        for row in rows:
            for output, start, end, first_count, last_count in [
                ('assignment_to_pick_seconds', 'assigned_second', 'picked_second', 'assignment_count', 'pickup_count'),
                ('pickup_to_completion_seconds', 'picked_second', 'completed_second', 'pickup_count', 'completion_count')]:
                unambiguous = row.get(first_count) == row.get(last_count) == 1 and row.get('requeue_count') == 0
                row[output] = row[end] - row[start] if unambiguous and finite(row.get(start)) and finite(row.get(end)) and row[end] >= row[start] else None
            row['timing_scope'] = 'observed_events_only'
            if not row.get('task_observed', row.get('task_exists')):
                quality['missing_fields'].append('task_events:' + row['requested_task_id'])
            if row.get('requeue_count', 0) or any(row.get(key, 0) > 1 for key in ('assignment_count', 'pickup_count', 'completion_count')):
                quality['warnings'].append('multiple_execution_cycles_require_event_correlation')
    elif qid == 'w7':
        for row in rows:
            if not row.get('goods_id'):
                quality['missing_fields'].append('cargo_identity:' + ','.join(row.get('selected_task_ids') or []))
            if row.get('cargo_identity_count', 0) > 1:
                quality['warnings'].append('cargo_identity_ambiguous')
            _trim(row, 'cargo_events', params['event_fetch_limit'], quality)
            _trim(row, 'handoffs', params['evidence_fetch_limit'], quality)
            fresh = [holder for holder in row.get('holder_observations') or [] if holder.get('state_fresh')]
            row['custody_scope'] = 'latest_robot_samples_and_history_not_inventory_authorization'
            row['conflicting_holder_observations'] = len(fresh) > 1
            if len(fresh) > 1:
                quality['warnings'].append('multiple_fresh_holder_observations_require_confirmation')
            if row.get('robots_with_samples', 0) < row.get('known_robots', 0):
                quality['missing_fields'].append('fleet_samples_for_custody')
    elif qid == 'w8':
        row = rows[0]
        row['observed_spatial_extent_m'] = math.hypot(row['max_x'] - row['min_x'], row['max_y'] - row['min_y']) if all(
            finite(row.get(key)) for key in ('min_x', 'max_x', 'min_y', 'max_y')) else None
        row['interpretation'] = 'sample_extent_not_fault_or_continuous_stop_duration'
        row['sampling_contract'] = 'complete_steps_do_not_require_one_agent_sample_each'
        if row.get('sample_count', 0) < 2:
            quality['missing_fields'].append('insufficient_samples_for_progress')
        if row.get('samples_with_x') != row.get('sample_count') or row.get('samples_with_y') != row.get('sample_count'):
            quality['missing_fields'].append('incomplete_sample_positions')
        if row.get('sample_count', 0) and row['observed_spatial_extent_m'] is None:
            quality['missing_fields'].append('finite_spatial_extent')
    elif qid == 'w9':
        for row in rows:
            row['observed_lateness_seconds'] = None
            try:
                if row.get('completion_event_count') == 1 and finite(row.get('completed_second')):
                    due_second = (datetime.fromisoformat(row['due_time']) - datetime.fromisoformat(row['dataset_start'])).total_seconds()
                    row['observed_lateness_seconds'] = row['completed_second'] - due_second
                else:
                    quality['warnings'].append('completion_time_absent_or_ambiguous')
            except (ValueError, TypeError, KeyError):
                quality['missing_fields'].append('completion_deadline:' + row['task_id'])
    elif qid == 'w10':
        stops = [row['marker']['tick'] for row in rows if row['marker']['type'] in {'robot_stopped', 'robot_power_isolated'}]
        resumes = [row['marker']['tick'] for row in rows if row['marker']['type'] in {'robot_stop_cleared', 'robot_reenabled'}]
        stop, resume = max(stops, default=-1), max(resumes, default=-1)
        quality['recovery_evidence'] = ('unknown_truncated' if quality['truncated']['rows'] else
            'resume_observed_after_stop' if stop >= 0 and resume > stop else
            'ambiguous_same_tick' if stop == resume and stop >= 0 else
            'stop_observed_without_later_resume' if stop > resume else 'no_stop_sequence_observed')
        quality['scope'] = 'robot_linked_markers_not_current_fault_registry'
    elif qid == 'w11':
        for row in rows:
            row['outcome'] = ('completion_observed_after_transfer' if row.get('completion_tick') is not None else
                              'later_execution_sample_observed' if row.get('post_transfer_sample') else 'not_observed')
            row['latency_unit'] = 'ticks'
    elif qid == 'w12':
        quality['scope'] = 'observed_primary_task_rack_references_not_physical_occupancy'
        for row in rows:
            if row.get('fresh_reference_count') == 0:
                quality['warnings'].append('rack_references_only_from_stale_samples')
            if not row.get('rack_id'):
                quality['missing_fields'].append('rack_id')
    elif qid == 'w13':
        observations = {}
        for row in sorted(rows, key=lambda value: (value['observed_tick'], value['commit_seq'])):
            row['ports'] = _json(row.get('ports_json'), dict, quality, 'ports_json')
            for port, count in (row['ports'] or {}).items():
                if type(count) is not int or count < 0:
                    quality['missing_fields'].append('port_count:' + port)
                    continue
                observations.setdefault(port, []).append(count)
        quality['port_trends'] = {port: dict(observations=len(counts), oldest_count=counts[0], newest_count=counts[-1],
            observed_delta=counts[-1] - counts[0] if len(counts) > 1 else None) for port, counts in observations.items()}
        quality['scope'] = 'returned_dispatch_samples_only_missing_port_keys_remain_unknown'


def _anchor_usable(row):
    step = row.get('step') or {}
    return (type(row.get('t')) is int and row['t'] >= 0 and row.get('step_count') == 1
            and step.get('complete') is True and step.get('tick') == row['t']
            and type(row.get('published_tick')) is int and row['t'] <= row['published_tick']
            and type(step.get('expected_robot_count')) is int and step['expected_robot_count'] > 0
            and type(step.get('received_robot_count')) is int
            and step['received_robot_count'] >= step['expected_robot_count'])


def execute_context(runner, options, *, context_index=0, query_factory=None):
    """Run context reads through a session or a caller-owned fixture runner.

    Each failed query is recorded independently. Later reads still require a
    valid anchor and, where applicable, observed task IDs.
    """
    validate_options(options)
    queries, automatic = select_queries(options.selection, explicit_tasks=bool(options.task_ids))
    context = dict(catalog_version=CATALOG_VERSION,
                   created_at=datetime.now().astimezone().isoformat(timespec='milliseconds'),
                   run_id=options.run_id, robot_id=options.robot_id,
                   context_index=context_index, requested_tick=options.tick, tick=None, dataset_id=None,
                   mode='explain' if options.explain else 'read', status='ok', queries={},
                   consistency='bounded_history_writer_epoch_unverified', action_authorized=False)
    records, basis, pool, task_ids = [], {}, None, list(dict.fromkeys(options.task_ids))
    anchor_ok = False
    started = time.perf_counter()
    for query in queries:
        params = {**DEFAULT_PARAMS, **options.params, 'run_id': options.run_id,
                  'robot_id': options.robot_id, 'at_tick': options.tick}
        if options.explain:
            params.update(t=options.tick or 0, dataset_id='__explain_only__',
                          task_ids=task_ids, from_tick=0)
        else:
            params.update(t=context['tick'], dataset_id=context['dataset_id'], task_ids=task_ids)
            params.setdefault('from_tick', max(0, (context['tick'] or 0) - options.history_ticks))
        params = {key: params[key] for key in query.required_params}
        record = dict(query_id=query.id, name=query.name, context_index=context_index,
                      run_id=options.run_id, robot_id=options.robot_id, automatic=query.id in automatic,
                      temporal_scope=query.temporal_scope, parameters=params, status='ok',
                      elapsed_ms=0.0, row_count=0)
        if options.include_cypher:
            record['cypher'] = query.cypher
        skip = ('anchor_unavailable' if query.id != 'w0' and not anchor_ok and not options.explain else
                'current_task_available' if query.id == 'w1' and query.id in automatic and task_ids else
                'no_observed_task_ids' if query.requires_tasks and not task_ids and not options.explain else
                'empty_history_window' if 'from_tick' in params and not options.explain and params['from_tick'] >= params['t'] else None)
        rows, quality = [], dict(missing_fields=[], warnings=[], truncated={})
        if skip:
            record.update(status='skipped', reason=skip)
        else:
            began = time.perf_counter()
            try:
                statement = ('EXPLAIN ' if options.explain else '') + query.cypher
                result = runner.run(query_factory(statement) if query_factory else statement, **params)
                rows = [dict(row) for row in result]
                summary = result.consume()
                record.update(row_count=len(rows), server_available_ms=summary.result_available_after,
                              server_consumed_ms=summary.result_consumed_after)
            except Exception as exc:
                record.update(status='error', error_type=type(exc).__name__, error=str(exc))
                context['status'] = 'error'
            record['elapsed_ms'] = (time.perf_counter() - began) * 1000
            if record['status'] == 'ok' and not options.explain:
                try:
                    quality = normalize_rows(query, rows, params, basis, pool)
                except (ValueError, TypeError, KeyError, AttributeError) as exc:
                    quality['missing_fields'].append('invalid_result_shape')
                    record['processing_error'] = str(exc)
                if query.id == 'w0':
                    basis = rows[0] if len(rows) == 1 else {}
                    anchor_ok = len(rows) == 1 and _anchor_usable(basis)
                    if anchor_ok:
                        context.update(tick=basis['t'], dataset_id=_dataset_scope(basis)[0],
                                       elapsed=(basis.get('step') or {}).get('elapsed'))
                        own_task = (basis.get('self_state') or {}).get('task_id')
                        if not options.task_ids and isinstance(own_task, str) and own_task:
                            task_ids.append(own_task)
                    else:
                        context['status'] = 'unavailable'
                elif query.id == 'w1':
                    pool = rows[0] if rows else None
                    if not options.task_ids:
                        candidates = [task['task_id'] for task in (pool or {}).get('candidate_tasks') or []]
                        task_ids = list(dict.fromkeys(task_ids + candidates))[:options.max_tasks]
        if record['status'] == 'ok' and (quality['missing_fields'] or quality['warnings']
                or any(quality['truncated'].values())) and context['status'] == 'ok':
            context['status'] = 'incomplete'
        context['queries'][query.id] = dict(name=query.name, status=record['status'],
                                            reason=record.get('reason'), rows=rows, quality=quality)
        records.append(record)
    context['elapsed_ms'] = (time.perf_counter() - started) * 1000
    context['task_ids'] = task_ids
    return json_safe(context), json_safe(records)


def read_context(driver, options, *, database='neo4j', timeout=30.0, context_index=0):
    """One read session per context; session.run auto-commits each query.

    The timeout applies to each implicit transaction. Consuming its result in
    execute_context finishes that transaction before the next query starts.
    """
    from neo4j import Query

    with driver.session(database=database, default_access_mode='READ') as session:
        context, records = execute_context(session, options, context_index=context_index,
            query_factory=lambda statement: Query(statement, timeout=timeout))
        context['transaction_mode'] = 'auto_commit_per_query'
        for record in records:
            record['transaction_mode'] = 'auto_commit_per_query'
        return context, records
