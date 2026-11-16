"""Operation-context dependencies, time bounds, missing facts and CLI output."""
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backend.robot_operation_context import (
    ContextOptions, DEFAULT_PARAMS, QUERIES, execute_context, normalize_rows,
    read_context, select_queries, validate_options,
)
from scripts.warehouse_llm_context_queries import main, parse_args, run_benchmark, summary_from


def basis_row(**changes):
    row = dict(run_id='run-a', t=20, published_tick=22, step_count=1,
               step=dict(tick=20, complete=True, expected_robot_count=3,
                         received_robot_count=3, elapsed=40),
               datasets=[dict(id='dataset:a')], robot_type='A1',
               self_state=dict(tick=19, stage='idle', battery=80, x=1., y=2., task_id=None, cargo_id=None),
               state_age_ticks=1, config_json='{"charge_request_soc":35}', observation_ready=True)
    row.update(changes)
    return row


def pool_row(**changes):
    row = dict(has_snapshot=True, snapshot_fresh=True, observed_tick=19, age_ticks=1,
               pending_json=json.dumps([
                   dict(task_id='ready', status='waiting', release_second=30),
                   dict(task_id='future', status='waiting', release_second=41),
                   dict(task_id='busy', status='accepted', release_second=10)]),
               active_jobs_json='[{"robot_id":2,"task_ids":["busy"]}]', tasks_json='{}', ports_json='{}')
    row.update(changes)
    return row


class FakeResult:
    def __init__(self, rows, consume_error=None):
        self.rows = deepcopy(rows)
        self.consume_error = consume_error
        self.consumed = False

    def __iter__(self):
        return iter(self.rows)

    def consume(self):
        if self.consume_error:
            raise self.consume_error
        self.consumed = True
        return SimpleNamespace(result_available_after=1, result_consumed_after=2)


class FakeTransaction:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []
        self.results = []
        self.rolled_back = False

    def run(self, cypher, **params):
        self.calls.append((cypher, params))
        response = self.responses[len(self.calls) - 1]
        if isinstance(response, Exception):
            raise response
        result = response if isinstance(response, FakeResult) else FakeResult(response)
        self.results.append(result)
        return result

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def rollback(self):
        self.rolled_back = True


class FakeSession:
    def __init__(self, responses):
        self.runner = FakeTransaction(responses)
        self.explicit_transaction_started = False

    def run(self, query, **params):
        return self.runner.run(query, **params)

    def begin_transaction(self, **kwargs):
        self.explicit_transaction_started = True
        raise AssertionError('查询上下文应逐条自动提交，不应开启共享事务')

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class FakeDriver:
    def __init__(self, responses):
        self.responses = responses
        self.sessions = []

    def session(self, **kwargs):
        session = FakeSession(deepcopy(self.responses))
        session.session_options = kwargs
        self.sessions.append(session)
        return session


class ContextTests(unittest.TestCase):
    def test_selection_is_deduplicated_and_dependencies_are_explicit(self):
        queries, automatic = select_queries('task_geometry,w2,w3')
        self.assertEqual([q.id for q in queries], ['w0', 'w1', 'w2', 'w3'])
        self.assertEqual(automatic, {'w0', 'w1'})
        queries, automatic = select_queries('w2', explicit_tasks=True)
        self.assertEqual([q.id for q in queries], ['w0', 'w2'])
        self.assertEqual(len(select_queries('w5-w0')[0]), 6)
        with self.assertRaises(ValueError):
            select_queries('w0-w14')
        with self.assertRaises(ValueError):
            select_queries('w0-w1000000000')

    def test_scope_cannot_be_overridden_and_fetch_limits_are_bounded(self):
        for params in [dict(t=999), dict(run_id='foreign'), dict(dataset_id='foreign'),
                       dict(max_peer_age_ticks=True), dict(event_fetch_limit=1), dict(event_fetch_limit=1002)]:
            with self.subTest(params=params), self.assertRaises(ValueError):
                validate_options(ContextOptions('run-a', params=params))

    def test_anchor_and_pool_populate_parameters_without_looking_at_future_tasks(self):
        tx = FakeTransaction([[basis_row()], [pool_row()], [], [], [], []])
        context, records = execute_context(tx, ContextOptions('run-a', tick=20, selection='task_decision'))
        self.assertEqual(context['tick'], 20)
        self.assertEqual(context['task_ids'], ['ready'])
        for _, params in tx.calls[1:]:
            self.assertEqual(params['t'], 20)
            self.assertEqual(params['run_id'], 'run-a')
            if 'dataset_id' in params:
                self.assertEqual(params['dataset_id'], 'dataset:a')
        self.assertEqual(tx.calls[2][1]['task_ids'], ['ready'])
        self.assertEqual(len(records), 6)

    def test_explicit_task_reads_do_not_fetch_pool_and_use_anchored_dataset(self):
        tx = FakeTransaction([[basis_row()], []])
        context, _ = execute_context(tx, ContextOptions('run-a', selection='w2', task_ids=('specific',)))
        self.assertEqual(len(tx.calls), 2)
        self.assertEqual(tx.calls[1][1]['task_ids'], ['specific'])
        self.assertEqual(context['dataset_id'], 'dataset:a')

    def test_own_task_avoids_automatic_pool_read(self):
        basis = basis_row()
        basis['self_state']['task_id'] = 'own-task'
        tx = FakeTransaction([[basis], []])
        context, records = execute_context(tx, ContextOptions('run-a', selection='w3'))
        self.assertEqual(context['task_ids'], ['own-task'])
        self.assertEqual(records[1]['reason'], 'current_task_available')
        self.assertEqual(len(tx.calls), 2)

    def test_missing_and_invalid_anchor_skip_dependent_reads_without_fallback(self):
        for row in [None, basis_row(step_count=0, step=None), basis_row(published_tick=19)]:
            with self.subTest(row=row):
                tx = FakeTransaction([[] if row is None else [row]])
                context, records = execute_context(tx, ContextOptions('run-a', selection='all'))
                self.assertEqual(context['status'], 'unavailable')
                self.assertEqual(len(tx.calls), 1)
                self.assertTrue(all(r['status'] == 'skipped' for r in records[1:]))

    def test_absent_dataset_does_not_block_runtime_reads_or_task_discovery(self):
        tx = FakeTransaction([[basis_row(datasets=[])], [pool_row()], [], [], [], [], [], []])
        context, records = execute_context(tx, ContextOptions('run-a', selection='w1,w3-w6,w8,w13'))
        self.assertEqual(context['tick'], 20)
        self.assertIsNone(context['dataset_id'])
        self.assertEqual(context['task_ids'], ['ready'])
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        self.assertTrue(all('dataset_id' not in params for _, params in tx.calls))

    def test_static_namespace_is_optional_and_ambiguity_only_affects_static_reads(self):
        cases = [(dict(datasets=[], declared_dataset_id='declared'), 'declared'),
                 (dict(datasets=[]), None),
                 (dict(datasets=[dict(id='a'), dict(id='b')]), None),
                 (dict(declared_dataset_id='different'), None)]
        for changes, expected in cases:
            with self.subTest(changes=changes):
                tx = FakeTransaction([[basis_row(**changes)], [], []])
                context, records = execute_context(tx, ContextOptions('run-a', selection='w2,w3', task_ids=('T',)))
                self.assertEqual(context['tick'], 20)
                self.assertEqual(context['dataset_id'], expected)
                self.assertEqual(tx.calls[1][1]['dataset_id'], expected)
                self.assertNotIn('dataset_id', tx.calls[2][1])
                self.assertTrue(all(r['status'] == 'ok' for r in records), records)
                self.assertNotEqual(context['status'], 'unavailable')

    def test_runtime_evidence_without_static_definition_is_usable(self):
        rows = [dict(requested_task_id='T', task_observed=True,
                     execution_sample=dict(tick=19), ownership_changes=[], handoffs=[], operation_events=[])]
        quality = normalize_rows(QUERIES['w3'], rows, dict(t=20, evidence_fetch_limit=17))
        self.assertEqual(quality['missing_fields'], [])
        timing = dict(requested_task_id='T', task_observed=True, assignment_count=1, pickup_count=1,
                      completion_count=1, requeue_count=0, assigned_second=1, picked_second=2, completed_second=3)
        quality = normalize_rows(QUERIES['w6'], [timing], {})
        self.assertEqual(quality['missing_fields'], [])
        self.assertEqual(timing['pickup_to_completion_seconds'], 1)

    def test_missing_static_definition_and_cargo_remain_explicit(self):
        geometry = dict(requested_task_id='T', task_exists=False, boundary_bindings=[])
        quality = normalize_rows(QUERIES['w2'], [geometry], DEFAULT_PARAMS)
        self.assertIn('task_definition:T', quality['missing_fields'])
        self.assertIsNone(geometry['deadline_slack_seconds'])
        self.assertEqual(geometry['capability']['complete_task']['result'], 'unknown')
        cargo = dict(goods_id=None, selected_task_ids=['T'], cargo_identity_count=2,
                     cargo_events=[], handoffs=[], holder_observations=[])
        quality = normalize_rows(QUERIES['w7'], [cargo], DEFAULT_PARAMS)
        self.assertIn('cargo_identity:T', quality['missing_fields'])
        self.assertIn('cargo_identity_ambiguous', quality['warnings'])

    def test_failed_query_or_commit_does_not_skip_later_auto_commit_queries(self):
        for failure in [RuntimeError('query failed'), FakeResult([pool_row()], consume_error=RuntimeError('commit failed'))]:
            with self.subTest(failure=failure):
                driver = FakeDriver([[basis_row()], failure, [], []])
                context, records = read_context(driver, ContextOptions('run-a', selection='w1,w2,w4', task_ids=('known',)))
                self.assertEqual(context['status'], 'error')
                self.assertEqual([r['status'] for r in records], ['ok', 'error', 'ok', 'ok'])
                calls = driver.sessions[0].runner.calls
                self.assertEqual(len(calls), 4)
                self.assertEqual(calls[2][1]['task_ids'], ['known'])
                self.assertEqual(calls[2][1]['t'], 20)
                self.assertEqual(calls[2][1]['dataset_id'], 'dataset:a')

    def test_failed_task_discovery_skips_only_queries_without_required_tasks(self):
        driver = FakeDriver([[basis_row()], RuntimeError('pool query failed'), []])
        context, records = read_context(driver, ContextOptions('run-a', selection='w1,w2,w4'))
        self.assertEqual([r['status'] for r in records], ['ok', 'error', 'skipped', 'ok'])
        self.assertEqual(records[2]['reason'], 'no_observed_task_ids')
        self.assertEqual(context['queries']['w4']['status'], 'ok')
        self.assertEqual(len(driver.sessions[0].runner.calls), 3)

    def test_failed_anchor_still_blocks_unscoped_followup_queries(self):
        driver = FakeDriver([RuntimeError('anchor failed')])
        context, records = read_context(driver, ContextOptions('run-a', selection='all'))
        self.assertEqual(context['status'], 'error')
        self.assertTrue(all(r['reason'] == 'anchor_unavailable' for r in records[1:]))
        self.assertEqual(len(driver.sessions[0].runner.calls), 1)

    def test_explain_submits_all_queries_without_needing_observed_tasks(self):
        tx = FakeTransaction([[] for _ in QUERIES])
        context, records = execute_context(tx, ContextOptions('run-a', selection='all', explain=True))
        self.assertEqual(context['mode'], 'explain')
        self.assertTrue(all(query.startswith('EXPLAIN ') for query, _ in tx.calls))
        self.assertEqual(len(records), 14)

    def test_read_context_uses_session_run_and_per_query_timeout(self):
        driver = FakeDriver([[basis_row()], [dict(states=[], events=[])]])
        context, records = read_context(driver, ContextOptions('run-a', selection='core'), database='custom', timeout=4)
        session = driver.sessions[0]
        self.assertEqual(session.session_options, dict(database='custom', default_access_mode='READ'))
        self.assertFalse(session.explicit_transaction_started)
        self.assertEqual(len(session.runner.calls), 2)
        self.assertTrue(all(query.timeout == 4 for query, _ in session.runner.calls))
        self.assertEqual([str(query) for query, _ in session.runner.calls], [QUERIES['w0'].cypher, QUERIES['w5'].cypher])
        self.assertTrue(all(result.consumed for result in session.runner.results))
        self.assertEqual(context['transaction_mode'], 'auto_commit_per_query')
        self.assertTrue(all(record['transaction_mode'] == 'auto_commit_per_query' for record in records))

    def test_truncation_is_preserved_across_multiple_task_rows(self):
        rows = [dict(requested_task_id='a', task_exists=True, execution_sample=None,
                     ownership_changes=[dict(tick=i) for i in range(3)], handoffs=[], operation_events=[]),
                dict(requested_task_id='b', task_exists=True, execution_sample=None,
                     ownership_changes=[], handoffs=[], operation_events=[])]
        quality = normalize_rows(QUERIES['w3'], rows, dict(t=20, evidence_fetch_limit=3))
        self.assertTrue(quality['truncated']['ownership_changes'])
        self.assertEqual(len(rows[0]['ownership_changes']), 2)
        self.assertFalse(rows[1]['truncated']['ownership_changes'])

    def test_malformed_or_missing_pool_is_not_interpreted_as_empty_ready_pool(self):
        for changes in [dict(pending_json='broken'), dict(has_snapshot=False, snapshot_fresh=False)]:
            row = pool_row(**changes)
            quality = normalize_rows(QUERIES['w1'], [row], DEFAULT_PARAMS, basis_row())
            self.assertTrue(quality['missing_fields'])
            self.assertEqual(row['candidate_tasks'], [])

    def test_missing_rack_and_logical_port_are_unknown_not_zero_reach_or_origin(self):
        row = dict(requested_task_id='t', task_exists=True,
                   task=dict(due_time='2026-10-27T00:05:00+08:00'), dataset_start='2026-10-27T00:00:00+08:00',
                   warehouse_spec=dict(levels=3, rackHeight=1.8),
                   source=dict(kind='slot', level=3, rack_id='R1', x=10, y=20), source_rack=None,
                   target=dict(kind='logical-boundary'), target_rack=None, physical_ports=[], boundary_bindings=[])
        quality = normalize_rows(QUERIES['w2'], [row], DEFAULT_PARAMS, basis_row())
        self.assertEqual(row['capability']['complete_task']['result'], 'rejected')
        self.assertEqual(row['capability']['remaining_delivery']['result'], 'passed')
        self.assertIsNone(row['resolved_endpoints']['target'])
        self.assertIn('target_physical_port', quality['missing_fields'])
        self.assertEqual(row['deadline_slack_seconds'], 260)
        row['source']['level'] = 1
        normalize_rows(QUERIES['w2'], [row], DEFAULT_PARAMS, basis_row())
        self.assertEqual(row['capability']['complete_task']['result'], 'unknown')

    def test_peer_filter_uses_run_soc_config_and_never_marks_candidate_committed(self):
        rows = [dict(robot_id=2, robot_type='X3', state_fresh=True,
                     state=dict(stage='idle', battery=36, x=4, y=6)),
                dict(robot_id=3, robot_type='A1', state_fresh=True,
                     state=dict(stage='idle', battery=35, x=1, y=2))]
        basis = basis_row(config=dict(charge_request_soc=35))
        normalize_rows(QUERIES['w4'], rows, DEFAULT_PARAMS, basis)
        self.assertTrue(rows[0]['potential_receiver'])
        self.assertFalse(rows[1]['potential_receiver'])
        self.assertEqual(rows[0]['distance_m'], 5)
        self.assertIn('physically_empty', rows[0]['requires_confirmation'])

    def test_feedback_handles_single_sample_without_inventing_consumption(self):
        rows = [dict(states=[dict(tick=10, battery=50, x=1, y=2)], events=[])]
        normalize_rows(QUERIES['w5'], rows, DEFAULT_PARAMS)
        self.assertIsNone(rows[0]['observed_soc_delta'])
        self.assertIsNone(rows[0]['observed_displacement_m'])


class InsightTests(unittest.TestCase):
    def test_new_task_queries_declare_and_resolve_dependencies(self):
        self.assertEqual(len(QUERIES), 14)
        self.assertEqual({q.id for q in QUERIES.values() if q.requires_tasks}, {'w2', 'w3', 'w6', 'w7'})
        queries, automatic = select_queries('cargo_history')
        self.assertEqual([q.id for q in queries], ['w0', 'w1', 'w7'])
        self.assertEqual(automatic, {'w0', 'w1'})
        queries, _ = select_queries('w6,w7', explicit_tasks=True)
        self.assertEqual([q.id for q in queries], ['w0', 'w6', 'w7'])

    def test_new_task_query_skips_empty_observed_pool(self):
        tx = FakeTransaction([[basis_row()], [pool_row(pending_json='[]')]])
        context, records = execute_context(tx, ContextOptions('run-a', selection='w6'))
        self.assertEqual(records[-1]['reason'], 'no_observed_task_ids')
        self.assertEqual(len(tx.calls), 2)
        self.assertEqual(context['task_ids'], [])

    def test_timing_does_not_mix_multiple_execution_cycles(self):
        row = dict(requested_task_id='T', task_exists=True, assignment_count=1, assigned_second=10,
                   pickup_count=1, picked_second=40, completion_count=1, completed_second=140, requeue_count=0)
        normalize_rows(QUERIES['w6'], [row], {})
        self.assertEqual(row['assignment_to_pick_seconds'], 30)
        self.assertEqual(row['pickup_to_completion_seconds'], 100)
        row['requeue_count'] = 1
        quality = normalize_rows(QUERIES['w6'], [row], {})
        self.assertIsNone(row['assignment_to_pick_seconds'])
        self.assertIsNone(row['pickup_to_completion_seconds'])
        self.assertIn('multiple_execution_cycles_require_event_correlation', quality['warnings'])

    def test_custody_conflict_is_an_observation_not_transfer_permission(self):
        row = dict(goods_id='G', cargo_events=[], handoffs=[], known_robots=4, robots_with_samples=3,
                   initial_locations=[dict(location_id='INITIAL')], holder_observations=[
                       dict(robot_id=1, state_fresh=True), dict(robot_id=2, state_fresh=True),
                       dict(robot_id=3, state_fresh=False)])
        quality = normalize_rows(QUERIES['w7'], [row], DEFAULT_PARAMS)
        self.assertTrue(row['conflicting_holder_observations'])
        self.assertIn('fleet_samples_for_custody', quality['missing_fields'])
        self.assertIn('not_inventory_authorization', row['custody_scope'])
        self.assertEqual(row['initial_locations'][0]['location_id'], 'INITIAL')

    def test_progress_uses_full_window_extent_without_inventing_missing_tick_count(self):
        row = dict(sample_count=5, sampled_tick_count=5, samples_with_x=5, samples_with_y=5,
                   min_x=0, max_x=10, min_y=0, max_y=0, observed_complete_ticks=100)
        quality = normalize_rows(QUERIES['w8'], [row], {})
        self.assertEqual(row['observed_spatial_extent_m'], 10)
        self.assertEqual(quality['missing_fields'], [])
        self.assertNotIn('missing_steps', row)
        self.assertIn('not_fault', row['interpretation'])

    def test_completion_feedback_uses_runtime_seconds_and_reports_ambiguous_completion(self):
        row = dict(task_id='T', completed_second=120, completion_event_count=1,
                   due_time='2026-10-27T00:01:40+08:00', dataset_start='2026-10-27T00:00:00+08:00')
        normalize_rows(QUERIES['w9'], [row], DEFAULT_PARAMS)
        self.assertEqual(row['observed_lateness_seconds'], 20)
        row['completion_event_count'] = 2
        quality = normalize_rows(QUERIES['w9'], [row], DEFAULT_PARAMS)
        self.assertIsNone(row['observed_lateness_seconds'])
        self.assertIn('completion_time_absent_or_ambiguous', quality['warnings'])

    def test_recovery_does_not_order_stop_and_clear_in_same_tick(self):
        rows = [dict(marker=dict(type='robot_stopped', tick=5)),
                dict(marker=dict(type='robot_stop_cleared', tick=5))]
        quality = normalize_rows(QUERIES['w10'], rows, DEFAULT_PARAMS)
        self.assertEqual(quality['recovery_evidence'], 'ambiguous_same_tick')
        rows[1]['marker']['tick'] = 6
        quality = normalize_rows(QUERIES['w10'], rows, DEFAULT_PARAMS)
        self.assertEqual(quality['recovery_evidence'], 'resume_observed_after_stop')
        quality = normalize_rows(QUERIES['w10'], [], DEFAULT_PARAMS)
        self.assertEqual(quality['recovery_evidence'], 'not_observed')

    def test_handoff_without_later_sample_is_unknown_rather_than_failed(self):
        rows = [dict(completion_tick=None, post_transfer_sample=None),
                dict(completion_tick=9, post_transfer_sample=dict(tick=9))]
        normalize_rows(QUERIES['w11'], rows, DEFAULT_PARAMS)
        self.assertEqual(rows[0]['outcome'], 'not_observed')
        self.assertEqual(rows[1]['outcome'], 'completion_observed_after_transfer')
        self.assertEqual(rows[1]['latency_unit'], 'ticks')

    def test_port_trend_does_not_fill_absent_port_with_zero(self):
        rows = [dict(observed_tick=10, commit_seq=10, ports_json='{"IN-1":3}'),
                dict(observed_tick=5, commit_seq=5, ports_json='{"IN-1":1,"IN-2":7}')]
        quality = normalize_rows(QUERIES['w13'], rows, DEFAULT_PARAMS)
        self.assertEqual(quality['port_trends']['IN-1']['observed_delta'], 2)
        self.assertEqual(quality['port_trends']['IN-2']['newest_count'], 7)
        self.assertIsNone(quality['port_trends']['IN-2']['observed_delta'])
        self.assertIn('missing_port_keys_remain_unknown', quality['scope'])

    def test_all_new_window_queries_obey_empty_window_guard(self):
        anchor = basis_row(t=0, published_tick=0, self_state=None, observation_ready=False,
                           step=dict(tick=0, complete=True, expected_robot_count=3, received_robot_count=3, elapsed=0))
        for qid in ['w8', 'w9', 'w11', 'w13']:
            with self.subTest(qid=qid):
                tx = FakeTransaction([[anchor]])
                _, records = execute_context(tx, ContextOptions('run-a', selection=qid, tick=0))
                self.assertEqual(records[-1]['reason'], 'empty_history_window')
                self.assertEqual(len(tx.calls), 1)


class CliTests(unittest.TestCase):
    def test_list_and_dry_run_require_no_driver_or_password_and_do_not_log_password(self):
        stream = io.StringIO()
        with patch('sys.stdout', stream):
            self.assertEqual(main(['--list-queries']), 0)
        self.assertIn('w5', stream.getvalue())
        stream = io.StringIO()
        with patch('sys.stdout', stream):
            self.assertEqual(main(['--run-id', 'r', '--queries', 'all', '--dry-run', '--password', 'PRIVATE_TEST_VALUE']), 0)
        self.assertNotIn('PRIVATE_TEST_VALUE', stream.getvalue())
        self.assertEqual(len(json.loads(stream.getvalue())['queries']), 14)

    def test_parallel_results_and_metrics_are_written_and_output_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(output_dir=Path(folder) / 'new', max_workers=2, repeat=2,
                                   database='neo4j', transaction_timeout=3, print_context=False)
            driver = FakeDriver([[basis_row()]])
            summary = run_benchmark(driver, ContextOptions('run-a', selection='w0'), [1, 2], args)
            contexts = [json.loads(line) for line in (args.output_dir / 'contexts.jsonl').read_text().splitlines()]
            queries = [json.loads(line) for line in (args.output_dir / 'queries.jsonl').read_text().splitlines()]
            self.assertEqual(summary['contexts'], 4)
            self.assertEqual(len({c['context_index'] for c in contexts}), 4)
            self.assertEqual({q['robot_id'] for q in queries}, {1, 2})
            self.assertEqual(summary['queries']['w0']['executed'], 4)
            self.assertEqual(summary['queries']['w0']['rows'], 4)
            self.assertTrue((args.output_dir / 'summary.json').exists())
            self.assertTrue(all(not s.explicit_transaction_started for s in driver.sessions))
            self.assertTrue(all(result.consumed for s in driver.sessions for result in s.runner.results))
            with self.assertRaises(FileExistsError):
                run_benchmark(driver, ContextOptions('run-a'), [1], args)

    def test_summary_excludes_skipped_queries_from_latency_samples(self):
        summary = summary_from([
            dict(query_id='w0', status='ok', elapsed_ms=8, row_count=1),
            dict(query_id='w0', status='skipped', elapsed_ms=0, row_count=0)],
            [dict(status='ok', elapsed_ms=10)], 11)
        self.assertEqual(summary['queries']['w0']['mean_ms'], 8)
        self.assertEqual(summary['queries']['w0']['executed'], 1)

    def test_cli_accepts_ranges_and_reports_bad_params_before_connecting(self):
        _, _, ids = parse_args(['--run-id', 'r', '--robot-ids', '1-3,2,5'])
        self.assertEqual(ids, [1, 2, 3, 5])
        with patch('sys.stderr', io.StringIO()), self.assertRaises(SystemExit):
            parse_args(['--run-id', 'r', '--param', 't=999'])


if __name__ == '__main__':
    unittest.main()
