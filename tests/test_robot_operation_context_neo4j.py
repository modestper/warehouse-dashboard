"""Opt-in Cypher execution against an explicitly supplied isolated Neo4j.

All fixtures are unique CREATEs in one transaction, rolled back in tearDown.
No indexes, constraints, existing nodes or persistent run data are changed.
"""
from dataclasses import replace
import json
import logging
import os
import unittest
import uuid
from unittest.mock import patch

from backend.robot_operation_context import ContextOptions, QUERIES, execute_context, read_context


@unittest.skipUnless(os.environ.get('WAREHOUSE_CONTEXT_TEST_URI'), '需要显式指定隔离 Neo4j URI')
class OperationContextNeo4jTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from neo4j import GraphDatabase
        notifications = patch.object(logging.getLogger('neo4j.notifications'), 'disabled', True)
        notifications.start()
        cls.addClassCleanup(notifications.stop)
        password = os.environ.get('WAREHOUSE_CONTEXT_TEST_PASSWORD')
        cls.driver = GraphDatabase.driver(os.environ['WAREHOUSE_CONTEXT_TEST_URI'],
            auth=(os.environ.get('WAREHOUSE_CONTEXT_TEST_USER', 'neo4j'), password) if password else None)
        cls.driver.verify_connectivity()

    @classmethod
    def tearDownClass(cls):
        cls.driver.close()

    def setUp(self):
        self.prefix = 'operation-context-test-' + uuid.uuid4().hex
        self.session = self.driver.session(database=os.environ.get('WAREHOUSE_CONTEXT_TEST_DATABASE', 'neo4j'))
        self.tx = self.session.begin_transaction(timeout=30)
        self.addCleanup(self.session.close)
        self.addCleanup(self.tx.rollback)
        self.options = ContextOptions(self.prefix, robot_id=1, tick=10, selection='all')
        self.tx.run('''
CREATE (run:Run {id:$p, latest_step:10, config_json:'{"charge_request_soc":35}'})
CREATE (dataset:Dataset {id:$p+':dataset', start:'2026-10-27T00:00:00+08:00'})
CREATE (run)-[:USES_DATASET]->(dataset)
CREATE (warehouse:Warehouse {id:$p+':warehouse', levels:3, rackHeight:1.8})
CREATE (dataset)-[:USES_LAYOUT]->(warehouse)
CREATE (:Step {id:$p+':step:10', run_id:$p, tick:10, complete:true,
              elapsed:20, expected_robot_count:3, received_robot_count:3})
CREATE (r1:Robot {id:$p+':robot:1', run_id:$p, robot_id:1, type:'A1',
                  stage:'future', x:999, y:999, battery:1})
CREATE (:Robot {id:$p+':robot:2', run_id:$p, robot_id:2, type:'X3'})
CREATE (:Robot {id:$p+':robot:3', run_id:$p, robot_id:3, type:'R2'})
CREATE (:AgentState {id:$p+':state:1:8', run_id:$p, robot_node_id:r1.id,
                    robot_id:1, tick:8, stage:'idle', x:0.0, y:0.0, battery:80.0})
CREATE (:AgentState {id:$p+':state:1:12', run_id:$p, robot_node_id:r1.id,
                    robot_id:1, tick:12, stage:'future', x:999.0, y:999.0, battery:1.0})
CREATE (:AgentState {id:$p+':state:2:8', run_id:$p, robot_node_id:$p+':robot:2',
                    robot_id:2, tick:8, stage:'idle', x:3.0, y:4.0, battery:90.0})
CREATE (:AgentState {id:$p+':state:3:12', run_id:$p, robot_node_id:$p+':robot:3',
                    robot_id:3, tick:12, stage:'future', x:999.0, y:999.0, battery:1.0})
CREATE (:DispatchState {id:$p+':dispatch:9', run_id:$p, dataset_id:dataset.id,
                        tick:9, commit_seq:9, elapsed:18, pending_json:$pending,
                        active_jobs_json:'[]', tasks_json:'{}', ports_json:'{}'})
CREATE (task:WarehouseTask {id:$p+':task', dataset_id:dataset.id, task_id:'T',
                           task_type:'OUTBOUND', due_time:'2026-10-27T00:05:00+08:00'})
CREATE (dataset)-[:INCLUDES_TASK]->(task)
CREATE (goods:Goods {id:$p+':goods', goods_id:'A'})
CREATE (task)-[:CONCERNS]->(goods)
CREATE (source:Location {id:$p+':source', location_id:'R01-L3', rack_id:'R01',
                         kind:'slot', level:3, x:10.0, y:20.0})
CREATE (target:Location {id:$p+':target', location_id:'OUTBOUND', kind:'logical-boundary', physical:false})
CREATE (task)-[:FROM]->(source)
CREATE (task)-[:TO]->(target)
CREATE (rack:Rack {id:$p+':rack:R01'})
CREATE (warehouse)-[:HAS_RACK]->(rack)-[:HAS_SLOT]->(source)
CREATE (port:Location {id:$p+':port', location_id:'OUT-1', kind:'outbound', x:30.0, y:20.0})
CREATE (warehouse)-[:HAS_LOCATION]->(port)
CREATE (:RuntimeEvent {id:$p+':bound:9', run_id:$p, task_id:'T', tick:9,
                      type:'boundary_port_bound', details_json:'{"endpoint":"target","actual_id":"OUT-1"}'})
CREATE (:RuntimeEvent {id:$p+':bound:12', run_id:$p, task_id:'T', tick:12,
                      type:'boundary_port_bound', details_json:'{"endpoint":"target","actual_id":"FUTURE"}'})
CREATE (:RuntimeEvent {id:$p+':event:9', run_id:$p, robot_node_id:r1.id,
                      task_id:'T', tick:9, type:'picked'})
CREATE (:RuntimeEvent {id:$p+':event:12', run_id:$p, robot_node_id:r1.id,
                      task_id:'T', tick:12, type:'future'})
CREATE (:ExecutionState {id:$p+':execution:8', run_id:$p, task_id:'T', tick:8,
                         robot_id:1, status:'carrying'})
CREATE (:ExecutionState {id:$p+':execution:12', run_id:$p, task_id:'T', tick:12,
                         robot_id:2, status:'completed'})
CREATE (:OwnershipChange {id:$p+':owner:6', run_id:$p, task_id:'T', tick:6, to_robot_id:1})
CREATE (:OwnershipChange {id:$p+':owner:12', run_id:$p, task_id:'T', tick:12, to_robot_id:2})
CREATE (:Handoff {id:$p+':handoff:12', run_id:$p, task_id:'T', tick:12, donor_id:1, receiver_id:2})
CREATE (other:Dataset {id:$p+':other-dataset'})
CREATE (foreign:WarehouseTask {id:$p+':foreign-task', task_id:'T', dataset_id:other.id})
CREATE (other)-[:INCLUDES_TASK]->(foreign)
CREATE (foreign)-[:CONCERNS]->(:Goods {id:$p+':foreign-goods', goods_id:'FOREIGN'})
CREATE (:Robot {id:$p+':other:robot:2', run_id:$p+':other', robot_id:2, type:'A1'})
CREATE (:AgentState {id:$p+':foreign-state', run_id:$p+':other', robot_node_id:$p+':other:robot:2',
                    robot_id:2, tick:8, x:-999, y:-999})
''', p=self.prefix, pending=json.dumps([dict(task_id='T', status='waiting', release_second=5)])).consume()

    def test_all_queries_compile_and_execute_with_future_and_scope_isolation(self):
        context, records = execute_context(self.tx, self.options)
        self.assertTrue(all(record['status'] == 'ok' for record in records), records)
        self.assertEqual(len(records), 14)
        self.assertEqual(context['tick'], 10)
        q = context['queries']
        self.assertEqual(q['w0']['rows'][0]['self_state']['tick'], 8)
        self.assertEqual(q['w0']['rows'][0]['self_state']['x'], 0)
        geometry = q['w2']['rows'][0]
        self.assertEqual(geometry['goods_id'], 'A')
        self.assertEqual(geometry['resolved_endpoints']['target']['location_id'], 'OUT-1')
        self.assertEqual(geometry['capability']['complete_task']['result'], 'rejected')
        self.assertEqual(geometry['capability']['remaining_delivery']['result'], 'passed')
        evidence = q['w3']['rows'][0]
        self.assertEqual(evidence['execution_sample']['tick'], 8)
        self.assertEqual(evidence['ownership_changes'][0]['to_robot_id'], 1)
        self.assertEqual(evidence['handoffs'], [])
        self.assertEqual([e['tick'] for e in evidence['operation_events']], [9])
        peers = q['w4']['rows']
        self.assertEqual([r['robot_id'] for r in peers], [2, 3])
        self.assertEqual(peers[0]['distance_m'], 5)
        self.assertIsNone(peers[1]['state'])
        self.assertEqual([e['tick'] for e in q['w5']['rows'][0]['events']], [9])
        self.assertEqual(q['w8']['rows'][0]['sample_count'], 1)
        self.assertEqual(q['w8']['rows'][0]['max_x'], 0)

    def test_explain_all_queries(self):
        _, records = execute_context(self.tx, replace(self.options, explain=True))
        self.assertEqual(len(records), 14)
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)

    def test_auto_commit_query_continues_after_a_server_syntax_error(self):
        # EXPLAIN needs no visibility into this test's uncommitted fixture.
        # The deliberately invalid read must not poison the next transaction.
        broken = replace(QUERIES['w1'], cypher='RETURN (')
        with patch.dict(QUERIES, {'w1': broken}):
            context, records = read_context(self.driver,
                replace(self.options, selection='w1,w4', explain=True),
                database=os.environ.get('WAREHOUSE_CONTEXT_TEST_DATABASE', 'neo4j'))
        self.assertEqual([r['status'] for r in records], ['ok', 'error', 'ok'], records)
        self.assertEqual(context['transaction_mode'], 'auto_commit_per_query')

    def test_missing_step_stops_after_basis(self):
        self.tx.run('MATCH (s:Step {run_id:$p}) DELETE s', p=self.prefix).consume()
        context, records = execute_context(self.tx, self.options)
        self.assertEqual(context['status'], 'unavailable')
        self.assertTrue(all(r['status'] == 'skipped' for r in records[1:]))

    def test_run_namespace_resolves_definitions_without_dataset_link(self):
        self.tx.run('''
MATCH (run:Run {id:$p})-[link:USES_DATASET]->(dataset:Dataset)
SET run.dataset_id=dataset.id DELETE link
''', p=self.prefix).consume()
        context, records = execute_context(self.tx, replace(self.options, selection='w2', task_ids=('T',)))
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        row = context['queries']['w2']['rows'][0]
        self.assertEqual(row['goods_id'], 'A')
        self.assertEqual(row['task_resolution'], 'run_dataset_namespace')

    def test_execution_link_resolves_definitions_without_run_namespace(self):
        self.tx.run('''
MATCH (run:Run {id:$p})-[link:USES_DATASET]->(:Dataset)
DELETE link
WITH run MATCH (task:WarehouseTask {id:$p+':task'})
CREATE (:Execution {id:$p+':execution',run_id:$p,task_id:'T'})-[:EXECUTES]->(task)
''', p=self.prefix).consume()
        context, records = execute_context(self.tx, replace(self.options, selection='w2', task_ids=('T',)))
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        self.assertIsNone(context['dataset_id'])
        row = context['queries']['w2']['rows'][0]
        self.assertEqual(row['goods_id'], 'A')
        self.assertEqual(row['task_resolution'], 'run_execution_link')

    def test_no_static_nodes_still_returns_runtime_facts_and_unknown_geometry(self):
        self.tx.run('''
MATCH (dataset:Dataset {id:$p+':dataset'})-[:INCLUDES_TASK]->(task:WarehouseTask)
DETACH DELETE dataset, task
WITH 1 AS ignored
MATCH (self:AgentState {id:$p+':state:1:8'}) SET self.task_id='T'
WITH self MATCH (peer:AgentState {id:$p+':state:2:8'}) SET peer.cargo_id='A'
WITH peer MATCH (sample:ExecutionState {id:$p+':execution:8'}) SET sample.cargo_id='A'
WITH sample MATCH (future:ExecutionState {id:$p+':execution:12'}) SET future.cargo_id='FUTURE'
WITH future MATCH (picked:RuntimeEvent {id:$p+':event:9'}) SET picked.time=20, picked.cargo_id='A'
CREATE (:RuntimeEvent {id:$p+':assign',run_id:$p,task_id:'T',tick:5,time:5,type:'task_assigned'})
CREATE (:RuntimeEvent {id:$p+':done',run_id:$p,task_id:'T',tick:10,time:360,cargo_id:'A',
                      robot_node_id:$p+':robot:1',type:'shipped'})
''', p=self.prefix).consume()
        context, records = execute_context(self.tx, self.options)
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        self.assertEqual(context['tick'], 10)
        self.assertIsNone(context['dataset_id'])
        q = context['queries']
        self.assertFalse(q['w2']['rows'][0]['task_exists'])
        self.assertIsNone(q['w2']['rows'][0]['goods_id'])  # no cross-dataset T fallback
        self.assertTrue(q['w3']['rows'][0]['task_observed'])
        self.assertEqual(q['w3']['rows'][0]['execution_sample']['tick'], 8)
        self.assertEqual(q['w6']['rows'][0]['assignment_to_pick_seconds'], 15)
        cargo = q['w7']['rows'][0]
        self.assertEqual(cargo['goods_id'], 'A')
        self.assertEqual(cargo['cargo_identity_count'], 1)
        self.assertEqual(cargo['initial_locations'], [])
        self.assertEqual(cargo['holder_observations'][0]['robot_id'], 2)
        self.assertEqual(q['w9']['rows'][0]['task_id'], 'T')
        self.assertIsNone(q['w9']['rows'][0]['observed_lateness_seconds'])
        self.assertIsNone(q['w12']['rows'][0]['rack_id'])
        self.assertIn('T', q['w12']['rows'][0]['observed_task_ids'])

    def test_ambiguous_execution_links_do_not_choose_a_static_definition(self):
        self.tx.run('''
MATCH (task:WarehouseTask {id:$p+':task'}), (foreign:WarehouseTask {id:$p+':foreign-task'})
CREATE (execution:Execution {id:$p+':execution',run_id:$p,task_id:'T'})
CREATE (execution)-[:EXECUTES]->(task), (execution)-[:EXECUTES]->(foreign)
''', p=self.prefix).consume()
        context, records = execute_context(self.tx, replace(self.options, selection='w2,w3', task_ids=('T',)))
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        row = context['queries']['w2']['rows'][0]
        self.assertFalse(row['task_exists'])
        self.assertEqual(row['task_resolution'], 'ambiguous_execution_links')
        self.assertTrue(context['queries']['w3']['rows'][0]['task_observed'])

    def test_exact_state_lookup_and_sparse_history_fallback(self):
        self.tx.run('''
CREATE (:AgentState {id:$p+':robot:1:state:10',run_id:$p,robot_node_id:$p+':robot:1',
                    robot_id:1,tick:10,stage:'idle',x:1.0,y:2.0,battery:70.0})
''', p=self.prefix).consume()
        context, records = execute_context(self.tx, replace(self.options, selection='w0,w4'))
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        self.assertEqual(context['queries']['w0']['rows'][0]['self_state']['tick'], 10)
        self.assertEqual(context['queries']['w4']['rows'][0]['state']['tick'], 8)
        self.assertIsNone(context['queries']['w4']['rows'][1]['state'])
        # Even a malformed canonical ID cannot introduce a future observation.
        self.tx.run('MATCH (s:AgentState {id:$p+\':robot:1:state:10\'}) SET s.tick=12', p=self.prefix).consume()
        context, _ = execute_context(self.tx, replace(self.options, selection='w0'))
        self.assertEqual(context['queries']['w0']['rows'][0]['self_state']['tick'], 8)

    def test_actual_query_limits_report_truncation(self):
        self.tx.run('''UNWIND [1,2,3] AS tick
CREATE (:RuntimeEvent {id:$p+':extra:'+toString(tick), run_id:$p,
                      robot_node_id:$p+':robot:1', tick:tick, type:'observed'})''', p=self.prefix).consume()
        context, _ = execute_context(self.tx, replace(self.options, selection='w5', params=dict(event_fetch_limit=3)))
        feedback = context['queries']['w5']
        self.assertEqual(len(feedback['rows'][0]['events']), 2)
        self.assertTrue(feedback['quality']['truncated']['events'])

    def test_task_timing_and_completion_feedback(self):
        self.tx.run('''
MATCH (picked:RuntimeEvent {id:$p+':event:9'}) SET picked.time=20
CREATE (:RuntimeEvent {id:$p+':assign',run_id:$p,task_id:'T',tick:5,time:5,type:'task_assigned'})
CREATE (:RuntimeEvent {id:$p+':done',run_id:$p,task_id:'T',tick:10,time:360,
                      robot_node_id:$p+':robot:1',type:'shipped'})''', p=self.prefix).consume()
        context, records = execute_context(self.tx, replace(self.options, selection='w6,w9', task_ids=('T',)))
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        timing = context['queries']['w6']['rows'][0]
        self.assertEqual(timing['assignment_to_pick_seconds'], 15)
        self.assertEqual(timing['pickup_to_completion_seconds'], 340)
        self.assertEqual(context['queries']['w9']['rows'][0]['observed_lateness_seconds'], 60)

    def test_cross_task_cargo_history_and_rack_references(self):
        self.tx.run('''
MATCH (owner:AgentState {id:$p+':state:2:8'}) SET owner.cargo_id='A'
WITH owner MATCH (self:AgentState {id:$p+':state:1:8'}) SET self.task_id='T'
CREATE (:RuntimeEvent {id:$p+':cargo-next',run_id:$p,task_id:'NEXT',cargo_id:'A',tick:7,type:'picked'})
CREATE (:RuntimeEvent {id:$p+':cargo-foreign',run_id:$p+':other',cargo_id:'A',tick:7,type:'picked'})
CREATE (:Handoff {id:$p+':cargo-handoff',run_id:$p,goods_id:'A',task_id:'T',tick:6,donor_id:1,receiver_id:2})''', p=self.prefix).consume()
        context, records = execute_context(self.tx, replace(self.options, selection='w7,w12', task_ids=('T',)))
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        cargo = context['queries']['w7']['rows'][0]
        self.assertEqual([e['task_id'] for e in cargo['cargo_events']], ['NEXT'])
        self.assertEqual([s['robot_id'] for s in cargo['holder_observations']], [2])
        self.assertEqual(len(cargo['handoffs']), 1)
        rack = context['queries']['w12']['rows'][0]
        self.assertEqual(rack['rack_id'], 'R01')
        self.assertEqual(rack['fresh_reference_count'], 1)

    def test_handoff_outcome_excludes_future_completion(self):
        self.tx.run('''
CREATE (:Handoff {id:$p+':handoff:6',run_id:$p,task_id:'T',tick:6,donor_id:1,receiver_id:2})''', p=self.prefix).consume()
        context, records = execute_context(self.tx, replace(self.options, selection='w11'))
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        row = context['queries']['w11']['rows'][0]
        self.assertIsNone(row['completion_tick'])
        self.assertEqual(row['outcome'], 'later_execution_sample_observed')
        self.tx.run('''CREATE (:ExecutionState {id:$p+':done:9',run_id:$p,task_id:'T',tick:9,status:'completed'})''', p=self.prefix).consume()
        context, _ = execute_context(self.tx, replace(self.options, selection='w11'))
        row = context['queries']['w11']['rows'][0]
        self.assertEqual(row['observed_completion_latency_ticks'], 3)

    def test_old_recovery_markers_and_port_pressure_history(self):
        self.tx.run('''
CREATE (:RuntimeEvent {id:$p+':stop',run_id:$p,robot_node_id:$p+':robot:1',type:'robot_stopped',tick:2})
CREATE (:RuntimeEvent {id:$p+':resume',run_id:$p,robot_node_id:$p+':robot:1',type:'robot_stop_cleared',tick:3})
CREATE (:DispatchState {id:$p+':dispatch:8',run_id:$p,dataset_id:$p+':dataset',
                       tick:8,commit_seq:8,elapsed:16,ports_json:'{"IN-1":2}'})
WITH 1 AS ignored
MATCH (latest:DispatchState {id:$p+':dispatch:9'}) SET latest.ports_json='{"IN-1":3}'
''', p=self.prefix).consume()
        context, records = execute_context(self.tx, replace(self.options, selection='w10', history_ticks=1))
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        self.assertEqual(context['queries']['w10']['quality']['recovery_evidence'], 'resume_observed_after_stop')
        context, records = execute_context(self.tx, replace(self.options, selection='w13'))
        self.assertTrue(all(r['status'] == 'ok' for r in records), records)
        self.assertEqual(context['queries']['w13']['quality']['port_trends']['IN-1']['observed_delta'], 1)


if __name__ == '__main__':
    unittest.main()
