"""Write ownership, acknowledgements and Step barriers without a live DB."""
import os
from types import SimpleNamespace
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from backend.warehouse_knowledge_writer import WarehouseKnowledgeWriter, WriteExecutor


class FakeExecutor:
    def __init__(self, connection):
        self.connection = connection

    def apply(self, method, *args):
        if method == 'fail':
            raise ValueError('injected database failure')
        if method == 'close':
            return None
        if method == 'append':
            return {'backend':'neo4j-direct','durable_seq':args[0]['knowledge_seq'],'mirrored_seq':0,'lag':0}
        if method == 'robots':
            return {'tick':next(iter(args[0].values()))['tick'],'robots':sorted(args[0])}
        if method == 'mutate':
            args[0]['changed'] = True
        return {'pid':os.getpid(),'connection':self.connection,'args':args}

    def close(self):
        pass


class WriteRoutingTests(unittest.TestCase):
    def test_real_live_steps_use_main_and_process_routes_without_journal(self):
        from backend.robot_actor.local import LocalRobotPool
        from ops.warehouse_live_runtime import WarehouseLiveRuntime
        root=Path(__file__).resolve().parents[1]
        datasets={'medium19':{'scene':str(root/'data/layouts/expansion-candidates/medium/layout.json'),
            'dataset':str(root/'data/examples/wheeled-medium-sample19'),
            'config_json':str(root/'config/wheeled-medium-100.json')}}
        class Pool(LocalRobotPool):
            def __init__(self,*args,**kwargs):super().__init__(*args)
        for mode in ('main','process'):
            for source in ('memory','neo4j'):
                with self.subTest(mode=mode,source=source),tempfile.TemporaryDirectory() as folder:
                    reader=MagicMock()
                    reader.timings={}
                    reader.latest_step=None
                    def read(run_id,tick,ids,fields):
                        reader.latest_step=tick
                        return deepcopy(runtime._sim.robots)
                    reader.read.side_effect=read
                    def route(mode,connection,**kwargs):
                        return WarehouseKnowledgeWriter(mode,connection,executor_class=FakeExecutor,**kwargs)
                    with patch.dict('os.environ',{'NEO4J_URI':'bolt://unused','NEO4J_PASSWORD':'test-only'},clear=True), \
                            patch('backend.warehouse_knowledge_writer.WarehouseKnowledgeWriter',side_effect=route), \
                            patch('backend.robot_actor.ray_actor.RayRobotPool',Pool), \
                            patch('backend.robot_actor.kg_state.KGStateReader',return_value=reader):
                        runtime=WarehouseLiveRuntime(source_root=root,output_root=folder,datasets=datasets,
                            dataset_id='medium19',run_id='routed-live',knowledge_backend='neo4j',no_journal=True,
                            mobility='wheeled',robot_processes='ray',robot_graph=True,kg_write_mode=mode,
                            kg_state_source=source,robot_context_interval=0)
                        try:
                            frame=runtime.command('step')
                            self.assertEqual(frame['elapsed'],1)
                            self.assertEqual(frame['knowledge']['write_mode'],mode)
                            self.assertEqual(frame['knowledge']['writer_pid']==os.getpid(),mode=='main')
                            self.assertEqual(frame['metadata']['kg_write_mode'],mode)
                            self.assertEqual(list(Path(folder).rglob('journal.sqlite*')),[])
                            self.assertEqual(reader.read.called,source=='neo4j')
                        finally:
                            runtime.shutdown()

    def test_main_and_process_have_distinct_real_write_owner_pids(self):
        for mode in ('main','process'):
            with self.subTest(mode=mode):
                writer=WarehouseKnowledgeWriter(mode, {'database':'warehouse'}, executor_class=FakeExecutor, timeout=15)
                try:
                    result=writer.call('inspect',{'tick':7})
                    self.assertEqual(result['connection']['database'],'warehouse')
                    self.assertEqual(result['pid']==os.getpid(),mode=='main')
                    payload={'tick':7}
                    writer.call('mutate',payload)
                    self.assertNotIn('changed',payload)
                    writer.append({'run_id':'routed-test','knowledge_seq':1},[])
                    self.assertEqual(writer.status('routed-test')['writer_pid'],result['pid'])
                    self.assertEqual(writer.status('routed-test')['durable_seq'],1)
                    with self.assertRaisesRegex((ValueError,RuntimeError),'injected database failure'):
                        writer.call('fail')
                finally:
                    writer.close()
                if mode=='process':
                    self.assertFalse(writer.process.is_alive())

    @unittest.skipUnless(os.environ.get('WAREHOUSE_KG_WRITER_RAY_TEST')=='1','opt-in real Ray actor test')
    def test_ray_mode_uses_a_real_dedicated_actor(self):
        import ray
        writer=WarehouseKnowledgeWriter('ray',{'database':'warehouse'},ray_address='local',ray_num_cpus=1,
                                        executor_class=FakeExecutor,timeout=45)
        try:
            result=writer.call('inspect',{'tick':1})
            self.assertNotEqual(result['pid'],os.getpid())
            self.assertEqual(writer.pid,result['pid'])
            self.assertEqual(result['connection']['database'],'warehouse')
        finally:
            writer.close()
            ray.shutdown()

    def test_executor_orders_history_before_current_and_global_step(self):
        graph,glob,store=MagicMock(),MagicMock(),MagicMock()
        with patch('backend.warehouse_runtime_graph.Neo4jRuntimeGraphWriter',return_value=graph), \
                patch('backend.global_writer.GlobalWriter',return_value=glob), \
                patch('backend.neo4j_live_store.Neo4jLiveStore',return_value=store):
            executor=WriteExecutor({'uri':'bolt://unused','user':'neo4j','password':'test','database':'warehouse'})
            executor.apply('bootstrap',{'test':'config'},{'id':'run'},False)
            snapshots={1:{'tick':1,'current_robot':{'robot_id':1}}}
            receipt=executor.apply('robots',snapshots)
            self.assertEqual(receipt,{'tick':1,'robots':[1]})
            self.assertEqual([call[0] for call in graph.method_calls][-2:],
                             ['record_robot_snapshot','record_robot_current_state'])
            executor.apply('global',{'tick':1})
            glob.write.assert_called_once_with({'tick':1})
            executor.close()

    def test_main_and_process_frames_do_not_call_actor_writers(self):
        from backend.robot_actor.ray_actor import RayRobotProcessSplit
        for mode in ('main','process','ray'):
            with self.subTest(mode=mode):
                sim=object.__new__(RayRobotProcessSplit)
                sim.tick=7;sim.elapsed=7;sim.run_id='routing-test'
                sim.config=SimpleNamespace(max_active_robots=100)
                sim.robots={i:{'robot_id':i} for i in range(1,101)}
                sim.pending_events=[];sim._graph_cursor=0;sim._graph_pending=[]
                sim.kg_timing={};sim.kg_state_source='memory'
                sim.frame=lambda:{'backend':'test'}
                writer=MagicMock();writer.mode=mode
                writer.write_robots.return_value={'tick':7,'robots':list(sim.robots)}
                sim.graph_writer=writer
                pool=MagicMock()
                pool.write_graph_all.return_value={i:'ref' for i in sim.robots}
                sim._robot_pool=lambda:pool
                sim._emit_robot_graph()
                if mode=='ray':
                    pool.write_graph_all.assert_called_once()
                    writer.write_robots.assert_not_called()
                else:
                    writer.write_robots.assert_called_once()
                    pool.write_graph_all.assert_not_called()

    def test_global_step_requires_all_successful_robot_receipts(self):
        from backend.robot_actor.ray_actor import RayRobotProcessSplit
        for count in (99,100):
            with self.subTest(count=count):
                sim=object.__new__(RayRobotProcessSplit)
                sim.tick=7;sim.elapsed=7;sim._global_tick=6;sim._global_cursor=0
                sim.robots={i:{} for i in range(1,101)}
                sim.pending_events=[];sim.kg_state_source='memory';sim.graph_writer=MagicMock()
                sim.graph_writer.mode='process';sim._step_client=None
                sim.flush_robot_graph=lambda:{'tick':7,'robots':list(range(1,count+1))}
                sim.manifest={'duration_seconds':10};sim.metrics={'completed':0};sim.assists={};sim.tasks={}
                sim.global_record_identity={};sim.kg_timing={}
                with patch('backend.global_writer.build_global_record',return_value={'tick':7}):
                    if count==99:
                        with self.assertRaisesRegex(RuntimeError,'incomplete robot receipts'):sim._submit_global_step()
                        sim.graph_writer.write_global.assert_not_called()
                    else:
                        sim._submit_global_step()
                        record=sim.graph_writer.write_global.call_args.args[0]
                        self.assertTrue(record['step']['complete'])
                        self.assertEqual(record['step']['source'],'warehouse-process-writer')


if __name__=='__main__':unittest.main()
