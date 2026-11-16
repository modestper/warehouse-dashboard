"""Real input coverage, graph isolation and runtime selection for registered datasets."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from backend.warehouse_graph import load_graph_package
from ops.start_neo4j_live import DEFAULT_REGISTRY, main
from ops.warehouse_live import dataset_descriptions, load_registry


ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    'medium19': ('wheeled-medium-sample19', 19),
    'medium1728': ('wheeled-medium-peak', 1728),
    'sample19': ('warehouse-v5', 19),
    'peak1728': ('regression/peak', 1728),
    'day': ('regression/day', 6912),
    'peak-2h': ('regression/peak-2h', 768),
    'hotspot-2h': ('regression/hotspot-2h', 924),
    'continuous-2d': ('regression/continuous-2d', 1034),
    'seven-patterns': ('seven-patterns-20261028', 50112),
}


class RegisteredDatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.datasets, _ = load_registry(DEFAULT_REGISTRY)

    def test_registry_covers_every_existing_task_package(self):
        actual = {p.parent.resolve() for p in (ROOT / 'data/examples').rglob('tasks.csv')}
        self.assertEqual(set(self.datasets), set(EXPECTED))
        self.assertEqual({Path(s['dataset']) for s in self.datasets.values()}, actual)
        descriptions = dataset_descriptions(self.datasets)['datasets']
        self.assertEqual(len(descriptions), 9)
        for row in descriptions:
            with self.subTest(dataset=row['id']):
                directory, count = EXPECTED[row['id']]
                self.assertTrue(row['available'])
                self.assertEqual(row['task_count'], count)
                self.assertEqual(Path(self.datasets[row['id']]['dataset']), ROOT / 'data/examples' / directory)

    def test_every_package_validates_and_its_graph_nodes_are_isolated(self):
        from backend.wide.data import validate_dataset
        from ops.warehouse_live_runtime import validate_disturbances
        seen_tasks, seen_goods, seen_datasets = set(), set(), set()
        for alias, spec in self.datasets.items():
            with self.subTest(dataset=alias):
                validate_dataset(spec['scene'], spec['dataset'])
                package = load_graph_package(spec['dataset'], spec['scene'], spec['locations_csv'],
                                             dataset_id=spec['graph_dataset_id'])
                self.assertEqual(package.summary()['tasks'], EXPECTED[alias][1])
                self.assertNotIn(package.dataset['id'], seen_datasets)
                seen_datasets.add(package.dataset['id'])
                task_ids = {task['id'] for task in package.tasks}
                goods_ids = {row['id'] for row in package.inventory} | {task['goods_node_id'] for task in package.tasks}
                self.assertFalse(task_ids & seen_tasks)
                self.assertFalse(goods_ids & seen_goods)
                seen_tasks.update(task_ids)
                seen_goods.update(goods_ids)
                if spec.get('disturbances_json'):
                    validate_disturbances(json.loads(Path(spec['disturbances_json']).read_text()))

    def test_existing_medium_registry_resolves_identical_specs(self):
        wheeled, default = load_registry(ROOT / 'config/live-wheeled-datasets.json')
        self.assertEqual(default, 'medium1728')
        for alias, spec in wheeled.items():
            self.assertEqual(spec, self.datasets[alias])

    def test_all_aliases_select_their_original_mobility_and_locations_for_launch(self):
        live = MagicMock()
        live.poll.return_value = live.returncode = 0
        for alias, spec in self.datasets.items():
            with self.subTest(dataset=alias), redirect_stdout(io.StringIO()), \
                    patch('ops.start_neo4j_live.prepare_environment', return_value=({}, spec)), \
                    patch('ops.start_neo4j_live.ensure_port_free'), \
                    patch('ops.start_neo4j_live.subprocess.run') as import_process, \
                    patch('ops.start_neo4j_live.subprocess.Popen', return_value=live) as spawn:
                self.assertEqual(main(['--dataset-id', alias]), 0)
                import_command = import_process.call_args.args[0]
                self.assertEqual(import_command[import_command.index('--locations') + 1], spec['locations_csv'])
                self.assertEqual(import_command[import_command.index('--graph-dataset-id') + 1], spec['graph_dataset_id'])
                command = spawn.call_args.args[0]
                self.assertEqual(command[command.index('--mobility') + 1], spec['mobility'])
                self.assertEqual(command[command.index('--registry') + 1], str(DEFAULT_REGISTRY))
                self.assertIn('--no-journal', command)

    def test_list_datasets_is_offline_and_unknown_alias_fails_before_connection(self):
        output = io.StringIO()
        with patch('neo4j.GraphDatabase.driver') as driver, \
                patch('ops.start_neo4j_live.getpass.getpass') as password, \
                patch('ops.start_neo4j_live.subprocess.Popen') as spawn:
            with redirect_stdout(output):
                self.assertEqual(main(['--list-datasets']), 0)
            self.assertEqual({row['id'] for row in json.loads(output.getvalue())['datasets']}, set(EXPECTED))
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as stopped:
                main(['--dataset-id', 'unknown-input'])
            self.assertEqual(stopped.exception.code, 2)
            driver.assert_not_called()
            password.assert_not_called()
            spawn.assert_not_called()


class RegisteredRuntimeTests(unittest.TestCase):
    def test_live_new_run_switches_map_config_and_physics_together(self):
        from ops.warehouse_live_runtime import WarehouseLiveRuntime
        datasets, _ = load_registry(DEFAULT_REGISTRY)
        with tempfile.TemporaryDirectory(prefix='warehouse-registry-') as folder, \
                patch.dict('os.environ', {}, clear=False):
            runtime = WarehouseLiveRuntime(source_root=ROOT, output_root=folder, datasets=datasets,
                dataset_id='medium19', run_id='medium-first', mobility='wheeled', robot_processes='local')
            try:
                first = runtime.latest()
                self.assertEqual(first['metadata']['mobility'], 'wheeled')
                self.assertEqual(first['metadata']['motion_speed_mps'], 1.5)
                old = runtime.command('new_run', {'dataset_id': 'sample19', 'run_id': 'legacy-next'})
                self.assertEqual(old['metadata']['mobility'], 'legacy')
                self.assertEqual(old['metadata']['motion_speed_mps'], .8)
                self.assertEqual(len(runtime.scene()['racks']), 36)
                self.assertFalse(hasattr(runtime._sim, 'battery_pool'))
                self.assertEqual(runtime.command('step')['elapsed'], 1)
                restored = runtime.command('new_run', {'dataset_id': 'medium19', 'run_id': 'medium-again'})
                self.assertEqual(restored['metadata']['mobility'], 'wheeled')
                self.assertEqual(len(runtime.scene()['racks']), 48)
            finally:
                runtime.shutdown()

    def test_legacy_robot_split_preserves_actual_dispatch_and_motion(self):
        from backend.warehouse.composition import physical_classes
        scene = ROOT / 'data/layouts/warehouse100-wide-v2.json'
        dataset = ROOT / 'data/examples/warehouse-v5'
        sequential_class, config_class = physical_classes('legacy')
        split_class, _ = physical_classes('legacy', robot_processes='local')
        sequential = sequential_class(scene, dataset, 'legacy-equivalence', config_class(), knowledge_mode='rules')
        split = split_class(scene, dataset, 'legacy-equivalence', config_class(), knowledge_mode='rules')
        try:
            for sim in (sequential, split):
                shift = min(task['release_second'] for task in sim.tasks.values())
                for task in sim.tasks.values():
                    task['release_second'] -= shift
                for _ in range(60):
                    sim.step()
            self.assertTrue(any(t.get('assigned_at') is not None for t in split.tasks.values()))
            self.assertEqual(split.tasks, sequential.tasks)
            self.assertEqual(split.robots, sequential.robots)
        finally:
            split.close()


if __name__ == '__main__':
    unittest.main()
