"""Exercise the shell entry point with isolated Docker and Neo4j stand-ins."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ContainerDatabaseTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="warehouse-copy-cli-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / "ops").mkdir()
        (self.root / "bin").mkdir()
        self.script = self.root / "ops/copy_neo4j_run_container.sh"
        shutil.copyfile(ROOT / "ops/copy_neo4j_run_container.sh", self.script)
        self.events = self.root / "events.jsonl"
        self.env = {
            **os.environ,
            "PATH": f"{self.root / 'bin'}:{os.environ['PATH']}",
            "COPY_TEST_EVENTS": str(self.events),
            "SOURCE_NEO4J_PASSWORD": "dummy-source",
            "TARGET_NEO4J_PASSWORD": "dummy-target",
        }
        common = """
            import json, os, sys
            def record(**event):
                with open(os.environ['COPY_TEST_EVENTS'], 'a') as stream:
                    stream.write(json.dumps(event) + '\\n')
        """
        self.write_executable("bin/docker", common + """
            args = sys.argv[1:]
            record(kind='docker', args=args)
            if args[0] == 'ps':
                print('source-id|127.0.0.1:47687->7687/tcp')
            elif args[0] == 'inspect':
                print('sha256:source-image')
            elif args[0] == 'run':
                print('target-id')
            elif args[0] != 'info':
                raise AssertionError(args)
        """)
        self.write_executable("bin/sudo", """
            raise AssertionError('Tests must never invoke sudo')
        """)
        # Run the real inline preflight/readiness Python, supplying only a fake driver.
        # Copy CLI invocations are recorded rather than allowed to connect or write.
        self.write_executable("run_in_lade.sh", common + """
            import types
            args = sys.argv[1:]
            if args == ['python', '-B', '-']:
                neo4j = types.ModuleType('neo4j')
                exceptions = types.ModuleType('neo4j.exceptions')
                for name in ('ServiceUnavailable', 'SessionExpired', 'AuthError'):
                    setattr(exceptions, name, type(name, (Exception,), {}))
                class Result:
                    def single(self): return {'id': 'source-run'}
                    def consume(self): pass
                class Connection:
                    def __init__(self, uri, **kwargs): self.uri = uri
                    def __enter__(self): return self
                    def __exit__(self, *args): pass
                    def session(self, database, **kwargs):
                        record(kind='session', uri=self.uri, database=database)
                        return self
                    def run(self, query, **kwargs): return Result()
                neo4j.GraphDatabase = types.SimpleNamespace(driver=Connection)
                neo4j.READ_ACCESS = 'READ'
                sys.modules['neo4j'] = neo4j
                sys.modules['neo4j.exceptions'] = exceptions
                exec(compile(sys.stdin.read(), '<shell-inline>', 'exec'))
            else:
                record(kind='copy', args=args)
        """)

    def write_executable(self, relative, source):
        path = self.root / relative
        path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(source), encoding="utf-8")
        path.chmod(0o700)

    def invoke(self, *args):
        self.events.unlink(missing_ok=True)
        result = subprocess.run(
            ["bash", str(self.script), *args], env=self.env,
            text=True, capture_output=True, timeout=15,
        )
        events = [json.loads(line) for line in self.events.read_text().splitlines()] if self.events.exists() else []
        return result, events

    def assert_target_database(self, expected, *args):
        result, events = self.invoke(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        creation = [e['args'] for e in events if e['kind'] == 'docker' and e['args'][0] == 'run']
        self.assertEqual(len(creation), 1)
        self.assertIn(f"NEO4J_initial_dbms_default__database={expected}", creation[0])
        sessions = [e for e in events if e['kind'] == 'session']
        self.assertEqual([(e['uri'], e['database']) for e in sessions], [
            ('bolt://127.0.0.1:47687', 'neo4j'),
            ('bolt://127.0.0.1:48687', expected),
        ])
        copies = [e['args'] for e in events if e['kind'] == 'copy']
        self.assertEqual(len(copies), 2)
        for call in copies:
            self.assertEqual(call[call.index('--target-database') + 1], expected)
            self.assertEqual(call[call.index('--run-id') + 1], 'million-110902')
        self.assertNotIn('--apply', copies[0])
        self.assertIn('--apply', copies[1])
        self.assertIn(f"业务库：{expected}。", result.stdout)
        self.assertIn(f"-d {expected}", result.stdout)

    def test_default_database_remains_neo4j(self):
        self.assert_target_database('neo4j', 'million-110902')

    def test_custom_database_reaches_initialization_readiness_and_both_copy_phases(self):
        for name in ('Warehouse-Copy', 'warehouse.copy', 'abc', 'a' * 63):
            with self.subTest(name=name):
                self.assert_target_database(name.lower(), 'million-110902', name)

    def test_invalid_names_fail_before_docker_or_credentials(self):
        for name in ('', 'ab', 'a' * 64, '1warehouse', 'warehouse_copy', 'system',
                     'System-copy', 'warehouse copy', '中文库', 'warehouse.', 'warehouse-'):
            with self.subTest(name=name):
                result, events = self.invoke('million-110902', name)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn('目标数据库名', result.stderr)
                self.assertEqual(events, [])

    def test_help_and_invalid_usage_never_touch_docker(self):
        result, events = self.invoke('--help')
        self.assertEqual(result.returncode, 0)
        self.assertIn('[目标数据库名]', result.stdout)
        self.assertEqual(events, [])
        for args in ((), ('bad/run',), ('million-110902', 'warehouse-copy', 'extra')):
            with self.subTest(args=args):
                result, events = self.invoke(*args)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(events, [])


if __name__ == '__main__':
    unittest.main()
