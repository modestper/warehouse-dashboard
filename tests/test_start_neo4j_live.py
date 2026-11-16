"""Launcher preflight and ownership of its child services."""
import unittest
from contextlib import redirect_stderr
import io
from unittest.mock import MagicMock, patch

from ops.start_neo4j_live import DatabaseSelectionError, live_command, main, parser, prepare_environment, verify_database


class LauncherTests(unittest.TestCase):
    def test_missing_database_is_explained_without_fallback_or_startup(self):
        from neo4j.exceptions import Neo4jError
        class MissingDatabase(Neo4jError):
            code='Neo.ClientError.Database.DatabaseNotFound'
        driver=MagicMock()
        result=MagicMock()
        result.data.return_value=[{'name':'neo4j'},{'name':'system'}]
        driver.session.return_value.__enter__.return_value.run.side_effect=[MissingDatabase('missing'),result]
        with self.assertRaisesRegex(DatabaseSelectionError,'warehouse.*不存在.*neo4j.*不会创建'):
            verify_database(driver,'warehouse')
        self.assertEqual([call.kwargs['database'] for call in driver.session.call_args_list],['warehouse','system'])
        with patch('ops.start_neo4j_live.prepare_environment',side_effect=DatabaseSelectionError('目标库不存在')), \
                patch('ops.start_neo4j_live.subprocess.run') as run, \
                patch('ops.start_neo4j_live.subprocess.Popen') as spawn,redirect_stderr(io.StringIO()), \
                self.assertRaises(SystemExit) as stopped:
            main([])
        self.assertEqual(stopped.exception.code,2)
        run.assert_not_called()
        spawn.assert_not_called()

    def test_other_database_errors_are_not_misreported_as_missing_database(self):
        from neo4j.exceptions import Neo4jError
        class AuthenticationFailure(Neo4jError):
            code='Neo.ClientError.Security.Unauthorized'
        driver=MagicMock()
        error=AuthenticationFailure('authentication failed')
        driver.session.return_value.__enter__.return_value.run.side_effect=error
        with self.assertRaises(AuthenticationFailure):verify_database(driver,'warehouse')
        self.assertEqual(driver.session.call_count,1)

    def test_live_stack_uses_direct_persistence_and_stateful_robot_writers(self):
        args = parser().parse_args([])
        command = live_command(args)
        self.assertIn('--no-journal', command)
        for option, expected in (('--knowledge-backend', 'neo4j'), ('--robot-processes', 'ray'),
                                 ('--ray-address', 'local'), ('--port', '14583')):
            self.assertEqual(command[command.index(option) + 1], expected)
        self.assertIn('--robot-graph', command)
        self.assertNotIn('--legacy-a-graph', command)

    def test_check_only_never_imports_or_starts_a_service(self):
        with patch('ops.start_neo4j_live.prepare_environment', return_value=({}, {})), \
                patch('ops.start_neo4j_live.subprocess.run') as run, \
                patch('ops.start_neo4j_live.subprocess.Popen') as spawn, \
                patch('ops.start_neo4j_live.ensure_port_free') as bind:
            self.assertEqual(main(['--check-only']), 0)
            run.assert_not_called()
            spawn.assert_not_called()
            bind.assert_not_called()

    def test_modes_are_forwarded_without_starting_a_separate_http_writer(self):
        live = MagicMock()
        live.poll.return_value = 0
        live.returncode = 0
        with patch('ops.start_neo4j_live.prepare_environment', return_value=({'test': 'env'}, {})), \
                patch('ops.start_neo4j_live.ensure_port_free'), \
                patch('ops.start_neo4j_live.subprocess.Popen', return_value=live) as spawn:
            self.assertEqual(main(['--skip-import', '--database', 'warehouse']), 0)
        self.assertEqual(spawn.call_count, 1)
        for call in spawn.call_args_list:
            self.assertTrue(call.kwargs['start_new_session'])
        live.send_signal.assert_not_called()

    def test_lade_environment_selects_every_effective_child_argument(self):
        env = dict(KG_BACKEND='neo4j', KG_RUN_ID='warehouse-from-env', KG_WRITE_MODE='process',
                   KG_STATE_SOURCE='neo4j', KG_RECORD_AGENT_STATE='1',
                   NEO4J_URI='bolt://127.0.0.1:17687', NEO4J_DATABASE='warehouse', NEO4J_USER='neo4j')
        with patch.dict('os.environ', env, clear=True):
            args = parser().parse_args(['--ray-address', 'local', '--ray-num-cpus', '5'])
            command = live_command(args)
        for option, expected in (('--kg-write-mode','process'),('--run-id','warehouse-from-env'),
                ('--kg-state-source','neo4j'),('--graph-state-interval','1'),('--ray-num-cpus','5'),
                ('--ray-actor-cpus','0.05')):
            self.assertEqual(command[command.index(option)+1],expected)
        self.assertEqual(args.uri,env['NEO4J_URI'])
        self.assertEqual(args.database,'warehouse')

    def test_connection_cli_overrides_environment_and_normalizes_aliases(self):
        env = dict(NEO4J_URI='bolt://generic', NEO4J_DATABASE='generic-db', NEO4J_PASSWORD='test-only-secret',
                   WAREHOUSE_NEO4J_URI='bolt://stale', WAREHOUSE_NEO4J_DATABASE='stale-db')
        with patch.dict('os.environ', env, clear=True), patch('neo4j.GraphDatabase.driver') as factory:
            args=parser().parse_args(['--uri','bolt://explicit','--database','warehouse'])
            child_env,_=prepare_environment(args)
        self.assertEqual(factory.call_args.args,('bolt://explicit',))
        self.assertEqual(factory.return_value.__enter__.return_value.session.call_args.kwargs['database'],'warehouse')
        for prefix in ('NEO4J_','WAREHOUSE_NEO4J_'):
            self.assertEqual(child_env[prefix+'URI'],'bolt://explicit')
            self.assertEqual(child_env[prefix+'DATABASE'],'warehouse')
            self.assertEqual(child_env[prefix+'PASSWORD'],'test-only-secret')

    def test_existing_run_id_prevents_import_and_launch(self):
        with patch.dict('os.environ', {'NEO4J_PASSWORD':'test-only','KG_RUN_ID':'existing-run'}, clear=True), \
                patch('neo4j.GraphDatabase.driver') as factory, \
                patch('ops.start_neo4j_live.subprocess.run') as run, \
                patch('ops.start_neo4j_live.subprocess.Popen') as spawn:
            factory.return_value.__enter__.return_value.session.return_value.__enter__.return_value.run.return_value.single.return_value={'existing':'existing-run'}
            with self.assertRaisesRegex(ValueError,'KG_RUN_ID 已存在'):
                main([])
        run.assert_not_called()
        spawn.assert_not_called()


if __name__ == '__main__':
    unittest.main()
