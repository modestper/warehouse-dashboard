import unittest
from ops.warehouse_kg_config import Neo4jConnection, env_bool, knowledge_backend, state_source


class ConfigTests(unittest.TestCase):
    def test_lade_names_win_over_inherited_warehouse_aliases(self):
        config=Neo4jConnection.from_env({'NEO4J_URI':'bolt://new','WAREHOUSE_NEO4J_URI':'bolt://old',
            'NEO4J_DATABASE':'warehouse','WAREHOUSE_NEO4J_DATABASE':'old','NEO4J_PASSWORD':'test-secret'})
        self.assertEqual(config.uri,'bolt://new')
        self.assertEqual(config.database,'warehouse')
        self.assertNotIn('test-secret',repr(config))
        env=config.environment({})
        self.assertEqual(env['WAREHOUSE_NEO4J_URI'],env['NEO4J_URI'])
        self.assertEqual(env['WAREHOUSE_NEO4J_PASSWORD'],env['NEO4J_PASSWORD'])

    def test_alias_fallback_and_database_defaults(self):
        self.assertEqual(Neo4jConnection.from_env({'WAREHOUSE_NEO4J_URI':'bolt://legacy'}).uri,'bolt://legacy')
        self.assertEqual(Neo4jConnection.from_env({}).database,'neo4j')
        self.assertEqual(Neo4jConnection.from_env({},default_database='warehouse').database,'warehouse')

    def test_history_and_state_values_are_not_silently_ignored(self):
        self.assertTrue(env_bool('KG_RECORD_AGENT_STATE',environ={'KG_RECORD_AGENT_STATE':'1'}))
        self.assertFalse(env_bool('KG_RECORD_AGENT_STATE',environ={'KG_RECORD_AGENT_STATE':'0'}))
        with self.assertRaises(ValueError):env_bool('KG_RECORD_AGENT_STATE',environ={'KG_RECORD_AGENT_STATE':'true'})
        self.assertEqual(state_source('ray'),'memory')
        self.assertEqual(state_source('neo4j'),'neo4j')
        with self.assertRaises(ValueError):state_source('ignored')
        self.assertEqual(knowledge_backend('graph'),'neo4j')
        with self.assertRaises(ValueError):knowledge_backend('memory')


if __name__=='__main__':unittest.main()
