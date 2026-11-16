"""LaDe-compatible environment parsing, shared by all warehouse writers."""
from dataclasses import dataclass, field
import os

WRITE_MODES = ('main', 'ray', 'process')


def env_bool(name, default=True, environ=None):
    env = os.environ if environ is None else environ
    value = env.get(name)
    if value is None:
        return default
    if value not in ('0', '1'):
        raise ValueError(f'{name} must be 0 or 1')
    return value == '1'


def state_source(value):
    if value == 'ray':
        return 'memory'
    if value not in ('memory', 'neo4j'):
        raise ValueError('KG_STATE_SOURCE must be ray, memory or neo4j')
    return value


def knowledge_backend(value):
    value = value.lower()
    if value == 'graph':
        return 'neo4j'
    if value not in ('neo4j', 'journal'):
        raise ValueError('KG_BACKEND must be neo4j/graph or journal; memory-only storage is not supported')
    return value


@dataclass(frozen=True)
class Neo4jConnection:
    uri: str = 'bolt://127.0.0.1:47687'
    user: str = 'neo4j'
    password: str = field(default='', repr=False)
    database: str = 'neo4j'

    @classmethod
    def from_env(cls, environ=None, default_database='neo4j'):
        env = os.environ if environ is None else environ
        # LaDe-style inline variables override inherited warehouse aliases.
        def value(name, fallback):
            return env.get('NEO4J_' + name) or env.get('WAREHOUSE_NEO4J_' + name) or fallback
        return cls(value('URI', 'bolt://127.0.0.1:47687'), value('USER', 'neo4j'),
                   value('PASSWORD', ''), value('DATABASE', default_database))

    def writer_config(self):
        return dict(uri=self.uri, user=self.user, password=self.password, database=self.database)

    def environment(self, base=None):
        result = dict(os.environ if base is None else base)
        for key, value in self.writer_config().items():
            result['NEO4J_' + key.upper()] = value
            result['WAREHOUSE_NEO4J_' + key.upper()] = value
        return result
