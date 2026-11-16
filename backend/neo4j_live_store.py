"""Synchronous live commits in Neo4j, without a local SQLite journal.

Robot actors still publish the queryable runtime graph. This store atomically
archives each commit's event batch and the latest full recovery checkpoint.
Its commit sequence is separate from the actors' complete Step watermark.
"""
import json
import time

from .knowledge import KnowledgeError, canonical, checkpoint_identity, digest, make_batch


class Neo4jLiveStore:
    mirror = None

    def __init__(self, *, uri, user, password, database, driver=None):
        if not password:
            raise ValueError('Neo4j-only persistence requires WAREHOUSE_NEO4J_PASSWORD')
        if driver is None:
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=10)
        self.driver, self.database = driver, database
        self.last_error = None
        self.timings = {'journal_seconds': 0., 'mirror_seconds': 0., 'commits': 0}
        self._sequences = {}
        try:
            with driver.session(database=database) as session:
                session.run('CREATE CONSTRAINT warehouse_live_checkpoint_run IF NOT EXISTS '
                            'FOR (n:WarehouseLiveCheckpoint) REQUIRE n.run_id IS UNIQUE').consume()
                session.run('CREATE CONSTRAINT warehouse_live_commit_sequence IF NOT EXISTS '
                            'FOR (n:WarehouseLiveCommit) REQUIRE (n.run_id,n.seq) IS UNIQUE').consume()
        except BaseException:
            driver.close()
            raise

    @classmethod
    def from_env(cls):
        from ops.warehouse_kg_config import Neo4jConnection
        return cls(**Neo4jConnection.from_env().writer_config())

    def append(self, checkpoint, events):
        started = time.perf_counter()
        run_id, seq = checkpoint['run_id'], checkpoint['knowledge_seq']

        def write(tx):
            # A dependent property update takes the head's write lock before
            # inspecting its sequence, including across competing writers.
            row = tx.run('MERGE (h:WarehouseLiveCheckpoint {run_id:$run_id}) '
                         'ON CREATE SET h.seq=0 '
                         'SET h.lock_version=coalesce(h.lock_version,0)+1 '
                         'RETURN h.seq AS seq,h.payload AS payload,h.checksum AS checksum',
                         run_id=run_id).single()
            previous = None
            if row['payload'] is not None:
                previous = json.loads(row['payload'])
                if digest(previous) != row['checksum']:
                    raise KnowledgeError('Neo4j checkpoint checksum mismatch')
            if previous is not None and seq == row['seq']:
                if checkpoint_identity(previous) != checkpoint_identity(checkpoint):
                    raise KnowledgeError('conflicting duplicate Neo4j commit')
                batch_row = tx.run('MATCH (c:WarehouseLiveCommit {run_id:$run_id,seq:$seq}) '
                                   'RETURN c.events_checksum AS checksum', run_id=run_id, seq=seq).single()
                if batch_row is None or batch_row['checksum'] != digest(events):
                    raise KnowledgeError('conflicting duplicate Neo4j events')
                return
            if seq != row['seq'] + 1:
                raise KnowledgeError('non-contiguous Neo4j commit sequence')
            batch = make_batch(checkpoint, events, previous)
            tx.run('CREATE (c:WarehouseLiveCommit {run_id:$run_id,seq:$seq,tick:$tick,'
                   'checksum:$checksum,events_checksum:$events_checksum,payload:$payload}) '
                   'WITH c MATCH (h:WarehouseLiveCheckpoint {run_id:$run_id}) '
                   'MERGE (c)-[:CHECKPOINT_HEAD]->(h)', run_id=run_id, seq=seq,
                   tick=checkpoint['tick'], checksum=digest(batch), events_checksum=digest(events),
                   payload=canonical(batch).decode()).consume()
            tx.run('MATCH (h:WarehouseLiveCheckpoint {run_id:$run_id}) '
                   'SET h.seq=$seq,h.tick=$tick,h.payload=$payload,h.checksum=$checksum',
                   run_id=run_id, seq=seq, tick=checkpoint['tick'],
                   payload=canonical(checkpoint).decode(), checksum=digest(checkpoint)).consume()

        try:
            with self.driver.session(database=self.database) as session:
                session.execute_write(write)
        except Exception as exc:
            self.last_error = 'Neo4j direct commit failed: ' + type(exc).__name__
            raise KnowledgeError(self.last_error) from exc
        self._sequences[run_id] = seq
        self.last_error = None
        self.timings['mirror_seconds'] += time.perf_counter() - started
        self.timings['commits'] += 1

    def status(self, run_id):
        seq = self._sequences.get(run_id, 0)
        return {'backend': 'neo4j-direct', 'run_id': run_id, 'local_journal': False,
                'durable_seq': seq, 'direct_commit_seq': seq, 'mirrored_seq': 0, 'lag': 0,
                'last_error': self.last_error, 'watermark_scope': 'live-commits-not-actor-steps'}

    def close(self):
        self.driver.close()
