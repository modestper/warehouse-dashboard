"""Offline copy-contract checks; these do not substitute for a real Neo4j run."""
from collections import Counter
from copy import deepcopy
import importlib.util
from pathlib import Path
import re
import unittest

SPEC=importlib.util.spec_from_file_location('copy_run',Path(__file__).resolve().parents[1]/'scripts/copy_neo4j_run.py')
MOD=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(MOD)


class Result(list):
    def single(self):return self[0] if self else None
    def data(self):return list(self)
    def consume(self):return None


class Source:
    def __init__(self):
        from neo4j.time import Date
        self.nodes={
            '0':{'id':'run-A','labels':['Run']},
            '1':{'id':'a-robot','run_id':'run-A','labels':['Robot','特殊`标签'],
                 'values':[1.,2.], 'date':Date(2026,11,10),'raw':bytearray(b'abc')},
            '2':{'run_id':'run-A','labels':['AgentState'],'tick':1},
            '3':{'id':'run-B','labels':['Run']},
            '4':{'run_id':'run-B','labels':['Robot']},
            '5':{'id':'shared-dataset','labels':['Dataset']},
        }
        self.edges=[('1','0','IN_RUN',{}),('2','1','ABOUT',{'value':1}),
                    ('2','1','ABOUT',{'value':2}),('1','1','SELF',{'ok':True}),
                    ('4','3','IN_RUN',{}),('0','5','USES_DATASET',{}),
                    ('4','1','FOREIGN',{})]
        self.queries=[]
        self.change_count=False
        self.count_reads=0
    def selected(self,run):
        return {key for key,n in self.nodes.items() if n.get('run_id')==run or 'Run' in n['labels'] and n.get('id')==run}
    def run(self,query,**params):
        self.queries.append(query)
        assert not any(word in query for word in ('CREATE ','DELETE ','SET ','REMOVE ','DROP '))
        chosen=self.selected(params['run'])
        if 'AS latest_step' in query:return Result([{'id':'0','latest_step':1}])
        if 'count(n)' in query:
            self.count_reads+=1
            return Result([{'n':len(chosen)+(self.change_count and self.count_reads>1)}])
        if 'count(r)' in query and '<>' not in query:
            return Result([{'n':sum(a in chosen and b in chosen for a,b,_,_ in self.edges)}])
        if '<>' in query:
            counts=Counter(t for a,b,t,_ in self.edges if (a in chosen)!=(b in chosen))
            return Result([{'type':t,'n':n} for t,n in sorted(counts.items())])
        if 'labels(n) AS labels' in query:
            return Result([{'id':key,'labels':n['labels'],
                'props':{k:deepcopy(v) for k,v in n.items() if k!='labels'}}
                for key,n in self.nodes.items() if key in chosen])
        if 'properties(r)' in query:
            return Result([{'start':a,'end':b,'type':t,'props':deepcopy(p)}
                for a,b,t,p in self.edges if a in chosen and b in chosen])
        raise AssertionError(query)


class Target:
    def __init__(self):self.nodes={};self.edges=[];self.constraint=False;self.writes=[]
    def execute_write(self,fn,rows):return fn(self,rows)
    def run(self,query,**params):
        if query=='MATCH (n) RETURN n LIMIT 1':return Result([{'n':next(iter(self.nodes))}]) if self.nodes else Result()
        if query.startswith('SHOW CONSTRAINTS'):return Result([{'name':MOD.CONSTRAINT}]) if self.constraint else Result()
        if query.startswith('CREATE CONSTRAINT'):
            self.constraint=True;self.writes.append(query);return Result()
        if query.startswith('CALL '):return Result()
        if 'CREATE (n:' in query:
            labels=[x.replace('``','`') for x in re.findall(r'`((?:``|[^`])*)`',query.split(' SET ')[0])]
            for row in params['rows']:
                self.nodes[row['id']]={'labels':labels.copy(),'props':{**deepcopy(row['props']),MOD.KEY:row['id']}}
            return Result([{'n':len(params['rows'])}])
        if 'CREATE (a)-[r:' in query:
            kind=re.search(r'\[r:`((?:``|[^`])*)`',query).group(1).replace('``','`')
            count=0
            for row in params['rows']:
                if row['start'] in self.nodes and row['end'] in self.nodes:
                    self.edges.append((row['start'],row['end'],kind,deepcopy(row['props'])));count+=1
            return Result([{'n':count}])
        if query=='MATCH (n) RETURN count(n) AS n':return Result([{'n':len(self.nodes)}])
        if query=='MATCH ()-[r]->() RETURN count(r) AS n':return Result([{'n':len(self.edges)}])
        if 'UNWIND labels(n)' in query:
            counts=Counter(x for n in self.nodes.values() for x in n['labels'])
            return Result([{'label':k,'n':v} for k,v in counts.items()])
        if query.startswith('MATCH ()-[r]->() RETURN type(r)'):
            return Result([{'type':k,'n':v} for k,v in Counter(e[2] for e in self.edges).items()])
        if 'REMOVE n:' in query:
            chosen=[n for n in self.nodes.values() if MOD.LABEL in n['labels']][:params['batch']]
            for n in chosen:n['labels'].remove(MOD.LABEL);n['props'].pop(MOD.KEY)
            return Result([{'n':len(chosen)}])
        if query.startswith('DROP CONSTRAINT'):
            self.constraint=False;return Result()
        raise AssertionError(query)


class CopyContractTests(unittest.TestCase):
    def test_only_selected_run_and_induced_edges_preserve_parallel_edges_loops_and_native_types(self):
        source,target=Source(),Target();original=deepcopy(source.nodes)
        report=MOD.copy_run(source,target,'run-A',size=2,copy_schema=False)
        self.assertEqual(report['status'],'complete')
        self.assertEqual(report['nodes'],3);self.assertEqual(report['relationships'],4)
        self.assertEqual(set(target.nodes),{'0','1','2'})
        self.assertEqual([e[3]['value'] for e in target.edges if e[2]=='ABOUT'],[1,2])
        self.assertEqual(report['excluded_cross_boundary_relationships'],[
            {'type':'FOREIGN','n':1},{'type':'USES_DATASET','n':1}])
        for key,node in target.nodes.items():
            self.assertEqual(set(node['labels']),set(original[key]['labels']))
            self.assertEqual(node['props'],{k:v for k,v in original[key].items() if k!='labels'})
        self.assertEqual(source.nodes,original)
        self.assertFalse(target.constraint)
    def test_nonempty_target_refused_before_any_write(self):
        source,target=Source(),Target();target.nodes['keep']={'labels':['Keep'],'props':{'x':1}}
        with self.assertRaisesRegex(ValueError,'不是空库'):MOD.copy_run(source,target,'run-A')
        self.assertEqual(target.writes,[]);self.assertEqual(list(target.nodes),['keep'])
    def test_unknown_run_does_not_create_schema(self):
        target=Target()
        with self.assertRaisesRegex(ValueError,'没有找到'):MOD.copy_run(Source(),target,'missing')
        self.assertEqual(target.writes,[])
    def test_source_change_refuses_success_and_keeps_partial_target_for_inspection(self):
        source,target=Source(),Target();source.change_count=True
        with self.assertRaisesRegex(RuntimeError,'源 run 发生变化'):
            MOD.copy_run(source,target,'run-A',copy_schema=False)
        self.assertTrue(target.constraint)
        self.assertTrue(all(MOD.LABEL in n['labels'] for n in target.nodes.values()))
    def test_schema_only_for_selected_labels_and_types(self):
        rows=[{'name':'robot','entityType':'NODE','labelsOrTypes':['Robot'],'createStatement':'CREATE robot'},
              {'name':'foreign','entityType':'NODE','labelsOrTypes':['Other'],'createStatement':'CREATE other'},
              {'name':'edge','entityType':'RELATIONSHIP','labelsOrTypes':['ABOUT'],'createStatement':'CREATE edge'}]
        session=type('Schema',(),{'run':lambda self,q:Result(rows if q.startswith('SHOW CONSTRAINTS') else [])})()
        self.assertEqual([r['name'] for r in MOD.schema_for(session,{'Robot'},{'ABOUT'})],['robot','edge'])
    def test_reserved_property_collision_never_overwrites_source_property(self):
        source,target=Source(),Target();source.nodes['1'][MOD.KEY]='keep'
        with self.assertRaisesRegex(ValueError,'保留标签/属性'):
            MOD.copy_run(source,target,'run-A',size=10,copy_schema=False)
        self.assertEqual(source.nodes['1'][MOD.KEY],'keep')


if __name__=='__main__':unittest.main()
