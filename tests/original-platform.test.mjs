import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {adaptPlatformFrame,platformControl} from '../dist/original-platform.mjs';
import {controlAvailability} from '../dist/live-model.mjs';
const scene=JSON.parse(readFileSync(new URL('../data/layouts/warehouse100-wide-v2.json',import.meta.url)));
const robot={robot_id:1,robot_type_key:'X3',x:2.25,y:2.4,battery:50,carrying:false,stage:'idle',task_id:null,cargo_id:null};
const frame=(tick=0,changes={})=>({runId:'real-run',runtimeInstanceId:'instance',tick,elapsed:tick,robots:[{...robot}],metrics:{completed:3},occupiedSlots:['R01-L1-C1-F1'],events:[],...changes});

test('activation cap and actual job count are visible without replacing the online fleet',()=>{
  const f=adaptPlatformFrame(frame(0,{activation:{active:49,limit:50}}),scene);
  assert.match(f.scene.name,/激活 49 \/ 50 台/);
  assert.equal(f.activation.limit,50);
});
test('original renderer receives real 36 racks, 1728 slots and exactly source occupancy',()=>{
  const f=adaptPlatformFrame(frame(),scene);
  assert.equal(f.racks.length,36);assert.equal(f.racks.flatMap(r=>r.slots).length,1728);
  assert.deepEqual(f.racks.flatMap(r=>r.slots).filter(s=>s.occupied).map(s=>s.slotId),['R01-L1-C1-F1']);
  assert.equal(f.racks[0].width,9.6);assert.equal(f.racks[0].depth,1.2);
});
test('robot source type, IDs, cargo and exact position survive without fake targets',()=>{
  const r=adaptPlatformFrame(frame(),scene).robots[0];
  assert.equal(r.typeKey,'X3');assert.equal(r.id,1);assert.equal(r.x,2.25);
  assert.equal(r.taskId,null);assert.equal(r.cargoId,null);assert.equal(r.rackId,null);assert.equal(r.slotId,null);
  assert.deepEqual(r.path,[]);assert.equal(r.actionDuration,0);
});
test('view aliases retain raw stage, carried cargo, and actual task identity',()=>{
  const raw={...robot,stage:'to_target',carrying:true,task_id:'lys-T',cargo_id:'lys-G'};
  const r=adaptPlatformFrame(frame(1,{robots:[raw]}),scene).robots[0];
  assert.equal(r.stage,'toRack');assert.equal(r.rawStage,'to_target');assert.equal(r.taskId,'lys-T');assert.equal(r.carrying,true);
});
test('waiting/charging and genuine fault are separate from an invented blocked flag',()=>{
  for(const stage of ['traffic_wait','charging'])assert.equal(adaptPlatformFrame(frame(1,{robots:[{...robot,stage}]}),scene).robots[0].category,'idle');
  assert.equal(adaptPlatformFrame(frame(1,{robots:[{...robot,stage:'fault_wait'}]}),scene).robots[0].category,'warning');
});
test('observed trail connects consecutive source ticks only and resets after gaps/runs/restarts',()=>{
  const trails=new Map(),a=frame(10),b=frame(11,{robots:[{...robot,x:2.45}]});
  adaptPlatformFrame(a,scene,null,trails);assert.equal(adaptPlatformFrame(b,scene,a,trails).robots[0].observedTrail.length,2);
  const gap=frame(15,{robots:[{...robot,x:2.85}]});assert.equal(adaptPlatformFrame(gap,scene,b,trails).robots[0].observedTrail.length,1);
  assert.equal(adaptPlatformFrame({...gap,runId:'next'},scene,gap,trails).robots[0].observedTrail.length,1);
  assert.equal(adaptPlatformFrame({...gap,runtimeInstanceId:'restart'},scene,gap,trails).robots[0].observedTrail.length,1);
});
test('pause repeats do not grow trails; bounded history retains at most 80 actual positions',()=>{
  const trails=new Map();let previous=null;
  for(let i=0;i<100;i++){const f=frame(i,{robots:[{...robot,x:i/5}]});adaptPlatformFrame(f,scene,previous,trails);previous=f;}
  assert.equal(adaptPlatformFrame(previous,scene,previous,trails).robots[0].observedTrail.length,80);
});
test('source events retain exact IDs and use reverse chronological simulation time',()=>{
  const f=adaptPlatformFrame(frame(2,{events:[{kind:'picked',sim_time:1,task_id:'T<unsafe>'},{kind:'placed',sim_time:2,goods_id:'G'}]}),scene);
  assert.equal(f.events[0].text,'placed · G');assert.match(f.events[1].text,/T<unsafe>/);
});
test('original control names map to the same single writer with run fence and dataset alias',()=>{
  assert.deepEqual(platformControl('set_speed',20,frame(),'week'),{action:'speed',value:20,expected_run_id:'real-run'});
  assert.deepEqual(platformControl('reset',null,frame(),'week'),{action:'new_run',dataset_id:'week',expected_run_id:'real-run'});
  assert.throws(()=>platformControl('set_flow_rate',999,frame(),'week'));
  assert.throws(()=>platformControl('pause',null,null,'week'));
});
test('new run uses the requested name independently of the current run fence',()=>{
  assert.deepEqual(platformControl('reset',null,frame(),'week','  exp-20261104_01.v2  '),{
    action:'new_run',dataset_id:'week',run_id:'exp-20261104_01.v2',expected_run_id:'real-run'});
  assert.deepEqual(platformControl('reset',null,frame(),'week','   '),{
    action:'new_run',dataset_id:'week',expected_run_id:'real-run'});
  for(const name of ['../old','bad/name','.hidden','中文','a b','a'.repeat(129)]){
    assert.throws(()=>platformControl('reset',null,frame(),'week',name),/runId/);
  }
  assert.equal(platformControl('reset',null,frame(),'week','a'.repeat(128)).run_id.length,128);
  assert.deepEqual(platformControl('pause',null,frame(),'week','unused'),{action:'pause',expected_run_id:'real-run'});
});
test('max tick and run ID travel together, blanks keep automatic naming and unlimited ticks',()=>{
  assert.deepEqual(platformControl('reset',null,frame(),'week','limited-run',' 10000 '),{
    action:'new_run',expected_run_id:'real-run',dataset_id:'week',run_id:'limited-run',max_ticks:10000});
  assert.deepEqual(platformControl('reset',null,frame(),'week','',''),{
    action:'new_run',expected_run_id:'real-run',dataset_id:'week'});
  assert.equal(platformControl('reset',null,frame(),'week','','1').max_ticks,1);
  assert.equal(platformControl('reset',null,frame(),'week','','2147483647').max_ticks,2147483647);
  for(const limit of ['0','-1','1.5','1e4','NaN','Infinity','2147483648',true,[],{}]){
    assert.throws(()=>platformControl('reset',null,frame(),'week','valid-run',limit),/最大 tick/);
  }
  assert.deepEqual(platformControl('pause',null,frame(),'week','unused','bad'),{
    action:'pause',expected_run_id:'real-run'});
});
test('limit disables both resume and single step while preserving new-run controls',()=>{
  const capped=frame(3,{status:'paused',running:false,maxTicks:3,tickLimitReached:true});
  assert.equal(controlAvailability(capped).start,false);
  assert.equal(controlAvailability(capped).step,false);
  assert.equal(controlAvailability(capped).new_run,true);
  assert.equal(controlAvailability({...capped,tick:2,tickLimitReached:false}).start,true);
});
test('formal entry retains original canvases, inspector, gestures and on-demand knowledge',()=>{
  const html=readFileSync(new URL('../dist/index.html',import.meta.url),'utf8');
  for(const id of ['warehouseCanvas','robotPortrait','selectedTaskId','filterList','levelSelect','playButton','datasetSelect','queryKind','queryResult','newRunMaxTicks','runTickLimit'])assert.match(html,new RegExp(`id="${id}"`));
  assert.match(html,/type="module" src="\.\/app\.js/);assert.match(html,/Shift \/ 右键平移/);
  assert.doesNotMatch(html,/<script[^>]*src="\.\/live\.js/);
  const app=readFileSync(new URL('../dist/app.js',import.meta.url),'utf8');
  assert.match(app,/if\(window.location.protocol==="file:"\)initializeSimulation\(\)/);
  assert.match(app,/drawRackCached/);assert.match(app,/backendSocket!==currentSocket/);
});
