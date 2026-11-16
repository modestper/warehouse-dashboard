"""Read-only comparison of fixed warehouse task sets; no simulation or DB I/O."""
import argparse
from collections import Counter
import csv
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

ROOT = Path(__file__).resolve().parents[1]
SLOT = re.compile(r'^(R\d+)-L(\d+)-C(\d+)-F(\d+)$')


def read_csv(path):
    with path.open(newline='', encoding='utf-8-sig') as stream:
        return list(csv.DictReader(stream))


def temporal(rows, start, duration, interval):
    bins = [0] * math.ceil(duration/interval)
    for row in rows:
        bins[(int((datetime.fromisoformat(row['release_time'])-start).total_seconds()))//interval] += 1
    return bins


def slot_id(row):
    return row['target_id'] if row['task_type'] == 'INBOUND' else row['source_id']


def analyze(directory, scenes):
    rows = read_csv(directory/'tasks.csv')
    inventory = read_csv(directory/'initial_inventory.csv')
    manifest = json.loads((directory/'manifest.json').read_text())
    start = datetime.fromisoformat(manifest['start'])
    duration = manifest['duration_seconds']
    release = sorted(int((datetime.fromisoformat(row['release_time'])-start).total_seconds()) for row in rows)
    types = Counter(row['task_type'] for row in rows)
    assert len(rows) == manifest['tasks'], directory
    assert len(set(row['task_id'] for row in rows)) == len(rows), directory
    assert types['INBOUND'] == manifest['inbound'] and types['OUTBOUND'] == manifest['outbound'], directory
    assert len(inventory) == manifest['initial_inventory'], directory
    assert all(0 <= second < duration for second in release), directory
    assert all(SLOT.fullmatch(slot_id(row)) for row in rows), directory
    for name, expected in manifest['files'].items():
        assert hashlib.sha256((directory/name).read_bytes()).hexdigest() == expected, (directory,name)
    scene = scenes.get(manifest.get('scene_sha256'))
    rack_counts = Counter(SLOT.fullmatch(slot_id(row))[1] for row in rows)
    slots = Counter(slot_id(row) for row in rows)
    levels = Counter(SLOT.fullmatch(slot_id(row))[2] for row in rows)
    rack_total = len(scene['data']['racks']) if scene else None
    top_n = math.ceil(rack_total*.2) if rack_total else None
    minute = temporal(rows,start,duration,60)
    five_minutes = temporal(rows,start,duration,300)
    hourly = temporal(rows,start,duration,3600)
    five_minute_inbound = temporal([r for r in rows if r['task_type']=='INBOUND'],start,duration,300)
    assert sum(minute) == sum(five_minutes) == sum(hourly) == len(rows), directory
    gaps = [(b-a,a,b) for a,b in zip([0]+release,release+[duration])]
    peak5 = max(range(len(five_minutes)),key=five_minutes.__getitem__)
    deadlines = [(datetime.fromisoformat(row['due_time'])-datetime.fromisoformat(row['release_time'])).total_seconds() for row in rows]
    stations = Counter(row['source_id'] if row['task_type']=='INBOUND' else row['target_id'] for row in rows)
    hot_slots = set(manifest.get('hotspot_slot_ids',[]))
    result = {
        'dataset':str(directory.relative_to(ROOT/'data/examples')),
        'path':str(directory.relative_to(ROOT)), 'tasks':len(rows),
        'inbound':types['INBOUND'],'outbound':types['OUTBOUND'],
        'initial_inventory':len(inventory),'duration_seconds':duration,
        'scenario':manifest.get('scenario'),'start':manifest['start'],
        'scene':scene['path'] if scene else None,'rack_total':rack_total,
        'capacity':scene['data']['capacity'] if scene else manifest.get('capacity'),
        'mean_tasks_per_hour':len(rows)*3600/duration,
        'first_release_second':release[0],'last_release_second':release[-1],
        'max_same_second':max(Counter(release).values()),
        'max_minute':max(minute),'max_five_minutes':max(five_minutes),
        'peak_five_minute_range':[peak5*300,min(duration,(peak5+1)*300)],
        'hourly_min':min(hourly),'hourly_max':max(hourly),
        'hourly_cv':statistics.pstdev(hourly)/statistics.mean(hourly),
        'empty_minutes':minute.count(0),'total_minutes':len(minute),
        'longest_release_gap':max(gaps),
        'rack_coverage':len(rack_counts),'slot_coverage':len(slots),
        'rack_task_range_over_all_racks':[
            min(rack_counts.get(r['id'],0) for r in scene['data']['racks']),max(rack_counts.values())] if scene else None,
        'top_racks':rack_counts.most_common(10),
        'top_20pct_rack_count':top_n,
        'top_20pct_racks_task_fraction':sum(v for _,v in rack_counts.most_common(top_n))/len(rows) if top_n else None,
        'level_counts':dict(sorted(levels.items())),
        'rack_counts':dict(sorted(rack_counts.items())),
        'station_counts':dict(sorted(stations.items())),
        'logical_boundaries':any(s in ('INBOUND','OUTBOUND') for s in stations),
        'declared_hotspot_slots':len(hot_slots),
        'tasks_in_declared_hotspot':sum(slot_id(row) in hot_slots for row in rows) if hot_slots else None,
        'deadline_seconds':{'min':min(deadlines),'median':statistics.median(deadlines),'max':max(deadlines)},
        'minute_tasks':minute,'five_minute_tasks':five_minutes,'hourly_tasks':hourly,
        'five_minute_inbound':five_minute_inbound,
    }
    if manifest.get('scenario') == 'peak':
        windows = [(round(duration*a),round(duration*b)) for a,b in ((.05,.20),(.40,.55),(.75,.90))]
        result['peak_waves'] = [{'start':a,'end':b,'tasks':sum(a<=s<b for s in release)} for a,b in windows]
    if (directory/'profiles-report.json').exists():
        report=json.loads((directory/'profiles-report.json').read_text())
        definitions=report['spatial_definitions']
        result['days']=[]
        for day in report['days']:
            i=day['day_number']-1
            selected=[row for row in rows if int((datetime.fromisoformat(row['release_time'])-start).total_seconds())//86400==i]
            day_start=start + timedelta(days=i)
            hours=temporal(selected,day_start,86400,3600)
            same_second=Counter(row['release_time'] for row in selected)
            directions=Counter(row['task_type'] for row in selected)
            assert len(selected)==day['tasks'] and hours==day['hourly_tasks'],i
            spatial_counts=Counter(SLOT.fullmatch(slot_id(row))[1] for row in selected)
            assert dict(spatial_counts)==day['rack_task_counts'],i
            segments=[]
            for a,b in ((0,8),(8,16),(16,24)):
                group=[row for row in selected if a<=datetime.fromisoformat(row['release_time']).hour<b]
                segments.append({'hours':[a,b],**dict(Counter(row['task_type'] for row in group))})
            hot_a=set(definitions['hotspot_a']['slot_ids'])
            hot_b=set(definitions['hotspot_b']['slot_ids'])
            morning=[row for row in selected if datetime.fromisoformat(row['release_time']).hour<12]
            afternoon=[row for row in selected if datetime.fromisoformat(row['release_time']).hour>=12]
            if day['profile']=='local_hotspot':
                assert sum(slot_id(r) in hot_a for r in selected)==day['hotspot_statistics']['inside_current_hotspot']
            elif day['profile']=='moving_hotspot':
                assert (sum(slot_id(r) in hot_a for r in morning)+sum(slot_id(r) in hot_b for r in afternoon)
                        ==day['hotspot_statistics']['inside_current_hotspot'])
            elif day['profile']=='mixed_recovery':
                high=[r for r in selected if 6<=datetime.fromisoformat(r['release_time']).hour<12]
                hot=set(definitions['mixed_hotspot']['slot_ids'])
                assert sum(slot_id(r) in hot for r in high)==day['hotspot_statistics']['inside_current_hotspot']
            result['days'].append({
                'day':i+1,'profile':day['profile'],'tasks':len(selected),
                'inbound':directions['INBOUND'],'outbound':directions['OUTBOUND'],
                'hourly_tasks':hours,'hourly_min':min(hours),'hourly_max':max(hours),
                'max_same_second':max(same_second.values()),'same_second_batch_sizes':dict(Counter(same_second.values())),
                'peak_window_tasks':sum(hours[h] for h in (8,9,14,15,20,21)),
                'six_hour_counts':[sum(hours[a:a+6]) for a in (0,6,12,18)],
                'top_racks':spatial_counts.most_common(8),'eight_hour_type_counts':segments,
                'hotspot_statistics':day['hotspot_statistics'],
                'morning_hotspot_a_fraction':sum(slot_id(r) in hot_a for r in morning)/len(morning),
                'afternoon_hotspot_b_fraction':sum(slot_id(r) in hot_b for r in afternoon)/len(afternoon),
            })
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,required=True)
    args=parser.parse_args()
    scenes={}
    for path in (ROOT/'data/layouts').rglob('*.json'):
        data=json.loads(path.read_text())
        if 'spec' in data and 'racks' in data:
            scenes[hashlib.sha256(path.read_bytes()).hexdigest()]={'data':data,'path':str(path.relative_to(ROOT))}
    result=[analyze(path.parent,scenes) for path in sorted((ROOT/'data/examples').rglob('tasks.csv'))]
    comparisons=[]
    for old,new in (('warehouse-v5','wheeled-medium-sample19'),('regression/peak','wheeled-medium-peak')):
        before={r['task_id']:r for r in read_csv(ROOT/'data/examples'/old/'tasks.csv')}
        after={r['task_id']:r for r in read_csv(ROOT/'data/examples'/new/'tasks.csv')}
        assert before.keys()==after.keys(),(old,new)
        fields=next(iter(before.values())).keys()
        comparisons.append({'before':old,'after':new,'changed_rows_by_field':{
            key:sum(before[t][key]!=after[t][key] for t in before) for key in fields}})
    args.output_dir.mkdir(parents=True,exist_ok=True)
    (args.output_dir/'statistics.json').write_text(json.dumps({'datasets':result,'comparisons':comparisons},ensure_ascii=False,indent=2)+'\n')
    columns=['dataset','tasks','inbound','outbound','duration_seconds','mean_tasks_per_hour','first_release_second',
             'last_release_second','max_same_second','max_minute','max_five_minutes','rack_coverage','rack_total','slot_coverage','logical_boundaries']
    with (args.output_dir/'summary.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=columns,extrasaction='ignore')
        writer.writeheader();writer.writerows(result)
    with (args.output_dir/'five-minute-distribution.csv').open('w',newline='') as stream:
        writer=csv.writer(stream);writer.writerow(['dataset','start_second','end_second','tasks','inbound','outbound'])
        for item in result:
            for i,count in enumerate(item['five_minute_tasks']):
                inbound=item['five_minute_inbound'][i]
                writer.writerow([item['dataset'],i*300,min(item['duration_seconds'],(i+1)*300),count,inbound,count-inbound])
    for item in result:
        print(json.dumps({k:v for k,v in item.items() if k in ('dataset','tasks','inbound','outbound','duration_seconds',
            'mean_tasks_per_hour','max_minute','max_five_minutes','rack_coverage','slot_coverage')},ensure_ascii=False))
    print('PAIRS',json.dumps(comparisons,ensure_ascii=False))


if __name__=='__main__':main()
