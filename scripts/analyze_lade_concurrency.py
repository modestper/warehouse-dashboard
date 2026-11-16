"""Run the existing LaDe local engine and save a concurrency witness.

No Neo4j, Ray, LLM, or LaDe source/output files are changed. All task loading,
dispatch, assistance, battery, and motion behavior comes from LaDe itself.
"""
import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import sys
import time


class RecentFrames:
    """Retain one frame while preserving the engine's global step counter."""

    def __init__(self):
        self.count = 0
        self.latest = None

    def append(self, frame):
        self.count += 1
        self.latest = frame

    def __len__(self):
        return self.count


def sha256(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--lade-root', type=Path, default=Path('/data/zc/datasets/LaDe/viewer'))
    p.add_argument('--city', default='Shanghai')
    p.add_argument('--day', default='0621')
    p.add_argument('--drones', type=int, default=100)
    p.add_argument('--step-seconds', type=int, default=20)
    p.add_argument('--seed', type=int, default=13)
    p.add_argument('--max-sim-hours', type=float, default=36)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    if a.drones <= 0 or a.step_seconds <= 0 or a.max_sim_hours <= 0:
        p.error('drones, step-seconds and max-sim-hours must be positive')
    a.output.mkdir(parents=True, exist_ok=False)
    sys.path.insert(0, str(a.lade_root.resolve()))
    from scripts.build_delivery_sim import CITY_FILES
    from simulation.delivery_sim import (
        AuctionCoordinator, SimulationEnv, build_region_depots,
        load_delivery_tasks, load_fixed_region_depots, make_region_drones,
        current_target,
    )
    from simulation.planner import RuleBasedPlanner

    class ObservedEnv(SimulationEnv):
        def release_and_assign(self):
            super().release_and_assign()
            # Observe the dispatch boundary too: jobs that finish within one
            # step must not disappear from a concurrency existence check.
            self.dispatch_frame = self.snapshot([])
    started = time.monotonic()
    csv_path = a.lade_root.parent / 'delivery' / CITY_FILES[a.city]
    tasks = load_delivery_tasks(csv_path, city=a.city, day=a.day,
                                limit=sys.maxsize, sample_seed=a.seed)
    if not tasks:
        raise ValueError('No valid tasks matched the requested city and day')
    depots = build_region_depots(tasks)
    charging_depots = load_fixed_region_depots(a.city)
    drones = make_region_drones(a.drones, depots, capacity=6, seed=a.seed)
    for drone in drones:
        drone.planner = RuleBasedPlanner()
    env = ObservedEnv(tasks, drones, AuctionCoordinator(),
                        step_minutes=a.step_seconds / 60,
                        charging_depots=list(charging_depots.values()))

    # Verify the bounded frame collector against the unmodified engine's list.
    # The same task/actor input is replayed, including event and Step history.
    control = SimulationEnv(deepcopy(tasks), deepcopy(drones), AuctionCoordinator(),
                            step_minutes=a.step_seconds / 60,
                            charging_depots=list(charging_depots.values()))
    bounded = deepcopy(env)
    bounded.frames = RecentFrames()
    for _ in range(12):
        assert control.step_once() == bounded.step_once()
    assert control.knowledge_store.events == bounded.knowledge_store.events
    assert control.knowledge_store.steps == bounded.knowledge_store.steps
    del control, bounded
    env.frames = RecentFrames()
    modules = ['delivery_sim.py', 'planner.py', 'knowledge.py', 'ray_delivery_sim.py']
    config = {
        'city': tasks[0].city, 'day': a.day, 'all_valid_day_tasks': True,
        'task_count': len(tasks), 'drones': a.drones, 'seed': a.seed,
        'step_seconds': a.step_seconds, 'depot_mode': 'region',
        'coordinator': 'AuctionCoordinator', 'planner': 'RuleBasedPlanner',
        'engine': 'SimulationEnv (local), with read-only dispatch observation', 'knowledge_backend': 'TemporalKnowledgeStore (memory)',
        'active_regions': len(depots), 'charging_depots': len(charging_depots),
        'first_release_minute': min(t.release_minute for t in tasks),
        'last_release_minute': max(t.release_minute for t in tasks),
        'queue_capacity': drones[0].queue_capacity, 'cargo_capacity': drones[0].cargo_capacity,
        'speed_m_per_minute': drones[0].speed_m_per_minute,
        'battery_capacity_m': drones[0].battery_capacity_m,
        'low_battery_threshold_m': drones[0].low_battery_threshold_m,
        'csv_path': str(csv_path), 'csv_sha256': sha256(csv_path),
        'source_sha256': {name: sha256(a.lade_root / 'simulation' / name) for name in modules},
        'fixed_depots_sha256': sha256(a.lade_root / 'data/fixed_region_depots.json'),
        'collector_equivalence_verified_steps': 12,
        'stop_policy': 'first all-drone positive-energy business witness at dispatch/end-step; or proved all-energy-exhausted absorbing state; or horizon/completion',
    }
    save(a.output / 'config.json', config)
    print(json.dumps({'loaded': config, 'load_wall_seconds': time.monotonic()-started}, ensure_ascii=False), flush=True)
    max_steps = math.ceil(a.max_sim_hours * 3600 / a.step_seconds)
    peaks = {'delivering': 0, 'business_positive_battery': 0, 'moving': 0, 'carrying': 0,
             'dispatch_business_positive_battery': 0}
    peak_frames = {}
    peak_observations = {}
    first_status_hundred = None
    witness = None
    absorbing = None
    last_progress = time.monotonic()
    with (a.output / 'concurrency.csv').open('w') as series:
        series.write('step,time_minute,time_label,delivering,business_positive_battery,moving,carrying,idle,returning,charging,completed,dispatch_business_positive_battery\n')
        for step in range(1, max_steps + 1):
            before = {d.drone_id: d.position for d in drones}
            frame = env.step_once()
            dispatch = env.dispatch_frame
            dispatch_active = [r for r in dispatch['drones'] if r['queue']
                               and r['status'] in ('queued', 'delivering')
                               and r['battery_remaining_m'] > 1e-9 and r['target'] is not None]
            statuses = Counter(d.status for d in drones)
            active = [d for d in drones if d.status == 'delivering' and d.queue
                      and d.battery_remaining_m > 1e-9 and current_target(d) is not None]
            moving = sum(math.dist(before[d.drone_id], d.position) > 1e-8 for d in drones)
            carrying = sum(bool(d.onboard_tasks) for d in drones)
            counts = {'delivering': statuses['delivering'], 'business_positive_battery': len(active),
                      'moving': moving, 'carrying': carrying,
                      'dispatch_business_positive_battery': len(dispatch_active)}
            for key, value in counts.items():
                if value > peaks[key]:
                    peaks[key] = value
                    observed_frame = dispatch if key.startswith('dispatch_') else frame
                    peak_frames[key] = observed_frame
                    peak_observations[key] = {'value': value, 'step': step,
                        'time_minute': observed_frame['time_minute'],
                        'time_label': observed_frame['time_label'],
                        'boundary': 'after_dispatch' if key.startswith('dispatch_') else 'after_motion'}
            if counts['delivering'] == a.drones and first_status_hundred is None:
                first_status_hundred = {'step': step, 'time_minute': env.now_minute,
                                        'time_label': frame['time_label'], **counts}
            series.write(','.join(map(str, [step, env.now_minute, frame['time_label'],
                counts['delivering'], len(active), moving, carrying, statuses['idle'],
                statuses['returning'], statuses['charging'], sum(len(d.completed) for d in drones), len(dispatch_active)])) + '\n')
            if len(active) == a.drones or len(dispatch_active) == a.drones:
                observed = dispatch if len(dispatch_active) == a.drones else frame
                ids = {r['id'] for r in observed['drones']}
                assert len(ids) == a.drones
                assert all(t.assigned_to == d.drone_id for d in drones for t in d.queue)
                witness = {'step': step, 'time_minute': env.now_minute,
                           'time_label': frame['time_label'], **counts,
                           'boundary': 'after_dispatch' if observed is dispatch else 'after_motion',
                           'observed_time_minute': observed['time_minute'],
                           'observed_time_label': observed['time_label'],
                           'min_battery_remaining_m': min(r['battery_remaining_m'] for r in observed['drones']),
                           'total_queued_tasks': sum(len(r['queue']) for r in observed['drones']),
                           'task_ownership_verified': True, 'unique_drones': len(ids)}
                save(a.output / 'witness-frame.json', observed)
                break
            # With zero energy and no drone at any charging depot, none can
            # ever reach a charger. The unchanged engine has no exogenous
            # charging/teleport mechanism; future orders cannot restore energy.
            if all(d.battery_remaining_m == 0 for d in drones):
                distances = [min(math.dist(d.position, (x['x'], x['y']))
                                 for x in charging_depots.values()) for d in drones]
                if min(distances) > 1e-8:
                    absorbing = {'step': step, 'time_minute': env.now_minute,
                        'time_label': frame['time_label'],
                        'zero_energy_drones': len(drones),
                        'min_distance_to_any_charger_m': min(distances),
                        'statuses': dict(statuses),
                        'proof': 'all batteries exactly zero; no drone at any charger; only motion-to-depot can recharge; no external interventions configured'}
                    save(a.output / 'energy-exhausted-frame.json', frame)
                    break
            if env.is_complete():
                break
            if time.monotonic() - last_progress >= 15:
                print(json.dumps({'progress_step': step, 'time': frame['time_label'],
                    'counts': counts, 'peaks': peaks, 'wall_seconds': time.monotonic()-started}), flush=True)
                series.flush()
                last_progress = time.monotonic()
    for key, observed_frame in peak_frames.items():
        save(a.output / ('peak-' + key + '-frame.json'), observed_frame)
    result = {'status': 'witness_found' if witness else ('energy_exhausted_absorbing_state' if absorbing else ('complete_no_witness' if env.is_complete() else 'horizon_no_witness')),
              'config_file': 'config.json', 'processed_steps': step,
              'end_time_minute': env.now_minute, 'end_time_label': frame['time_label'],
              'peaks_until_stop': peaks, 'first_all_delivering': first_status_hundred,
              'peak_observations': peak_observations, 'absorbing_state': absorbing,
              'witness': witness, 'completed_tasks_at_stop': sum(t.status == 'completed' for t in tasks),
              'wall_seconds': time.monotonic()-started,
              'whole_day_completed': env.is_complete()}
    save(a.output / 'result.json', result)
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
