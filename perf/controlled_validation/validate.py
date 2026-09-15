"""Audit completeness, partition distribution, affinity and counter windows."""
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
plan = json.loads((root.parent/'plan.json').read_text())
problems = []
cells = 0
max_envelope_ms = 0
for trial in plan:
    label = '{transport}-p{partitions}-r{repeat}-{version}'.format(**trial)
    for window in range(1,17):
        path = root/f'{label}-{window}.json'
        if not path.exists():
            problems.append(f'missing {path.name}')
            continue
        data = json.loads(path.read_text())
        cells += 1
        count = data['operations_per_partition']
        expected = trial['partitions'] * count
        if data['ops'] != expected or data['bytes'] != expected * data['size']:
            problems.append(f'operation/byte count {path.name}')
        if sorted(data['per_partition_ops']) != [[i,count] for i in range(trial['partitions'])]:
            problems.append(f'partition distribution {path.name}')
        before,after = data['role_before'],data['role_after']
        envelope = (after['start_ns']-before['start_ns'])/1e9
        extra = envelope-data['seconds']
        max_envelope_ms = max(max_envelope_ms,extra*1000)
        if extra < 0 or extra > max(.01,data['seconds']*.01):
            problems.append(f'CPU envelope {path.name}: {extra}s')
        for pid,a in before['tasks'].items():
            if after['tasks'][pid][2] != a[2]:
                problems.append(f'PID reuse {path.name}')
    affinity = root/(label+'-affinity.json')
    if affinity.exists():
        d=json.loads(affinity.read_text())
        cpus={int(x['cpus'].split(':')[1]) for x in d['ps']
              if x['name'].startswith('part-') and '-' not in x['cpus'].split(':')[1]}
        wanted=set(range(12,12+trial['partitions']*2))
        if not wanted.issubset(cpus):
            problems.append(f'affinity {label}: {cpus}, wanted {wanted}')
    if trial['trace'] and not (root/(label+'-trace.jsonl')).exists():
        problems.append(f'missing trace {label}')
audit=dict(expected_cells=len(plan)*16,actual_cells=cells,
           max_cpu_envelope_overhead_ms=max_envelope_ms,problems=problems)
(root/'audit.json').write_text(json.dumps(audit,indent=2))
print(json.dumps(audit,indent=2))
raise SystemExit(bool(problems))
