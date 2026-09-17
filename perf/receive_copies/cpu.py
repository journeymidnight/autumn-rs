"""Untraced windows: per-role process CPU seconds per GiB and host perf cycles.

cpu.py RESULTS_DIR > cpu.json  (reuses controlled_validation/analyze.py derive)
"""
import collections
import importlib.util
import json
from pathlib import Path
import statistics
import sys

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('controlled_analyze', HERE.parent / 'controlled_validation/analyze.py')
controlled = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controlled)

results = Path(sys.argv[1])
cells = collections.defaultdict(list)
for path in sorted(results.glob('*-p1-r*-*-*.json')):
    stem = path.stem
    if not stem.rsplit('-', 1)[-1].isdigit():
        continue
    data = json.loads(path.read_text())
    if data.get('trace') or 'role_before' not in data:
        continue
    row = controlled.derive(data, path.with_name(stem + '-perf.csv'))
    gib = row['bytes'] / 1024**3
    role = lambda names: sum(row['role_cpu'][n]['user'] + row['role_cpu'][n]['system'] for n in names) / gib
    cells[(row['transport'], row['size'], row['mode'], row['version'])].append(dict(
        trial=row['trial'], mib_per_sec=row['mib_per_sec'], p99_us=row['p99_us'],
        cpu_s_per_gib=row['process_cpu_s_per_gib'],
        en_s_per_gib=role(['en1', 'en2', 'en3']), ps_s_per_gib=role(['ps']),
        client_s_per_gib=role(['client']),
        cycles_u_per_gib=row['perf']['cycles:u'] / gib, cycles_k_per_gib=row['perf']['cycles:k'] / gib))
out = []
for (transport, size, mode, version), rows in sorted(cells.items()):
    med = lambda k: statistics.median(r[k] for r in rows)
    out.append(dict(transport=transport, size=size, mode=mode, version=version, n=len(rows),
                    samples=rows, **{k: med(k) for k in rows[0] if k != 'trial'}))
json.dump(out, sys.stdout, indent=1)
