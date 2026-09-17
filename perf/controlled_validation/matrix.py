"""Sequential, rotated trial order. Never overlap benchmark clusters."""
import json
from pathlib import Path
import subprocess
import sys

root = Path('/data08/autumn-controlled-validation')
driver = root / 'source/perf/controlled_validation/run.py'
plan = []
for repeat in range(3):
    for transport in ['tcp', 'ucx']:
        versions = ['baseline', 'pure', 'zc'] if transport == 'tcp' else ['baseline', 'pure']
        versions = versions[repeat % len(versions):] + versions[:repeat % len(versions)]
        for partitions in [1, 4]:
            for version in versions:
                plan.append(dict(repeat=repeat, transport=transport, partitions=partitions,
                                 version=version, trace=False))
# Diagnostics repeat exactly the work with BPF enabled. Compare probe overhead
# against the matching untraced runs; do not mix them into throughput medians.
for transport in ['tcp', 'ucx']:
    versions = ['baseline', 'pure', 'zc'] if transport == 'tcp' else ['baseline', 'pure']
    for partitions in [1, 4]:
        for version in versions:
            plan.append(dict(repeat=10, transport=transport, partitions=partitions,
                             version=version, trace=True))
(root / 'plan.json').write_text(json.dumps(plan, indent=2))
for trial in plan:
    label = '{transport}-p{partitions}-r{repeat}-{version}'.format(**trial)
    directory = root / 'trials' / label
    if (directory / 'completed').exists():
        continue
    if directory.exists():
        raise RuntimeError(f'incomplete trial {label}: inspect before restarting')
    command = [sys.executable, str(driver)]
    for name in ['transport', 'partitions', 'repeat', 'version']:
        command += ['--' + name, str(trial[name])]
    if trial['trace']:
        command.append('--trace')
    print('START', label, 'trace=' + str(trial['trace']), flush=True)
    with (root / 'logs' / (label + '.log')).open('w') as output:
        run = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT)
    print('END', label, run.returncode, flush=True)
    if run.returncode:
        raise SystemExit(run.returncode)
print('MATRIX_COMPLETE', flush=True)
