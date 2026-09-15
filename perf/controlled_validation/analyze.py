"""Summarize fixed-work trials; no pilot data, no tracing runs in medians."""
import collections
import csv
import json
import statistics
import sys
from pathlib import Path


def cpu_rows(text):
    return {line.split()[0]: list(map(int, line.split()[1:])) for line in text.splitlines()
            if line.startswith('cpu')}


def derive(data, perf_path):
    before, after = data['role_before'], data['role_after']
    role_cpu = {}
    for role, pid in dict(data['roles'], client=data['client_pid']).items():
        a, b = before['tasks'][str(pid)], after['tasks'][str(pid)]
        assert a[2] == b[2], 'PID reused during trial'
        role_cpu[role] = dict(user=(b[0]-a[0])/100, system=(b[1]-a[1])/100)
    ca, cb = cpu_rows(before['cpu']), cpu_rows(after['cpu'])
    host = {key: [(b-a)/100 for a,b in zip(ca[key], cb[key])] for key in ca}
    # User/nice/system/irq/softirq/steal are busy. Idle and iowait are separate.
    busy = lambda values: sum(values[i] for i in [0,1,2,5,6,7])
    selected = list(range(20)) + [32,33,34,40,41,42,43]
    selected_busy = sum(busy(host['cpu'+str(cpu)]) for cpu in selected)
    processes = data['before']['tasks']
    background = []
    excluded = {str(pid) for pid in [*data['roles'].values(), data['client_pid']]}
    for pid,b in data['after']['tasks'].items():
        a = processes.get(pid)
        if not a or a[3] != b[3] or pid in excluded:
            continue
        elapsed = ((b[1]-a[1])+(b[2]-a[2]))/100
        if elapsed > .01:
            background.append(dict(pid=int(pid),name=b[0],cpu_seconds=elapsed,
                                   kernel_thread=bool(b[4] & 0x00200000)))
    counters = {}
    counter_running_pct = {}
    with perf_path.open() as file:
        for row in csv.reader(line for line in file if not line.startswith('#') and line.strip()):
            if len(row) >= 3:
                if row[0].startswith('<'):
                    raise ValueError(f'unsupported counter: {row}')
                counters[row[2]] = float(row[0])
                counter_running_pct[row[2]] = float(row[4])
    gib = data['bytes']/1024**3
    seconds = sum(sum(value.values()) for value in role_cpu.values())
    return dict(trial=data['trial'],transport=data['transport'],version=data['version'],
                partitions=data['partitions'],size=data['size'],depth=data['depth'],mode=data['mode'],
                trace=data['trace'],window=data['window'],seconds=data['seconds'],bytes=data['bytes'],
                mib_per_sec=data['mib_per_sec'],p50_us=data['p50_us'],p99_us=data['p99_us'],
                per_partition_ops=data['per_partition_ops'],role_cpu=role_cpu,
                process_cpu_s_per_gib=seconds/gib,perf=counters,
                counter_running_pct=counter_running_pct,
                kernel_cycles_per_gib=counters['cycles:k']/gib,
                host_busy_cpu_s=busy(host['cpu']),selected_cpu_busy_s=selected_busy,
                selected_cpu_busy_s_per_gib=selected_busy/gib,
                measurement_envelope_seconds=(after['start_ns']-before['start_ns'])/1e9,
                snapshot_overhead_seconds=(before['end_ns']-before['start_ns'])/1e9,
                background_cpu_top=sorted(background,key=lambda x:-x['cpu_seconds'])[:20])


def main():
    root = Path(sys.argv[1])
    records = []
    for path in sorted(root.glob('*-*-r*-*-*.json')):
        if any(x in path.name for x in ['-affinity','-idle','r900','r901','r902']):
            continue
        data = json.loads(path.read_text())
        if 'role_before' not in data:
            continue
        record = derive(data,path.with_name(path.stem+'-perf.csv'))
        logdir = root.parent/'trials'/data['trial']
        warnings = []
        for logfile in logdir.glob('*.log'):
            for line in logfile.read_text(errors='replace').splitlines():
                if any(word in line for word in ['Local protection error','append timeout','background flush commit error','write batch error']):
                    warnings.append(dict(file=logfile.name,line=line))
        record['trial_warnings'] = warnings
        records.append(record)
    groups = collections.defaultdict(list)
    for record in records:
        if not record['trace']:
            key = tuple(record[k] for k in ['transport','partitions','size','depth','mode','version'])
            groups[key].append(record)
    summary = []
    for key, group in sorted(groups.items()):
        item = dict(zip(['transport','partitions','size','depth','mode','version'],key))
        item['n'] = len(group)
        for field in ['mib_per_sec','p50_us','p99_us','process_cpu_s_per_gib','kernel_cycles_per_gib','selected_cpu_busy_s_per_gib']:
            values = [x[field] for x in group]
            item[field] = dict(median=statistics.median(values),min=min(values),max=max(values))
        summary.append(item)
    (root/'derived.json').write_text(json.dumps(records,indent=2))
    (root/'summary.json').write_text(json.dumps(summary,indent=2))
    print('records',len(records),'cells',len(summary))
    for row in summary:
        if row['depth']==8:
            print(row['transport'],row['partitions'],row['size'],row['mode'],row['version'],row['n'],
                  round(row['mib_per_sec']['median'],1),round(row['process_cpu_s_per_gib']['median'],3))


if __name__ == '__main__':
    main()
