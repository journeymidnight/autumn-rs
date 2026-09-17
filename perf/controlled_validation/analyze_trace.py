"""Reduce BPF maps per exact marker window without double-counting CPU."""
import collections
import json
import sys
from pathlib import Path

ROLE = {'1':'en1','2':'en2','3':'en3','4':'ps','5':'manager','6':'client','7':'etcd'}


def main():
    root = Path(sys.argv[1])
    out = []
    for path in sorted(root.glob('*-r10-*-trace.jsonl')):
        trial = path.name.removesuffix('-trace.jsonl')
        maps = {}
        for line in path.read_text().splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if entry.get('type') == 'map':
                maps.update(entry['data'])
        windows = collections.defaultdict(dict)
        for key,value in maps.get('@elapsed_ns',{}).items():
            windows[int(key)]['seconds'] = value/1e9
        for name in ['kmalloc_bytes','kmalloc_calls','page_bytes','tcp_recv_copy_requested',
                     'tcp_send_copy_requested','tcp_send_requested','async_work','role_runtime_ns',
                     'cqe_positive_result','matched_cqe','enter_syscalls','zerocopy_notifications']:
            for key,value in maps.get('@'+name,{}).items():
                window,role = key.split(',')
                windows[int(window)].setdefault(name,{})[ROLE.get(role,role)] = value
        for name in ['sqe','cqe']:
            for key,value in maps.get('@'+name,{}).items():
                window,role,kind = key.split(',')
                windows[int(window)].setdefault(name,{}).setdefault(ROLE.get(role,role),{})[kind] = value
        for name in ['kernel_thread_runtime_ns','unrelated_selected_runtime_ns']:
            for key,value in maps.get('@'+name,{}).items():
                window,pid,comm = key.split(',',2)
                windows[int(window)].setdefault(name,[]).append(dict(pid=int(pid),comm=comm,ns=value))
        for name in ['softirq_ns','irq_ns']:
            for key,value in maps.get('@'+name,{}).items():
                window,rest = key.split(',',1)
                windows[int(window)].setdefault(name,{})[rest] = value
        for key,value in maps.get('@kernel_samples',{}).items():
            window,role,stack = key.split(',',2)
            windows[int(window)].setdefault('kernel_samples',[]).append(dict(role=ROLE.get(role,role),stack=stack,samples=value))
        for window,values in sorted(windows.items()):
            values.update(trial=trial,window=window)
            result = root/f'{trial}-{window}.json'
            if result.exists():
                run = json.loads(result.read_text())
                values.update({k:run[k] for k in ['size','depth','mode','version','transport','partitions','bytes']})
                values['gib'] = run['bytes']/1024**3
                values['marker_vs_bench_ratio'] = values['seconds']/run['seconds']
            else:
                values['mode'] = 'idle'
            for name in ['kernel_thread_runtime_ns','unrelated_selected_runtime_ns']:
                values[name] = sorted(values.get(name,[]),key=lambda x:-x['ns'])
            out.append(values)
    (root/'trace-summary.json').write_text(json.dumps(out,indent=2))
    print('trace windows',len(out))
    for row in out:
        if row.get('depth')==8 and row.get('size')==8388608 and row.get('mode')=='write':
            kt = sum(x['ns'] for x in row['kernel_thread_runtime_ns'])/1e9
            print(row['trial'],'kernel-thread CPU s',round(kt,4),
                  'recv copy bytes',sum(row.get('tcp_recv_copy_requested',{}).values()),
                  'PS SQEs',row.get('sqe',{}).get('ps',{}))


if __name__ == '__main__':
    main()
