import os,sys,json,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parent
version,transport=sys.argv[1:3]
data=root/('cluster-'+transport)
manager='127.0.0.1:39001' if transport=='tcp' else '[fdbd:dc62:3:302::14]:39001'
bench=next(p for p in (root/'baseline/target/release/deps').glob('core_path-*') if p.is_file() and os.access(p,os.X_OK))
pids={p.stem:int(p.read_text()) for p in data.glob('*.pid')}
(root/'pids.json').write_text(json.dumps(pids))
env=os.environ.copy();env.update(AUTUMN_PERF_PIDS=str(root/'pids.json'),UCX_TLS='rc_mlx5,ud_mlx5,tcp,self',UCX_NET_DEVICES='mlx5_1:1')
for repeat in range(int(os.environ.get('MATRIX_REPEATS','3'))):
 for size in [4096,65536,1048576,8388608]:
  for depth in [1,8]:
   for mode in ['read']:
    if os.statvfs(root).f_bavail*os.statvfs(root).f_frsize<150*1024**3:raise RuntimeError('bounded test free-space floor')
    label=f'static-{transport}-{version}-{repeat}-{size}-{depth}-{mode}'
    run=subprocess.run(['taskset','-c','40',str(bench),manager,transport,str(size),'2',str(depth),mode],env=env,capture_output=True,text=True,timeout=60)
    (root/'results'/(label+'.log')).write_text(run.stdout+run.stderr)
    if run.returncode:raise RuntimeError(label+': '+run.stderr[-1000:])
    d=json.loads(run.stdout.strip().splitlines()[-1]);d.update(version=version,repeat=repeat,client_version='baseline-0.18',topology='same-host single-NVMe RF3')
    d['cpu']={k:{kind:d['cpu_after'][k][kind]-v[kind] for kind in ['user','system']} for k,v in d['cpu_before'].items()}
    d['cpu_seconds_per_gib']=sum(sum(v.values()) for v in d['cpu'].values())/(d['ops']*size/1024**3)
    (root/'results'/(label+'.json')).write_text(json.dumps(d,indent=2))
    print(label,round(d['mib_per_sec'],2),round(d['cpu_seconds_per_gib'],3),flush=True)
