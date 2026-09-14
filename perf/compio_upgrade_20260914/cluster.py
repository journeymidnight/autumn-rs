import os,sys,time,json,subprocess,socket,signal
from pathlib import Path
root=Path(__file__).resolve().parent
mode,version,transport=sys.argv[1:4]
data=root/('cluster-'+transport)
data.mkdir(exist_ok=True)
bins={'baseline':root/'baseline/target/release','pure':root/'pure-upgrade','zc':root/'upgrade/target/release'}[version]
ip='127.0.0.1' if transport=='tcp' else 'fdbd:dc62:3:302::14'
def addr(port):return f'[{ip}]:{port}' if ':' in ip else f'{ip}:{port}'
mgr=addr(39001)
env=os.environ.copy();env.update(UCX_TLS='rc_mlx5,ud_mlx5,tcp,self',UCX_NET_DEVICES='mlx5_1:1')
def run(args):return subprocess.run([str(a) for a in args],env=env,check=True,timeout=45,text=True,capture_output=True)
def op(*args):return run([bins/'autumn-op','--manager',mgr,'--transport',transport,*args])
def launch(name,args):
 log=open(data/(name+'.log'),'a')
 p=subprocess.Popen([str(a) for a in args],env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
 (data/(name+'.pid')).write_text(str(p.pid))
def waitport(port):
 for _ in range(150):
  try:
   with socket.create_connection((ip,port),timeout=.2):return
  except OSError:time.sleep(.2)
 raise RuntimeError(f'port {port} not ready')
if mode=='stop':
 for name in ['ps','en1','en2','en3','manager','etcd']:
  f=data/(name+'.pid')
  if not f.exists():continue
  pid=int(f.read_text())
  try:
   cmd=Path(f'/proc/{pid}/cmdline').read_bytes().decode().replace(chr(0),' ')
   if str(root) not in cmd and cmd:raise RuntimeError(f'PID {pid} does not belong to test')
   os.kill(pid,signal.SIGTERM)
  except FileNotFoundError:continue
  for _ in range(1200):
   try:
    state=Path(f'/proc/{pid}/stat').read_text().split(')')[1].split()[0]
    if state=='Z':break
   except FileNotFoundError:break
   time.sleep(.1)
  else:raise RuntimeError(f'{name} did not exit')
 print('stopped');sys.exit()
if mode!='start':raise ValueError(mode)
launch('etcd',['etcd','--data-dir',data/'etcd','--listen-client-urls','http://127.0.0.1:39379','--advertise-client-urls','http://127.0.0.1:39379','--listen-peer-urls','http://127.0.0.1:39380','--initial-advertise-peer-urls','http://127.0.0.1:39380','--initial-cluster','default=http://127.0.0.1:39380'])
for _ in range(100):
 try:
  with socket.create_connection(('127.0.0.1',39379),timeout=.2):break
 except OSError:time.sleep(.2)
launch('manager',[bins/'autumn-manager-server','--port','39001','--listen',ip,'--transport',transport,'--etcd','127.0.0.1:39379','--admin-token','compio-isolated-test'])
for _ in range(100):
 try:
  op('info');break
 except (subprocess.SubprocessError,OSError):time.sleep(.5)
else:raise RuntimeError('manager not ready')
for n in range(1,4):
 disk=data/f'disk{n}';disk.mkdir(exist_ok=True)
 if not (disk/'disk_uuid').exists():op('format',disk)
 port=39100+n
 launch('en'+str(n),[bins/'autumn-extent-node','--port',port,'--listen',ip,'--advertise',addr(port),'--manager',mgr,'--transport',transport,'--data',disk,'--cpuset',f'{(n-1)*4}-{n*4-1}','--shard-stride','10'])
for _ in range(100):
 info=op('info').stdout
 if info.count('  node ')>=3:break
 time.sleep(.3)
else:raise RuntimeError('EN registration failed: '+info)
if not (data/'bootstrapped').exists():
 op('bootstrap','--replication','3+0','--admin-token','compio-isolated-test')
 op('namespace-create','--name','bench','--admin-token','compio-isolated-test')
 (data/'bootstrapped').touch()
ps=[bins/'autumn-ps','--port','39401','--psid','1','--listen',ip,'--advertise',addr(39401),'--manager',mgr,'--transport',transport,'--cpuset','12-31']
if version=='zc':ps+=['--tcp-zerocopy-min-bytes','65536']
launch('ps',ps)
for _ in range(160):
 info=op('info').stdout
 if '39402' in info or '39401' in info:
  print(info);break
 time.sleep(.3)
waitport(39401) if transport=='tcp' else None
print(json.dumps({'manager':mgr,'version':version,'transport':transport,'data':str(data)}))
