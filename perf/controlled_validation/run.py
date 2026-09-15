"""Run inside dongmao-autumn. Dedicated dirs/ports; fixed work, fresh trial data."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import time

ROOT = Path('/data08/autumn-controlled-validation')
DISKS = [Path(x) / 'autumn-controlled-validation' for x in ['/data03', '/data05', '/data08']]
MAGIC = 'autumn controlled validation temporary data'
ENV = dict(os.environ, UCX_TLS='rc_mlx5,ud_mlx5,tcp,self', UCX_NET_DEVICES='mlx5_1:1')
TOKEN = 'controlled-validation-only'


def run(command, **kwargs):
    return subprocess.run([str(x) for x in command], env=ENV, check=True,
                          capture_output=True, text=True, timeout=120, **kwargs)


def line(proc, timeout=180):
    if not select.select([proc.stdout], [], [], timeout)[0]:
        raise TimeoutError(f'benchmark {proc.pid} did not reach barrier')
    text = proc.stdout.readline()
    if not text:
        raise RuntimeError(f'benchmark exited {proc.poll()} before barrier')
    return text.strip()


def snapshot():
    tasks = {}
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            data = (path / 'stat').read_text()
            rest = data.rsplit(')', 1)[1].split()
            # Include every process, including kernel threads; starttime guards PID reuse.
            tasks[path.name] = [data.split('(', 1)[1].rsplit(')', 1)[0],
                                int(rest[11]), int(rest[12]), int(rest[19]), int(rest[6])]
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    return dict(monotonic_ns=time.monotonic_ns(), tasks=tasks,
                cpu=Path('/proc/stat').read_text(), softirqs=Path('/proc/softirqs').read_text(),
                diskstats=Path('/proc/diskstats').read_text())


def fast_snapshot(pids):
    start = time.monotonic_ns()
    tasks = {}
    for pid in pids:
        data = Path(f'/proc/{pid}/stat').read_text()
        rest = data.rsplit(')', 1)[1].split()
        tasks[str(pid)] = [int(rest[11]), int(rest[12]), int(rest[19])]
    cpu = Path('/proc/stat').read_text()
    return dict(start_ns=start, end_ns=time.monotonic_ns(), tasks=tasks, cpu=cpu)


class Counters:
    """Host perf runs disabled until all clients have finished warmup."""
    def __init__(self, directory, pids):
        self.control = directory / 'perf-control'
        self.ack = directory / 'perf-ack'
        os.mkfifo(self.control); os.mkfifo(self.ack)
        self.ctl = os.open(self.control, os.O_RDWR | os.O_NONBLOCK)
        self.ackfd = os.open(self.ack, os.O_RDWR | os.O_NONBLOCK)
        self.command_line = ['nsenter','-t','1','-m','--','perf','stat','-x,','--no-big-num',
                   '-D','-1','--control',f'fifo:{self.control},{self.ack}',
                   '-e','task-clock,cycles:u,cycles:k,instructions,context-switches',
                   '-p',','.join(map(str,pids)),'-o',str(directory / 'perf.csv')]
        self.directory = directory
        self.proc = None
        self.attempt = 0
        self.spawn()

    def spawn(self):
        self.proc = subprocess.Popen(self.command_line, stdout=subprocess.DEVNULL,
                                     stderr=open(self.directory / 'perf.stderr','w'))

    def command(self, value):
        while True:
            os.write(self.ctl, (value + '\n').encode())
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if select.select([self.ackfd], [], [], .1)[0]:
                    assert b'ack' in os.read(self.ackfd, 4096)
                    return
                if self.proc.poll() is not None:
                    break
            error = (self.directory / 'perf.stderr').read_text()
            # perf enumerates /proc/PID/task before opening counters. An idle
            # io-wq worker can exit between those operations. Retrying ESRCH
            # before go is safe: no measured request has been released yet.
            if value == 'enable' and 'No such process' in error and self.attempt < 3:
                self.proc.wait()
                self.attempt += 1
                (self.directory / f'attach-race-{self.attempt}.log').write_text(error)
                while select.select([self.ctl], [], [], 0)[0]:
                    os.read(self.ctl, 4096)
                self.spawn()
                continue
            raise RuntimeError('perf did not acknowledge ' + value + ': ' + error)

    def close(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT); self.proc.wait(timeout=15)
        os.close(self.ctl); os.close(self.ackfd)


class Cluster:
    def __init__(self, version, transport, partitions, repeat):
        self.version, self.transport, self.partitions = version, transport, partitions
        self.label = f'{transport}-p{partitions}-r{repeat}-{version}'
        self.work = ROOT / 'trials' / self.label
        self.work.mkdir(parents=True, exist_ok=False)
        self.bin = ROOT / 'bin' / version
        self.ip = '127.0.0.1' if transport == 'tcp' else 'fdbd:dc62:3:302::14'
        self.addr = lambda port: f'[{self.ip}]:{port}' if ':' in self.ip else f'{self.ip}:{port}'
        self.manager = self.addr(39001)
        self.procs = {}
        self.data = []
        self.trace = None

    def op(self, *args):
        return run([self.bin / 'autumn-op', '--manager', self.manager,
                    '--transport', self.transport, '--admin-token', TOKEN, *args]).stdout

    def launch(self, name, command, cpus):
        out = open(self.work / (name + '.log'), 'w')
        self.procs[name] = subprocess.Popen(
            ['numactl', '--membind=0', '--physcpubind=' + cpus, *map(str, command)],
            env=ENV, stdin=subprocess.DEVNULL, stdout=out, stderr=out, start_new_session=True)

    def start(self):
        # Bound live storage even if a future test matrix is enlarged.
        for disk in DISKS:
            disk.mkdir(exist_ok=True)
            if shutil.disk_usage(disk).free < 150 * 1024**3:
                raise RuntimeError(f'free-space floor: {disk}')
            data = disk / self.label
            data.mkdir(exist_ok=False)
            (data / '.controlled-test').write_text(MAGIC)
            self.data.append(data)
        self.launch('etcd', ['etcd', '--data-dir', self.work / 'etcd',
                    '--listen-client-urls', 'http://127.0.0.1:39379',
                    '--advertise-client-urls', 'http://127.0.0.1:39379',
                    '--listen-peer-urls', 'http://127.0.0.1:39380',
                    '--initial-advertise-peer-urls', 'http://127.0.0.1:39380',
                    '--initial-cluster', 'default=http://127.0.0.1:39380'], '32-33')
        for _ in range(100):
            try:
                run(['curl', '-fsS', 'http://127.0.0.1:39379/health'])
                break
            except subprocess.SubprocessError:
                time.sleep(.1)
        else:
            raise RuntimeError('etcd not ready')
        self.launch('manager', [self.bin / 'autumn-manager-server', '--port', 39001,
                    '--listen', self.ip, '--transport', self.transport,
                    '--etcd', '127.0.0.1:39379', '--admin-token', TOKEN], '34')
        for _ in range(100):
            try:
                self.op('info')
                break
            except subprocess.SubprocessError:
                time.sleep(.1)
        else:
            raise RuntimeError('manager not ready')
        for n, data in enumerate(self.data):
            self.op('format', data)
            port = 39101 + n
            cpus = f'{n*4}-{n*4+3}'
            self.launch('en' + str(n + 1), [self.bin / 'autumn-extent-node', '--port', port,
                        '--listen', self.ip, '--advertise', self.addr(port),
                        '--manager', self.manager, '--transport', self.transport,
                        '--data', data, '--cpuset', cpus, '--shard-stride', 10], cpus)
        for _ in range(100):
            if self.op('info').count('  node ') == 3:
                break
            time.sleep(.1)
        else:
            raise RuntimeError('replica registration missing')
        self.op('bootstrap', '--replication', '3+0')
        self.op('namespace-create', '--name', 'bench', '--admin-token', TOKEN)
        ps = [self.bin / 'autumn-ps', '--port', 39401, '--psid', 1,
              '--listen', self.ip, '--advertise', self.addr(39401),
              '--manager', self.manager, '--transport', self.transport, '--cpuset', '12-19']
        if self.version == 'zc':
            ps += ['--tcp-zerocopy-min-bytes', '65536']
        self.launch('ps', ps, '12-19')
        # Readiness is an actual RPC, not a cached manager address.
        for _ in range(100):
            try:
                run([self.bin / 'autumn-client', '--manager', self.manager,
                     '--transport', self.transport, '--namespace', 'bench/controlled',
                     'get', 'readiness'])
                break
            except subprocess.CalledProcessError as error:
                if error.returncode == 2 and 'key not found' in error.stderr:
                    break
                time.sleep(.2)
            except subprocess.SubprocessError:
                time.sleep(.2)
        else:
            raise RuntimeError('partition RPC not ready')
        if self.partitions > 1:
            self.op('presplit', '--namespace', 'bench', '--tenant', 'controlled',
                    '--count', self.partitions)
        info = self.op('info')
        if f'{self.partitions} partitions' not in info:
            raise RuntimeError('partition count mismatch: ' + info)
        (self.work / 'topology.txt').write_text(info)
        (self.work / 'pids.json').write_text(json.dumps({k:p.pid for k,p in self.procs.items()}))

    def benchmark(self, size, count, depth, mode, window, trace=False):
        bench = self.bin / 'controlled_path'
        err = open(self.work / f'{window}-{mode}-{size}-{depth}.stderr', 'w')
        env = dict(ENV, AUTUMN_PERF_WINDOW=str(window))
        p = subprocess.Popen(['numactl', '--membind=0', '--physcpubind=40-43', str(bench),
                              self.manager, self.transport, str(size), str(count),
                              str(depth), str(self.partitions), mode], env=env,
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err,
                             text=True, bufsize=1)
        counters = None
        try:
            assert line(p).startswith('READY ')
            if mode != 'load':
                directory = self.work / f'counters-{window}'
                directory.mkdir()
                counters = Counters(directory, [p.pid, *[x.pid for x in self.procs.values()]])
            before = snapshot()
            if counters: counters.command('enable')
            role_before = fast_snapshot([p.pid, *[x.pid for x in self.procs.values()]])
            p.stdin.write('go\n'); p.stdin.flush()
            assert line(p) == 'DONE'
            role_after = fast_snapshot([p.pid, *[x.pid for x in self.procs.values()]])
            if counters: counters.command('disable')
            after = snapshot()
            if counters: counters.close(); counters = None
            p.stdin.write('go\n'); p.stdin.flush()
            result = json.loads(line(p))
            assert p.wait(timeout=10) == 0
            result.update(version=self.version, transport=self.transport, trial=self.label,
                          window=window, trace=trace, before=before, after=after,
                          role_before=role_before, role_after=role_after,
                          roles={k:v.pid for k,v in self.procs.items()}, client_pid=p.pid)
            if mode != 'load':
                assert result['ops'] == count * self.partitions
                output = ROOT / 'results' / f'{self.label}-{window}.json'
                output.write_text(json.dumps(result))
                shutil.copy2(directory / 'perf.csv', ROOT / 'results' / f'{self.label}-{window}-perf.csv')
            return result
        finally:
            if counters: counters.close()
            if p.poll() is None:
                p.kill(); p.wait()

    def start_trace(self):
        roles = {'en1':1, 'en2':2, 'en3':3, 'ps':4, 'manager':5, 'etcd':7}
        prelude = 'BEGIN { ' + ' '.join(f'@role[{p.pid}]={roles[name]};' for name,p in self.procs.items())
        prelude += ' printf("TRACE_READY\\n"); }\n'
        script = self.work / 'trace.bt'
        script.write_text(prelude + (ROOT / 'source/perf/controlled_validation/trace.bt').read_text())
        out = open(self.work / 'trace.jsonl', 'w')
        err = open(self.work / 'trace.stderr', 'w')
        self.trace = subprocess.Popen(['bpftrace', '-f', 'json', str(script)], stdout=out, stderr=err,
                                      env=dict(ENV, BPFTRACE_MAX_MAP_KEYS='32768'))
        for _ in range(300):
            if 'TRACE_READY' in (self.work / 'trace.jsonl').read_text():
                return
            if self.trace.poll() is not None:
                raise RuntimeError((self.work / 'trace.stderr').read_text())
            time.sleep(.1)
        raise RuntimeError('trace collector readiness timeout')

    def stop_trace(self):
        if self.trace and self.trace.poll() is None:
            self.trace.send_signal(signal.SIGINT)
            self.trace.wait(timeout=30)
        if self.trace:
            shutil.copy2(self.work / 'trace.jsonl', ROOT / 'results' / (self.label + '-trace.jsonl'))
            shutil.copy2(self.work / 'trace.stderr', ROOT / 'results' / (self.label + '-trace.stderr'))

    def stop(self):
        self.stop_trace()
        for name in ['ps','en1','en2','en3','manager','etcd']:
            proc = self.procs.get(name)
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=150)

    def clean_data(self):
        assert all(p.poll() is not None for p in self.procs.values())
        for data in self.data:
            assert data.name == self.label and not data.is_symlink()
            assert (data / '.controlled-test').read_text() == MAGIC
            shutil.rmtree(data)
        shutil.rmtree(self.work / 'etcd')


def main():
    args = argparse.ArgumentParser()
    args.add_argument('--version', choices=['baseline','pure','zc'], required=True)
    args.add_argument('--transport', choices=['tcp','ucx'], required=True)
    args.add_argument('--partitions', type=int, choices=[1,4], required=True)
    args.add_argument('--repeat', type=int, required=True)
    args.add_argument('--trace', action='store_true')
    args.add_argument('--pilot', action='store_true')
    a = args.parse_args()
    c = Cluster(a.version, a.transport, a.partitions, a.repeat)
    success = False
    try:
        c.start()
        sizes = [4096,65536,1048576,8388608]
        for size in sizes:
            c.benchmark(size, 1, 8, 'load', 0)
        (c.work / 'topology-after-load.txt').write_text(c.op('info'))
        affinity = {}
        for name, proc in c.procs.items():
            affinity[name] = []
            for path in Path(f'/proc/{proc.pid}/task').iterdir():
                status = (path / 'status').read_text()
                affinity[name].append(dict(tid=int(path.name), name=(path / 'comm').read_text().strip(),
                                           cpus=next(x for x in status.splitlines() if x.startswith('Cpus_allowed_list:'))))
        (ROOT / 'results' / (c.label + '-affinity.json')).write_text(json.dumps(affinity))
        idle_before = snapshot(); time.sleep(1); idle_after = snapshot()
        (ROOT / 'results' / (c.label + '-idle.json')).write_text(json.dumps(dict(before=idle_before, after=idle_after)))
        if a.trace:
            c.start_trace()
            libc = ctypes.CDLL(None)
            def marker(phase):
                libc.prctl(3, ctypes.c_ulong(0x41555455), ctypes.c_ulong(phase),
                           ctypes.c_ulong(100), ctypes.c_ulong(0))
            marker(1); time.sleep(1); marker(0)
        window = 0
        for size in sizes:
            count = {4096:16384,65536:8192,1048576:2048,8388608:256}[size]
            if a.pilot:
                count = 64
            for depth in [1,8]:
                for mode in ['read','write']:
                    window += 1
                    operations = count
                    if mode == 'read' and not a.pilot:
                        operations = {4096:262144,65536:65536,1048576:8192,8388608:1024}[size]
                    r = c.benchmark(size,operations,depth,mode,window,a.trace)
                    print(c.label,size,depth,mode,round(r['mib_per_sec'],2),flush=True)
        success = True
    finally:
        c.stop()
        # Results precede reclamation. A failed run remains for diagnosis.
        if success:
            c.clean_data()
            (c.work / 'completed').write_text('results saved; services stopped; synthetic data removed')


if __name__ == '__main__':
    main()
