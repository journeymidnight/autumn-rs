"""Receive-copy accounting on one host: RF3, one partition, depth 8.

Reuses the controlled-validation cluster launcher with its own root, then runs
fixed-work windows twice per trial: untraced (throughput and CPU) and traced
with copies.bt (byte accounting; uprobes on every memcpy make its throughput
meaningless). Run inside dongmao-autumn only; see README.md.

copytrace.py --version base|new --transport tcp|ucx --repeat N [--pilot | --long]
(--long: untraced only, fixed work of several seconds per window, trial label
repeat 100+N, for the throughput/CPU comparison)
"""
import argparse
import ctypes
import importlib.util
import json
from pathlib import Path
import shutil
import signal
import subprocess
import time

HERE = Path(__file__).resolve().parent
ROOT = Path('/data08/autumn-receive-copies')

spec = importlib.util.spec_from_file_location('controlled', HERE.parent / 'controlled_validation/run.py')
controlled = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controlled)
controlled.ROOT = ROOT
controlled.DISKS = [Path(x) / 'autumn-receive-copies' for x in ['/data03', '/data05', '/data08']]
controlled.MAGIC = 'autumn receive-copy accounting temporary data'

SIZES = [65536, 1048576, 8388608]
WRITES = {4096: 16384, 65536: 4096, 1048576: 512, 8388608: 128}
READS = {4096: 65536, 65536: 8192, 1048576: 1024, 8388608: 256}
MODES = ['write', 'read', 'direct', 'get']


def libc_offsets():
    """glibc memcpy/memmove/memset are IFUNCs; dlsym returns the selected
    implementation, whose load-relative address is the uprobe offset."""
    libc = ctypes.CDLL('libc.so.6')
    base = None
    for line in open('/proc/self/maps'):
        if 'libc.so.6' in line:
            start, _perms, offset = line.split()[:3]
            base = int(start.split('-')[0], 16) - int(offset, 16)
            break
    address = lambda name: ctypes.cast(getattr(libc, name), ctypes.c_void_p).value - base
    assert address('memcpy') == address('memmove'), 'memcpy and memmove resolve differently'
    return address('memmove'), address('memset')


def mappings(pid=None, binary=None):
    """File mappings as (start, end, file offset, path). ASLR is off, so a
    fresh process of `binary` loads exactly where `LD_TRACE_LOADED_OBJECTS`
    reports; its PIE base is the kernel's fixed no-randomize base."""
    if pid is not None:
        out = []
        for line in open(f'/proc/{pid}/maps'):
            fields = line.split()
            if len(fields) >= 6 and fields[5].startswith('/'):
                start, end = (int(x, 16) for x in fields[0].split('-'))
                out.append((start, end, int(fields[2], 16), fields[5]))
        return out
    listing = subprocess.run([binary], env=dict(controlled.ENV, LD_TRACE_LOADED_OBJECTS='1'),
                             capture_output=True, text=True, check=True).stdout
    libs = []
    for line in listing.splitlines():
        parts = line.split()
        if '=>' in parts and parts[-1].startswith('(0x'):
            libs.append((int(parts[-1][1:-1], 16), parts[2]))
    # Only load bases are known: each library extends to the next one's base.
    libs.sort()
    out = [(0x555555554000, 0x555555554000 + (1 << 32), 0, str(binary))]
    for i, (base, path) in enumerate(libs):
        end = libs[i + 1][0] if i + 1 < len(libs) else base + (1 << 28)
        out.append((base, end, 0, path))
    return out


class Trial(controlled.Cluster):
    def op(self, *args):
        # Node registration can precede the nodes being allocatable; bootstrap
        # is retried (harness readiness only, with each refusal logged).
        for attempt in range(40):
            try:
                return super().op(*args)
            except subprocess.CalledProcessError as error:
                if args[0] != 'bootstrap':
                    raise
                with open(self.work / 'bootstrap-retries.log', 'a') as log:
                    log.write(f'attempt {attempt}: {error.stderr}\n')
                time.sleep(.5)
        return super().op(*args)

    def start_copy_trace(self):
        memmove, memset = libc_offsets()
        roles = {'en1': 1, 'en2': 2, 'en3': 3, 'ps': 4}
        prelude = 'BEGIN { ' + ' '.join(f'@role[{self.procs[n].pid}]={r};' for n, r in roles.items())
        prelude += ' printf("TRACE_READY\\n"); }\n'
        body = (HERE / 'copies.bt').read_text()
        body = body.replace(':MEMMOVE', f':{hex(memmove)}').replace(':MEMSET', f':{hex(memset)}')
        script = self.work / 'copies.bt'
        script.write_text(prelude + body)
        maps = {n: mappings(pid=self.procs[n].pid) for n in roles}
        maps['client'] = mappings(binary=self.bin / 'controlled_path')
        (ROOT / 'results' / f'{self.label}-maps.json').write_text(json.dumps(maps))
        output = self.work / 'copies.jsonl'
        self.trace = subprocess.Popen(
            ['bpftrace', '--unsafe', '-f', 'json', str(script)],
            stdout=open(output, 'w'), stderr=open(self.work / 'copies.stderr', 'w'),
            env=dict(controlled.ENV, BPFTRACE_MAX_MAP_KEYS='65536'))
        for _ in range(300):
            if 'TRACE_READY' in output.read_text():
                return
            if self.trace.poll() is not None:
                raise RuntimeError((self.work / 'copies.stderr').read_text())
            time.sleep(.1)
        raise RuntimeError('copy trace readiness timeout')

    def stop_copy_trace(self):
        if self.trace and self.trace.poll() is None:
            self.trace.send_signal(signal.SIGINT)
            self.trace.wait(timeout=120)
        if self.trace:
            for name in ['copies.jsonl', 'copies.stderr']:
                shutil.copy2(self.work / name, ROOT / 'results' / f'{self.label}-{name}')
        self.trace = None

    def stop(self):
        self.stop_copy_trace()
        super().stop()


def main():
    args = argparse.ArgumentParser()
    args.add_argument('--version', required=True, help='directory under bin/')
    args.add_argument('--transport', choices=['tcp', 'ucx'], required=True)
    args.add_argument('--repeat', type=int, required=True)
    args.add_argument('--pilot', action='store_true')
    # Throughput/CPU comparison: untraced only, windows of several seconds.
    args.add_argument('--long', action='store_true')
    args.add_argument('--only', default='', help='comma list of SIZE:MODE windows to run')
    args.add_argument('--sizes', default='', help='comma list of value sizes (default 64K,1M,8M)')
    a = args.parse_args()
    # Every process this driver starts inherits a fixed address layout, which
    # is what lets copies.bt return addresses resolve after the processes exit.
    ADDR_NO_RANDOMIZE = 0x0040000
    libc = ctypes.CDLL(None)
    libc.personality(libc.personality(0xffffffff) | ADDR_NO_RANDOMIZE)
    (ROOT / 'results').mkdir(parents=True, exist_ok=True)
    sizes = [int(x) for x in a.sizes.split(',')] if a.sizes else SIZES
    trial = Trial(a.version, a.transport, 1, (100 if a.long else 0) + a.repeat)
    success = False
    try:
        trial.start()
        for size in sizes:
            trial.benchmark(size, 1, 8, 'load', 0)
        window = 0
        for traced in [False] if a.long else [False, True]:
            if traced:
                trial.start_copy_trace()
            for size in sizes:
                for mode in MODES:
                    if a.only and f'{size}:{mode}' not in a.only.split(','):
                        continue
                    window += 1
                    count = (WRITES if mode == 'write' else READS)[size]
                    if a.long:
                        count *= {4096: 2, 65536: 2, 1048576: 8, 8388608: 16}[size] if mode == 'write' else 16
                    if a.pilot:
                        count = 16
                    r = trial.benchmark(size, count, 8, mode, window, traced)
                    print(trial.label, 'traced' if traced else 'untraced', size, mode,
                          round(r['mib_per_sec'], 1), flush=True)
            if traced:
                trial.stop_copy_trace()
        success = True
    finally:
        trial.stop()
        if success:
            trial.clean_data()
            (trial.work / 'completed').write_text('results saved; services stopped; data removed')


if __name__ == '__main__':
    main()
