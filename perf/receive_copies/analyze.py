"""Summarize receive-copy traces and untraced throughput.

analyze.py RESULTS_DIR > summary.json

Per trial and window: kernel TCP receive/send bytes, UCX Stream unpack bytes
and application memcpy bytes per role, each divided by the logical bytes the
window moved, plus the application copies grouped by the Rust frame nearest
the copy (the call site). Untraced windows contribute MiB/s and process CPU.
"""
import json
from pathlib import Path
import re
import subprocess
import sys

ROLES = {1: 'en1', 2: 'en2', 3: 'en3', 4: 'ps', 6: 'client'}
ROLE_MAPS = {1: 'en1', 2: 'en2', 3: 'en3', 4: 'ps', 6: 'client'}
UCX = re.compile(r'libuc[pts]\.so')


class Resolver:
    """Map a return address to UCX / other library / Rust source call site."""

    def __init__(self, maps):
        self.maps = maps
        self.cache = {}

    def module(self, role, address):
        """(path, ELF virtual address); the load base is the path's offset-0
        mapping, which also covers segments whose vaddr differs from offset."""
        maps = self.maps[ROLE_MAPS[role]]
        for start, end, _offset, path in maps:
            if start <= address < end:
                base = min(s for s, _e, o, p in maps if p == path and o == 0)
                return path, address - base
        return None, address

    def source(self, path, vaddr):
        key = (path, vaddr)
        if key not in self.cache:
            out = subprocess.run(['addr2line', '-e', path, '-f', '-i', '-C', hex(vaddr - 1)],
                                 capture_output=True, text=True).stdout.splitlines()
            frames = [(out[i], out[i + 1]) for i in range(0, len(out) - 1, 2)]
            self.cache[key] = frames
        return self.cache[key]

    def site(self, role, addresses):
        """First autumn-rs source frame among the addresses (innermost first);
        a copy in a frame.rs helper also names that helper's caller."""
        for address in addresses:
            path, vaddr = self.module(role, address)
            if path is None:
                continue
            if UCX.search(path):
                return 'ucx-unpack'
            if 'libc.so' in path or path.startswith('/usr/lib'):
                continue
            frames = self.source(path, vaddr)
            chain = [(fn, loc) for fn, loc in frames if '/crates/' in loc]
            if not chain:
                # Code generated without line info still names its function.
                chain = [(fn, '/crates/?') for fn, _ in frames if fn.startswith(('autumn', 'controlled'))]
            if not chain:
                continue
            label = lambda f: f"{re.sub(r'::h[0-9a-f]{16}$', '', f[0])[-70:]} @ {f[1].split('/crates/')[-1].split(' ')[0]}"
            if chain[0][1].split('/crates/')[-1].startswith('rpc/src/frame.rs') and len(chain) > 1:
                return label(chain[0]) + ' <- ' + label(chain[1])
            return label(chain[0])
        return 'unresolved'


def load_trace(path):
    maps = {}
    for line in path.read_text().splitlines():
        if not line.startswith('{'):
            continue
        record = json.loads(line)
        if record.get('type') == 'map':
            maps.update(record['data'])
    return maps


def split_key(key, fields):
    parts = key.split(',', fields - 1) if isinstance(key, str) else list(key)
    return [p.strip() for p in parts]


def main():
    results = Path(sys.argv[1])
    summary = {}
    for trace in sorted(results.glob('*-copies.jsonl')):
        label = trace.name[:-len('-copies.jsonl')]
        maps = load_trace(trace)
        windows = {}
        for path in results.glob(f'{label}-*.json'):
            suffix = path.stem[len(label) + 1:]
            if not suffix.isdigit():
                continue
            cell = json.loads(path.read_text())
            windows[int(suffix)] = dict(mode=cell['mode'], size=cell['size'], bytes=cell['bytes'],
                                        mib_per_sec=round(cell['mib_per_sec'], 1),
                                        traced=cell['trace'], p99_us=cell['p99_us'])
        out = {}
        def cell(window, role):
            w = out.setdefault(window, dict(windows.get(window, {}), roles={}))
            return w['roles'].setdefault(ROLES.get(role, str(role)), dict(
                tcp_recv=0, tcp_send=0, ucx_unpack=0, app_copy=0, memset=0, page_faults=0,
                small_memcpy_calls=0, small_memcpy_bytes=0, memset_calls=0, sites={}))
        for name, field in [('@tcp_recv_copy', 'tcp_recv'), ('@tcp_send', 'tcp_send'),
                            ('@memset_bytes', 'memset'), ('@user_page_faults', 'page_faults'),
                            ('@small_memcpy_calls', 'small_memcpy_calls'),
                            ('@small_memcpy_bytes', 'small_memcpy_bytes'),
                            ('@memset_calls', 'memset_calls')]:
            for key, value in maps.get(name, {}).items():
                window, role = map(int, split_key(key, 2))
                cell(window, role)[field] += value
        resolver = Resolver(json.loads((results / f'{label}-maps.json').read_text()))
        for key, value in maps.get('@memcpy_bytes', {}).items():
            window, role, ret, stack = split_key(key, 4)
            frames = [int(x, 16) for x in stack.split() if re.fullmatch(r'(0x)?[0-9a-f]+', x)]
            site = resolver.site(int(role), [int(ret)] + frames[1:])
            c = cell(int(window), int(role))
            if site == 'ucx-unpack':
                c['ucx_unpack'] += value
            else:
                c['app_copy'] += value
                c['sites'][site] = c['sites'].get(site, 0) + value
        for window, w in out.items():
            logical = w.get('bytes') or 0
            for role in w['roles'].values():
                if logical:
                    for field in ['tcp_recv', 'tcp_send', 'ucx_unpack', 'app_copy', 'memset']:
                        role[field + '_x'] = round(role[field] / logical, 3)
                role['sites'] = {k: round(v / logical, 3) if logical else v
                                 for k, v in sorted(role['sites'].items(), key=lambda kv: -kv[1])[:8]}
        summary[label] = dict(traced=out, untraced={k: v for k, v in windows.items() if not v['traced']})
    json.dump(summary, sys.stdout, indent=1, sort_keys=True)


if __name__ == '__main__':
    main()
