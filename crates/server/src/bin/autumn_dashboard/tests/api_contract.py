#!/usr/bin/env python3
"""Dashboard + real autumn-op + etcd/manager/EN/PS, with owned process cleanup.

Build first: cargo build -p autumn-server --bins
Run: bash crates/server/src/bin/autumn_dashboard/tests/api_contract.sh
Uses Cargo's target directory; AUTUMN_BIN_DIR can select another build.
"""
import json
import os
from pathlib import Path
import random
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[6]
BIN = Path(os.environ.get('AUTUMN_BIN_DIR') or (Path(json.loads(subprocess.check_output(
    ['cargo', 'metadata', '--format-version', '1', '--no-deps', '--locked', '--offline'], cwd=ROOT))['target_directory']) / 'debug'))
PAGE = Path(__file__).resolve().parents[1] / 'static/index.html'


def ports():
    for _ in range(100):
        base = random.randrange(22000, 50000)
        sockets = []
        try:
            for offset in (1, 101, 1101, 201, 301, 401, 402):
                s = socket.socket()
                sockets.append(s)
                s.bind(('127.0.0.1', base + offset))
            return base
        except OSError:
            pass
        finally:
            for s in sockets:
                s.close()
    raise RuntimeError('no free test port band')


def eventually(fn, timeout=30):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except (AssertionError, OSError, subprocess.SubprocessError, ValueError) as exc:
            last = exc
        time.sleep(.2)
    raise AssertionError(f'condition did not settle: {last}')


def run():
    base = ports()
    mgr, en, ps, dash, etcd = [base + n for n in (1, 101, 201, 301, 401)]
    address = f'127.0.0.1:{mgr}'
    children, logs = [], []
    with tempfile.TemporaryDirectory(prefix='autumn-dashboard-') as tmp:
        work = Path(tmp)
        def spawn(name, args):
            log = open(work / (name + '.log'), 'wb')
            logs.append(log)
            child = subprocess.Popen(args, stdout=log, stderr=log)
            children.append(child)
            return child
        def op(*args):
            result = subprocess.run([str(BIN / 'autumn-op'), '--manager', address,
                '--admin-token', 'dashboard-test-token', '--json', *map(str, args)],
                capture_output=True, text=True, timeout=15)
            if result.returncode:
                raise RuntimeError(result.stdout + result.stderr)
            return json.loads(result.stdout) if result.stdout.strip().startswith(('{', '[')) else result.stdout
        def http(route, body=None, expected=200):
            request = urllib.request.Request(f'http://127.0.0.1:{dash}{route}',
                data=None if body is None else json.dumps(body).encode(),
                headers={'Content-Type': 'application/json'})
            try:
                response = urllib.request.urlopen(request, timeout=40)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                raw = response.read()
                assert response.status == expected, (route, response.status, raw)
            return raw if route == '/' else json.loads(raw)
        def ready(port):
            with socket.create_connection(('127.0.0.1', port), timeout=.2):
                return True
        try:
            spawn('etcd', ['etcd', '--name', 'dashboard-test', '--data-dir', str(work/'etcd'),
                '--listen-client-urls', f'http://127.0.0.1:{etcd}', '--advertise-client-urls', f'http://127.0.0.1:{etcd}',
                '--listen-peer-urls', f'http://127.0.0.1:{etcd+1}', '--initial-advertise-peer-urls', f'http://127.0.0.1:{etcd+1}',
                '--initial-cluster', f'dashboard-test=http://127.0.0.1:{etcd+1}'])
            eventually(lambda: ready(etcd))
            manager = spawn('manager', [str(BIN/'autumn-manager-server'), '--port', str(mgr), '--listen', '127.0.0.1',
                '--admin-token', 'dashboard-test-token', '--etcd', f'127.0.0.1:{etcd}'])
            eventually(lambda: ready(mgr))
            # Read-only status proves that leader election/replay finished.
            def leader():
                try:
                    return op('auto-policy', 'status')
                except RuntimeError:
                    return False
            eventually(leader)
            for d in ('en/d0', 'en/d1'):
                (work/d).mkdir(parents=True)
            op('format', work/'en/d0', work/'en/d1')
            cpu = str(min(os.sched_getaffinity(0))) if hasattr(os, 'sched_getaffinity') else '0'
            spawn('en', [str(BIN/'autumn-extent-node'), '--data', f'{work}/en/d0,{work}/en/d1', '--port', str(en),
                '--manager', address, '--cpuset', cpu, '--advertise', f'127.0.0.1:{en}', '--listen', '127.0.0.1'])
            eventually(lambda: ready(en))
            def disks_ready():
                v = op('overview')
                return v.get('nodes') and len(v['nodes'][0].get('disks', [])) == 2 and all(d['reported'] for d in v['nodes'][0]['disks'])
            eventually(disks_ready)
            op('bootstrap', '--replication', '1+0')
            spawn('ps', [str(BIN/'autumn-ps'), '--psid', '1', '--port', str(ps), '--manager', address,
                '--listen', '127.0.0.1', '--advertise', f'127.0.0.1:{ps}'])
            eventually(lambda: ready(ps))
            spawn('dashboard', [str(BIN/'autumn-dashboard'), '--manager', address, '--autumn-op', str(BIN/'autumn-op'),
                '--port', str(dash), '--listen', '127.0.0.1', '--admin-token', 'dashboard-test-token'])
            eventually(lambda: ready(dash))
            assert http('/') == PAGE.read_bytes(), 'served page must be from this build'
            def overview_ready():
                v = http('/api/overview')
                return v if v.get('ps_servers') and v.get('partitions') and v['ps_servers'][0]['partition_count'] > 0 else None
            v = eventually(overview_ready)
            assert not v.get('errors'), v.get('errors')
            capacity = v['df']
            logical = capacity['logical_stored_sealed'] + capacity['logical_open_tail']
            assert capacity['logical_size'] == logical
            expected_amp = capacity['raw_used'] / logical if logical else 0
            assert abs(capacity['amplification'] - expected_amp) < 1e-12
            server = v['ps_servers'][0]
            assert {'ps_id','addr','last_heartbeat_secs_ago','partition_count','n','size','req_per_sec','write_bytes_per_sec','read_bytes_per_sec','total_extents','open_count','ready'} <= server.keys()
            assert server['last_heartbeat_secs_ago'] is not None and server['last_heartbeat_secs_ago'] < 60
            disks = v['nodes'][0]['disks']
            assert len(disks) == 2 and len({d['disk_id'] for d in disks}) == 2
            assert all(d['reported'] and d['online'] and not d['faulted'] and d['total'] > 0 and d['uuid'] for d in disks)
            pid = v['partitions'][0]['part_id']
            detail = http(f'/api/partition/{pid}')
            assert detail['has_overlap'] == 0 and isinstance(detail['extents'], list)
            assert http('/api/action', {'action':'compact','part_id':pid})['ok']
            def history_ready():
                ops = http('/api/ops')
                assert ops['history_error'] is None
                return ops if ops['history'] else None
            ops = eventually(history_ready)
            assert isinstance(ops['live'], list)
            assert {'op_id','kind','state','progress_done','progress_total','started_at','finished_at'} <= ops['history'][0].keys()
            # All switches off: safely exercise Armed without actuating maintenance.
            name = 'contract-\'"&policy'
            switches = {k:False for k in ('split','ec','compact','gc','merge','rebalance')}
            for route, body in [('/api/policies/activate', {}), ('/api/policies/activate', {'enabled':'false'}),
                ('/api/policies/activate', {'active':'--arm'}), ('/api/policies/delete', {'name':'--arm'}),
                ('/api/policies/upsert', {'name':'bad','switches':switches,'max_actions':2**32}),
                ('/api/policies/upsert', {'name':'bad','switches':{'gc':'yes'}}),
                ('/api/policies/upsert', {'name':'bad','switches':{'typo':True}})]:
                http(route, body, expected=400)
            config = {'name':name,'switches':switches,'interval':2,'cooldown':0,'max_actions':1}
            assert http('/api/policies/upsert', config)['ok']
            initial = http('/api/policies')
            assert initial['mode'] == 'off' and not initial['active']
            # Start selects and runs in one HTTP request, without a prior Observe.
            assert http('/api/policies/activate', {'active':name, 'enabled':True})['ok']
            started = http('/api/policies')
            assert (started['mode'], started['active']) == ('armed', name)
            assert http('/api/policies/activate', {'active':name})['ok']
            state = http('/api/policies')
            assert (state['mode'], state['active']) == ('dry_run', name)
            assert next(p for p in state['policies'] if p['name']==name)['switches'] == switches
            assert http('/api/policies/activate', {'enabled':True})['ok']
            assert http('/api/policies')['mode'] == 'armed'
            assert http('/api/policies/activate', {'enabled':False})['ok']
            assert http('/api/policies')['mode'] == 'off'
            assert http('/api/policies/delete', {'name':name})['ok']
            assert all(p['name'] != name for p in http('/api/policies')['policies'])
            failure = http('/api/policies/upsert', {**config,'name':'balanced'}, expected=502)
            assert failure['ok'] is False and 'built-in' in failure['output']
            # Unreachable manager must remain a gateway failure, including bare Arm.
            manager.terminate(); manager.wait(timeout=10)
            http('/api/policies', expected=502)
            http('/api/policies/activate', {'enabled':True}, expected=502)
            print('dashboard API contract OK: page, topology, disks, detail, durable ops, policy lifecycle, validation, upstream failures')
        except BaseException:
            for log in logs:
                log.flush()
            for log in work.glob('*.log'):
                print(f'--- {log.name} ---\n' + log.read_text(errors='replace')[-5000:])
            raise
        finally:
            for child in reversed(children):
                if child.poll() is None:
                    child.terminate()
            for child in children:
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill(); child.wait()
            for log in logs:
                log.close()


if __name__ == '__main__':
    run()
