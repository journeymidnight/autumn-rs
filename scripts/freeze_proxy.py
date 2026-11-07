#!/usr/bin/env python3
"""freeze_proxy.py LISTEN_PORT UPSTREAM_HOST:PORT

A plain TCP relay whose flows can be frozen: on SIGUSR1 every flow open at
that moment stops forwarding in both directions while both of its sockets stay
open. The kernel keeps ACKing, nothing is retransmitted, no FIN or RST is ever
sent — the peer is dead and the transport says it is fine. Flows accepted
AFTER the signal relay normally, so a client that reconnects gets through.

That split is the point: it separates "this connection is dead" from "this
peer is dead", which a SIGSTOP of the whole server cannot do.
"""
import signal
import socket
import sys
import threading
import time

listen_port = int(sys.argv[1])
up_host, up_port = sys.argv[2].rsplit(":", 1)
up_port = int(up_port)

flows = []
lock = threading.Lock()


class Flow:
    def __init__(self):
        self.frozen = False


def pump(src, dst, flow):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            # Frozen: hold what arrived and never read again. Sleeping keeps
            # the thread (and both sockets) alive for the life of the process.
            while flow.frozen:
                time.sleep(3600)
            dst.sendall(data)
    except OSError:
        pass
    if not flow.frozen:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def freeze(*_):
    with lock:
        for f in flows:
            f.frozen = True
        n = len(flows)
    print(f"froze {n} flows", flush=True)


signal.signal(signal.SIGUSR1, freeze)

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", listen_port))
srv.listen(128)
print(f"relaying 127.0.0.1:{listen_port} -> {up_host}:{up_port}", flush=True)
while True:
    client, _ = srv.accept()
    try:
        upstream = socket.create_connection((up_host, up_port))
    except OSError:
        client.close()
        continue
    for s in (client, upstream):
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    flow = Flow()
    with lock:
        flows.append(flow)
    for a, b in ((client, upstream), (upstream, client)):
        threading.Thread(target=pump, args=(a, b, flow), daemon=True).start()
