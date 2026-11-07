"""Recording reverse proxy: logs every S3 request LanceDB sends, forwards to an upstream."""
import http.client
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UP_HOST, UP_PORT = sys.argv[1], int(sys.argv[2])
LISTEN = int(sys.argv[3])
LOG = open(sys.argv[4], "a", buffering=1)
LOCK = threading.Lock()
KEEP = ("if-none-match", "if-match", "if-modified-since", "if-unmodified-since",
        "range", "content-length", "content-type", "x-amz-copy-source",
        "x-amz-copy-source-range", "x-amz-checksum-crc32", "x-amz-checksum-crc32c",
        "x-amz-sdk-checksum-algorithm", "x-amz-content-sha256", "content-md5",
        "x-amz-storage-class", "x-amz-checksum-algorithm", "expect", "transfer-encoding",
        "x-amz-decoded-content-length", "content-encoding", "x-amz-trailer")


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _go(self):
        body = b""
        if "chunked" in (self.headers.get("transfer-encoding") or "").lower():
            while True:
                size = int(self.rfile.readline().strip().split(b";")[0], 16)
                chunk = self.rfile.read(size)
                self.rfile.readline()
                if size == 0:
                    break
                body += chunk
        else:
            n = int(self.headers.get("content-length") or 0)
            body = self.rfile.read(n) if n else b""
        hdrs = {k: v for k, v in self.headers.items() if k.lower() != "transfer-encoding"}
        hdrs["Content-Length"] = str(len(body))
        c = http.client.HTTPConnection(UP_HOST, UP_PORT, timeout=120)
        c.request(self.command, self.path, body=body, headers=hdrs)
        r = c.getresponse()
        data = r.read()
        rec = {
            "m": self.command,
            "p": self.path,
            "h": {k.lower(): v for k, v in self.headers.items() if k.lower() in KEEP},
            "body_len": len(body),
            "st": r.status,
        }
        if self.command == "POST" and len(body) < 4096:
            rec["body"] = body.decode("utf-8", "replace")
        if r.status >= 400:
            rec["resp"] = data[:400].decode("utf-8", "replace")
        with LOCK:
            LOG.write(json.dumps(rec) + "\n")
        self.send_response(r.status)
        for k, v in r.getheaders():
            if k.lower() in ("transfer-encoding", "connection", "content-length"):
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _go


ThreadingHTTPServer(("127.0.0.1", LISTEN), H).serve_forever()
