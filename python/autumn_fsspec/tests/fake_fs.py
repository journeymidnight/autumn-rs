"""In-memory stand-in for the ``autumn.Fs`` sync surface used by the fsspec
facade (F-FS-UNIFY M3). Lets the whole filesystem be exercised offline with no
cluster, running the SAME facade code path as a live ``autumn.Fs``.

Mirrors the inode-layout semantics of the Rust core (crates/fuse): a real inode
tree (ROOT_INO = 1), ``resolve``/``lookup`` return None on a genuine miss,
``read`` clamps to EOF, ``write`` grows + zero-fills gaps, ``truncate`` shrinks
or grows. Errors raise ``RuntimeError`` like the PyO3 binding's ``PyRuntimeError``.
"""

from __future__ import annotations

ROOT_INO = 1
DT_DIR = 4
DT_REG = 8


class FakeFs:
    def __init__(self):
        # inode -> node dict. dirs carry `children` {name: ino}; files `data`.
        self.nodes = {
            ROOT_INO: {"type": "directory", "children": {}, "size": 0, "mtime": 0.0, "mode": 0o40755}
        }
        self._next = ROOT_INO + 1

    def _new(self, is_dir):
        i = self._next
        self._next += 1
        if is_dir:
            self.nodes[i] = {"type": "directory", "children": {}, "size": 0, "mtime": 0.0, "mode": 0o40755}
        else:
            self.nodes[i] = {"type": "file", "data": bytearray(), "size": 0, "mtime": 0.0, "mode": 0o100644}
        return i

    # ── path resolution + metadata ──────────────────────────────────────────

    def resolve(self, path):
        ino = ROOT_INO
        for comp in path.split("/"):
            if not comp or comp == ".":
                continue
            if comp == "..":
                raise RuntimeError("unsupported '..' component in path")
            node = self.nodes[ino]
            if node["type"] != "directory":
                return None
            child = node["children"].get(comp)
            if child is None:
                return None
            ino = child
        return ino

    def getattr(self, ino):
        n = self.nodes[ino]
        return {
            "ino": ino,
            "size": n["size"],
            "type": n["type"],
            "mode": n["mode"],
            "nlink": 1,
            "uid": 0,
            "gid": 0,
            "atime": n["mtime"],
            "mtime": n["mtime"],
            "ctime": n["mtime"],
        }

    def readdir(self, ino):
        n = self.nodes[ino]
        if n["type"] != "directory":
            raise RuntimeError("ENOTDIR")
        out = []
        for name, cino in n["children"].items():
            kind = DT_DIR if self.nodes[cino]["type"] == "directory" else DT_REG
            out.append((name, cino, kind))
        return out

    def lookup(self, parent, name):
        c = self.nodes[parent]["children"].get(name)
        if c is None:
            return None
        kind = DT_DIR if self.nodes[c]["type"] == "directory" else DT_REG
        return (c, kind)

    # ── mutations ───────────────────────────────────────────────────────────

    def mkdir(self, parent, name, mode=0o755):
        ch = self.nodes[parent]["children"]
        if name in ch:
            raise RuntimeError("EEXIST")
        i = self._new(True)
        ch[name] = i
        return i

    def create(self, parent, name, mode=0o644):
        ch = self.nodes[parent]["children"]
        if name in ch:
            raise RuntimeError("EEXIST")
        i = self._new(False)
        ch[name] = i
        return i

    def unlink(self, parent, name):
        ch = self.nodes[parent]["children"]
        c = ch.get(name)
        if c is None:
            raise RuntimeError("ENOENT")
        if self.nodes[c]["type"] == "directory":
            raise RuntimeError("EISDIR")
        del ch[name]
        del self.nodes[c]

    def rmdir(self, parent, name):
        ch = self.nodes[parent]["children"]
        c = ch.get(name)
        if c is None:
            raise RuntimeError("ENOENT")
        if self.nodes[c]["type"] != "directory":
            raise RuntimeError("ENOTDIR")
        if self.nodes[c]["children"]:
            raise RuntimeError("ENOTEMPTY")
        del ch[name]
        del self.nodes[c]

    def rename(self, old_parent, old_name, new_parent, new_name):
        sch = self.nodes[old_parent]["children"]
        c = sch.get(old_name)
        if c is None:
            raise RuntimeError("ENOENT")
        dch = self.nodes[new_parent]["children"]
        old = dch.get(new_name)
        if old is not None and old != c:
            if self.nodes[old]["type"] == "directory":
                if self.nodes[old]["children"]:
                    raise RuntimeError("ENOTEMPTY")
            del self.nodes[old]
        del sch[old_name]
        dch[new_name] = c

    # ── data ────────────────────────────────────────────────────────────────

    def read(self, ino, offset, size):
        n = self.nodes[ino]
        if n["type"] != "file":
            raise RuntimeError("EISDIR")
        sz = n["size"]
        if offset < 0:
            raise RuntimeError("negative offset")
        if offset >= sz:
            return b""
        return bytes(n["data"][offset : min(offset + size, sz)])

    def write(self, ino, offset, data):
        if offset < 0:
            raise RuntimeError("negative offset")
        n = self.nodes[ino]
        if n["type"] != "file":
            raise RuntimeError("EISDIR")
        buf = n["data"]
        if offset > len(buf):
            buf.extend(b"\x00" * (offset - len(buf)))
        buf[offset : offset + len(data)] = data
        n["size"] = max(n["size"], offset + len(data))
        return len(data)

    def flush(self, ino):
        pass

    def truncate(self, ino, size):
        n = self.nodes[ino]
        if n["type"] != "file":
            raise RuntimeError("EISDIR")
        buf = n["data"]
        if size < len(buf):
            del buf[size:]
        elif size > len(buf):
            buf.extend(b"\x00" * (size - len(buf)))
        n["size"] = size

    # ── leases (M2 thin wrappers; no-ops offline) ────────────────────────────

    def acquire(self, ino, mode="w"):
        return 0

    def heartbeat(self, ino):
        return True

    def release(self, ino):
        pass

    def forget(self, ino):
        pass

    def close(self):
        pass
