//! Symbolic links on a real autumn-fuse mount, in the shape of a HuggingFace
//! cache (`snapshots/<rev>/<file>` → `../../blobs/<hash>`): create, readlink,
//! read through, `ls -l` types, `cp -a` of the whole tree, and removal.
//! Spawns the real `target/debug/autumn-fuse` (build it first) against an
//! in-process cluster; needs `/dev/fuse` and `fusermount3`.

mod support;

use std::os::unix::fs::{symlink, FileTypeExt};
use std::path::Path;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;

use support::*;

fn mounted(dir: &Path) -> bool {
    std::fs::read_to_string("/proc/mounts")
        .expect("/proc/mounts")
        .lines()
        .any(|l| l.split_whitespace().nth(1) == Some(dir.to_str().unwrap()))
}

/// Kills the daemon and unmounts on every exit path.
struct Daemon {
    child: std::process::Child,
    dir: std::path::PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if mounted(&self.dir) {
            let _unmounted = std::process::Command::new("fusermount3").args(["-u", "-z"]).arg(&self.dir).status();
        }
        if self.child.try_wait().ok().flatten().is_none() {
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }
}

fn mount_fs() -> (Daemon, std::net::SocketAddr, tempfile::TempDir, (tempfile::TempDir, tempfile::TempDir)) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs = (tempfile::tempdir().expect("n1"), tempfile::tempdir().expect("n2"));
    let (n1, n2) = (pick_addr(), pick_addr());
    start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
    start_extent_node(n2, dirs.1.path().to_path_buf(), 2);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, 265).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 26501, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        start_partition_server(265, mgr_addr, pick_addr());
        compio::time::sleep(Duration::from_millis(1500)).await;
        create_namespaces(mgr_addr, &["fs"]).await;
    });

    let mount_dir = tempfile::tempdir().expect("mountpoint");
    let log = std::fs::File::create(mount_dir.path().with_extension("fuse.log")).expect("log file");
    let log_err = log.try_clone().expect("log file");
    let child = std::process::Command::new(support_binary_path("autumn-fuse"))
        .arg("--mountpoint")
        .arg(mount_dir.path())
        .arg("--manager")
        .arg(mgr_addr.to_string())
        .stdout(log)
        .stderr(log_err)
        .spawn()
        .expect("spawn autumn-fuse");
    let mut daemon = Daemon { child, dir: mount_dir.path().to_path_buf() };
    let deadline = Instant::now() + Duration::from_secs(90);
    while !mounted(&daemon.dir) {
        if let Some(status) = daemon.child.try_wait().expect("try_wait") {
            panic!("autumn-fuse exited before mounting: {status}");
        }
        assert!(Instant::now() < deadline, "autumn-fuse never mounted");
        std::thread::sleep(Duration::from_millis(200));
    }
    (daemon, mgr_addr, mount_dir, dirs)
}

/// One plain HTTP/1.1 request; the status line's code and the body.
fn http(addr: std::net::SocketAddr, method: &str, path: &str) -> (u16, Vec<u8>) {
    use std::io::{Read, Write};
    let mut c = std::net::TcpStream::connect(addr).unwrap();
    write!(c, "{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
    let mut raw = Vec::new();
    c.read_to_end(&mut raw).unwrap();
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("no header end") + 4;
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let code = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut body = raw[split..].to_vec();
    if head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
        body = dechunk(&body);
    }
    (code, body)
}

fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let eol = b.windows(2).position(|w| w == b"\r\n").unwrap();
        let n = usize::from_str_radix(std::str::from_utf8(&b[..eol]).unwrap().trim(), 16).unwrap();
        if n == 0 {
            return out;
        }
        out.extend_from_slice(&b[eol + 2..eol + 2 + n]);
        b = &b[eol + 2 + n + 2..];
    }
}

/// The real `autumn-s3` on a free port, killed on drop.
struct Gateway(std::process::Child, std::net::SocketAddr);

impl Drop for Gateway {
    fn drop(&mut self) {
        let _killed = self.0.kill();
        let _reaped = self.0.wait();
    }
}

fn gateway(mgr: std::net::SocketAddr) -> Gateway {
    let addr = pick_addr();
    let child = std::process::Command::new(support_binary_path("autumn-s3"))
        .args(["--manager", &mgr.to_string(), "--listen", "127.0.0.1", "--port", &addr.port().to_string(), "--workers", "1"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn autumn-s3");
    let deadline = Instant::now() + Duration::from_secs(60);
    while std::net::TcpStream::connect(addr).is_err() {
        assert!(Instant::now() < deadline, "autumn-s3 never listened");
        std::thread::sleep(Duration::from_millis(200));
    }
    Gateway(child, addr)
}

/// The tree a `hf download` leaves: blobs named by hash, a snapshot of links.
fn hf_cache(root: &Path) -> Vec<(&'static str, Vec<u8>)> {
    let files = vec![
        ("config.json", b"{\"model_type\": \"test\"}".to_vec()),
        // Past the 4 KiB inline threshold, across more than one extent.
        ("model.safetensors", (0..9 * 1024 * 1024).map(|i| (i * 7 % 251) as u8).collect()),
    ];
    std::fs::create_dir_all(root.join("blobs")).unwrap();
    std::fs::create_dir_all(root.join("snapshots/r1")).unwrap();
    for (i, (name, body)) in files.iter().enumerate() {
        let hash = format!("h{i}");
        std::fs::write(root.join("blobs").join(&hash), body).unwrap();
        symlink(format!("../../blobs/{hash}"), root.join("snapshots/r1").join(name)).unwrap();
    }
    files
}

fn check_snapshot(root: &Path, files: &[(&str, Vec<u8>)]) {
    for (i, (name, body)) in files.iter().enumerate() {
        let link = root.join("snapshots/r1").join(name);
        assert!(std::fs::symlink_metadata(&link).unwrap().file_type().is_symlink(), "{link:?} is not a link");
        assert_eq!(std::fs::read_link(&link).unwrap(), Path::new(&format!("../../blobs/h{i}")));
        assert_eq!(&std::fs::read(&link).unwrap(), body, "{link:?} reads back different bytes");
        let meta = std::fs::metadata(&link).unwrap();
        assert!(meta.file_type().is_file() && !meta.file_type().is_fifo());
        assert_eq!(meta.len(), body.len() as u64);
    }
}

#[test]
fn a_huggingface_cache_works_on_the_mount() {
    let (daemon, mgr, _mount, _dirs) = mount_fs();
    let root = daemon.dir.join("models--t");
    let files = hf_cache(&root);
    check_snapshot(&root, &files);

    // readdir reports the link type without a stat.
    let kinds: Vec<_> = std::fs::read_dir(root.join("snapshots/r1"))
        .unwrap()
        .map(|e| e.unwrap().file_type().unwrap().is_symlink())
        .collect();
    assert_eq!(kinds, vec![true, true]);

    // `cp -a` of a whole cache, links kept as links.
    let copy = daemon.dir.join("copy");
    let status = std::process::Command::new("cp").arg("-a").arg(&root).arg(&copy).status().unwrap();
    assert!(status.success(), "cp -a failed: {status}");
    check_snapshot(&copy, &files);

    // Removing a link leaves its target; a dangling link reads ENOENT.
    std::fs::remove_file(copy.join("snapshots/r1/config.json")).unwrap();
    assert_eq!(std::fs::read(copy.join("blobs/h0")).unwrap(), files[0].1);
    std::fs::remove_file(copy.join("blobs/h1")).unwrap();
    let err = std::fs::read(copy.join("snapshots/r1/model.safetensors")).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);

    // A link to a directory is walked through; a looping link reads ELOOP.
    symlink("snapshots/r1", root.join("latest")).unwrap();
    symlink("loop", root.join("loop")).unwrap();
    const ELOOP: i32 = 40; // Linux
    assert_eq!(std::fs::read(root.join("loop")).unwrap_err().raw_os_error(), Some(ELOOP));
    assert_eq!(std::fs::read(root.join("latest/config.json")).unwrap(), files[0].1);

    // Over S3 a snapshot's files are the blobs they lead to: GET, HEAD and
    // the listing (with the blob's size), while the dangling link of the
    // copy is no object.
    let s3 = gateway(mgr);
    for (name, body) in &files {
        let (code, got) = http(s3.1, "GET", &format!("/models--t/snapshots/r1/{name}"));
        assert_eq!(code, 200, "GET {name}");
        assert_eq!(&got, body, "GET {name} returned different bytes");
        assert_eq!(http(s3.1, "HEAD", &format!("/models--t/snapshots/r1/{name}")).0, 200);
    }
    let (code, list) = http(s3.1, "GET", "/models--t?list-type=2&prefix=snapshots/r1/");
    assert_eq!(code, 200);
    let list = String::from_utf8(list).unwrap();
    for (name, body) in &files {
        assert!(list.contains(&format!("<Key>snapshots/r1/{name}</Key>")), "{name} not listed:\n{list}");
        assert!(list.contains(&format!("<Size>{}</Size>", body.len())), "{name}'s size not listed:\n{list}");
    }
    // A looping link is no object either: the listing that holds it and a
    // GET of it answer as for a missing key, not 500.
    let (code, list) = http(s3.1, "GET", "/models--t?list-type=2");
    assert_eq!(code, 200, "{}", String::from_utf8_lossy(&list));
    assert!(!String::from_utf8(list).unwrap().contains("<Key>loop</Key>"));
    assert_eq!(http(s3.1, "GET", "/models--t/loop").0, 404);
    assert_eq!(http(s3.1, "HEAD", "/models--t/loop").0, 404);
    let (_, list) = http(s3.1, "GET", "/copy?list-type=2&prefix=snapshots/");
    assert!(!String::from_utf8(list).unwrap().contains("<Key>"), "a removed or dangling link was listed");
    assert_eq!(http(s3.1, "GET", "/copy/snapshots/r1/model.safetensors").0, 404);
}
