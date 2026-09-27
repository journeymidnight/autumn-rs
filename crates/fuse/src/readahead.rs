//! The mount's readahead window, set through sysfs.
//!
//! FUSE INIT can only lower readahead below the kernel's bdi default (128 KiB),
//! so the one way to raise it is `/sys/class/bdi/<major:minor>/read_ahead_kb`
//! of the mount's anonymous device. A page fault on an mmap reads this window
//! around the faulting page, so it bounds what each faulting thread has in
//! flight — on a high-latency path the window, not the mount's bandwidth, is
//! most of what an mmap loader (safetensors) gets.
//!
//! WHEN matters: the kernel applies the INIT reply as `min(bdi, negotiated)`,
//! overwriting anything written to sysfs before INIT was answered — measured,
//! a write right after mounting read back 128. So the file is opened while
//! INIT is being handled (a failure fails the mount) and written on the first
//! open after it, which is before any read can happen.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

/// Open the `read_ahead_kb` file of the FUSE mount at `mountpoint` (an absolute
/// path, as `/proc/self/mountinfo` spells it) for writing. Reads mountinfo and
/// sysfs only: nothing here touches the mount itself.
pub fn open(mountpoint: &Path) -> io::Result<File> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
    let dev = device_of(&mountinfo, mountpoint).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} not in /proc/self/mountinfo", mountpoint.display()),
        )
    })?;
    std::fs::OpenOptions::new()
        .write(true)
        .open(format!("/sys/class/bdi/{dev}/read_ahead_kb"))
}

/// Set the window through a file from [`open`].
pub fn write(mut file: File, kb: u32) -> io::Result<()> {
    file.write_all(kb.to_string().as_bytes())
}

/// The `major:minor` of the LAST mount at `mountpoint` (a later mount stacks
/// over an earlier one at the same path).
fn device_of(mountinfo: &str, mountpoint: &Path) -> Option<String> {
    let want = mountpoint.to_str()?;
    mountinfo.lines().rev().find_map(|line| {
        let mut fields = line.split(' ');
        let dev = fields.nth(2)?;
        let mnt = fields.nth(1)?;
        (unescape(mnt) == want).then(|| dev.to_string())
    })
}

/// mountinfo writes space, tab, newline and backslash as `\ooo` octal.
fn unescape(field: &str) -> String {
    let b = field.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 3 < b.len() && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c)) {
            out.push((b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const INFO: &str = "\
25 1 259:1 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p1 rw
283 25 0:283 / /mnt/autumn-ra rw,nosuid,nodev,relatime shared:150 - fuse autumn-fuse rw,user_id=0
290 25 0:290 / /mnt/with\\040space rw,relatime shared:151 - fuse autumn-fuse rw
291 283 0:291 / /mnt/autumn-ra rw,relatime shared:152 - fuse autumn-fuse rw
";

    #[test]
    fn finds_the_device_of_a_mountpoint() {
        assert_eq!(device_of(INFO, Path::new("/")), Some("259:1".to_string()));
    }

    #[test]
    fn a_stacked_mount_wins_over_the_one_below() {
        assert_eq!(device_of(INFO, Path::new("/mnt/autumn-ra")), Some("0:291".to_string()));
    }

    #[test]
    fn an_escaped_mountpoint_matches_its_real_path() {
        assert_eq!(device_of(INFO, Path::new("/mnt/with space")), Some("0:290".to_string()));
    }

    #[test]
    fn an_unmounted_path_has_no_device() {
        assert_eq!(device_of(INFO, Path::new("/mnt/autumn")), None);
    }
}
