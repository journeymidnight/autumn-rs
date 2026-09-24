//! `ListObjectsV2` over the directory tree, in S3 key order, resumable.
//!
//! S3 orders keys as raw bytes, and a directory `d` contributes keys that all
//! begin with `d/`. Directory entries are stored in NAME order, which differs
//! in one place: a name that extends `d` with a byte below `/` (`d.txt`,
//! `d-1`) sorts after `d` by name but before every `d/...` key. The walk
//! therefore holds a directory back until the next name sorts past `d/`.
//!
//! A page starts from its continuation token instead of from the top, and
//! skips whole subtrees that sort before it. Inside the token's directory the
//! scan starts at the token's own name; the only entries below that name which
//! can still sort after the token are directories named by a proper prefix of
//! it followed by a byte below `/` (`part` for token `part-00500`), and those
//! few are fetched by name. So a page costs about one page of entries plus one
//! scan and one batched lookup per level of the token's path.
//!
//! Names that are not UTF-8 cannot be S3 keys and are left out: a lossy
//! conversion would hand back a token that no longer compares with the name it
//! came from, and a paging client would see the same entry forever.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

use anyhow::Result;
use autumn_fuse::dir::DirChild;
use autumn_fuse::schema::{DT_DIR, DT_REG};

/// Where the walk reads directories from.
pub trait DirSource {
    /// Up to `limit` children of `dir` with names `>= from`, in name order, and
    /// where to resume the scan (`None` once exhausted).
    async fn children(&mut self, dir: u64, from: &[u8], limit: u32) -> Result<(Vec<DirChild>, Option<Vec<u8>>)>;
    /// The children of `dir` with exactly these names; absent names are skipped.
    async fn lookup(&mut self, dir: u64, names: &[Vec<u8>]) -> Result<Vec<DirChild>>;
}

/// One entry of a listing page.
#[derive(Debug, PartialEq, Eq)]
pub enum Item {
    Object { key: String, ino: u64 },
    Prefix(String),
}

impl Item {
    pub fn key(&self) -> &str {
        match self {
            Item::Object { key, .. } | Item::Prefix(key) => key,
        }
    }
}

/// One directory being read in S3 order.
struct Cursor {
    dir: u64,
    /// Key prefix of this directory's entries (`""` or `"a/b/"`).
    rel: Vec<u8>,
    /// Next name to scan from; `None` once the scan is exhausted.
    next_from: Option<Vec<u8>>,
    buf: VecDeque<DirChild>,
    /// Directories held back until the scan passes `name/`.
    pending: BinaryHeap<Reverse<(Vec<u8>, u64)>>,
    /// Only names starting with this are listed (the prefix's last component).
    filter: Vec<u8>,
    /// The part of the continuation token inside this directory.
    token: Option<Vec<u8>>,
}

/// Next entry of a cursor: sort key (`name`, or `name/` for a directory),
/// whether it is a directory, and its inode.
type Entry = (Vec<u8>, bool, u64);

impl Cursor {
    async fn open<S: DirSource>(
        src: &mut S,
        dir: u64,
        rel: Vec<u8>,
        filter: Vec<u8>,
        token: Option<Vec<u8>>,
    ) -> Result<Self> {
        let mut pending = BinaryHeap::new();
        let mut from = filter.clone();
        if let Some(t) = &token {
            let first = &t[..t.iter().position(|&b| b == b'/').unwrap_or(t.len())];
            // When the token continues below `first` (`part/f100`), every name
            // in `[first, first/)` — `part-00001`... — sorts before it, so the
            // scan starts at `first/` and `first` itself is fetched by name.
            // Starting at `first` scanned all of those siblings on every page.
            let descends = t.len() > first.len();
            let mut start = first.to_vec();
            if descends {
                start.push(b'/');
            }
            from = from.max(start);
            // Directories that are a proper prefix of the token's first name
            // followed by a byte below `/` sort after the token (`part/` >
            // `part-00500`) although their names sort before where the scan
            // starts; so does `first` when the token descends into it.
            let candidates: Vec<Vec<u8>> = (1..=first.len())
                .filter(|&j| if j == first.len() { descends } else { first[j] < b'/' })
                .map(|j| first[..j].to_vec())
                .filter(|n| n.starts_with(&filter))
                .collect();
            if !candidates.is_empty() {
                for c in src.lookup(dir, &candidates).await? {
                    if c.kind == DT_DIR && std::str::from_utf8(&c.name).is_ok() {
                        let mut sort_key = c.name;
                        sort_key.push(b'/');
                        pending.push(Reverse((sort_key, c.ino)));
                    }
                }
            }
        }
        Ok(Cursor {
            dir,
            rel,
            next_from: Some(from),
            buf: VecDeque::new(),
            pending,
            filter,
            token,
        })
    }

    async fn next<S: DirSource>(&mut self, src: &mut S, want: usize) -> Result<Option<Entry>> {
        loop {
            // Refill before consulting `pending`, so a held directory is never
            // emitted ahead of a name that sorts before it.
            while self.buf.is_empty() {
                let Some(from) = self.next_from.take() else {
                    break;
                };
                let limit = want.clamp(32, 1000) as u32;
                let (children, resume) = src.children(self.dir, &from, limit).await?;
                self.next_from = resume;
                for c in children {
                    // Names sharing the filter are contiguous in name order,
                    // so the first one without it ends the scan.
                    if !c.name.starts_with(&self.filter) {
                        self.next_from = None;
                        break;
                    }
                    if std::str::from_utf8(&c.name).is_ok() {
                        self.buf.push_back(c);
                    }
                }
            }
            let Some(c) = self.buf.front() else {
                return Ok(self.pending.pop().map(|Reverse((k, ino))| (k, true, ino)));
            };
            let is_dir = c.kind == DT_DIR;
            let mut sort_key = c.name.clone();
            if is_dir {
                sort_key.push(b'/');
            }
            if let Some(Reverse((held, _))) = self.pending.peek() {
                if *held < sort_key {
                    let Reverse((k, ino)) = self.pending.pop().expect("peeked");
                    return Ok(Some((k, true, ino)));
                }
            }
            let c = self.buf.pop_front().expect("fronted");
            if is_dir {
                // Directories fetched by name at `open` sort before the scan
                // start (names hold no `/`, so none equals `first/`), so the
                // scan never yields one of them a second time.
                self.pending.push(Reverse((sort_key, c.ino)));
            } else if c.kind == DT_REG {
                return Ok(Some((sort_key, false, c.ino)));
            }
        }
    }
}

/// List up to `max` entries whose keys start with `prefix` and sort after
/// `after`. `root` is the inode of the directory named by `prefix` up to its
/// last `/`. `recursive` lists every object below; otherwise immediate objects
/// and one `CommonPrefix` per subdirectory. Returns the page and, if more
/// entries follow, the continuation token (the last key on the page).
pub async fn list_page<S: DirSource>(
    src: &mut S,
    root: u64,
    prefix: &str,
    recursive: bool,
    after: Option<&str>,
    max: usize,
) -> Result<(Vec<Item>, Option<String>)> {
    if max == 0 {
        return Ok((Vec::new(), None));
    }
    let (dir_part, name_part) = match prefix.rfind('/') {
        Some(i) => (&prefix[..=i], &prefix[i + 1..]),
        None => ("", prefix),
    };
    // Only the token's part below `dir_part` matters; a token before the whole
    // prefix lists from its start, one after it lists nothing.
    let token = match after {
        None => None,
        Some(t) if t.as_bytes() < dir_part.as_bytes() => None,
        Some(t) if t.starts_with(dir_part) => Some(t.as_bytes()[dir_part.len()..].to_vec()),
        Some(_) => return Ok((Vec::new(), None)),
    };
    let mut stack = vec![
        Cursor::open(src, root, dir_part.as_bytes().to_vec(), name_part.as_bytes().to_vec(), token).await?,
    ];
    let mut out: Vec<Item> = Vec::new();
    while out.len() <= max {
        let Some(cur) = stack.last_mut() else {
            break;
        };
        let want = max + 1 - out.len();
        let Some((sort_key, is_dir, ino)) = cur.next(src, want).await? else {
            stack.pop();
            continue;
        };
        let token = cur.token.as_deref();
        let mut key = cur.rel.clone();
        key.extend_from_slice(&sort_key);
        if is_dir && recursive {
            let child_token = match token {
                Some(t) if t.starts_with(&sort_key) => {
                    Some(t[sort_key.len()..].to_vec()).filter(|r| !r.is_empty())
                }
                Some(t) if sort_key.as_slice() <= t => continue,
                _ => None,
            };
            let child = Cursor::open(src, ino, key, Vec::new(), child_token).await?;
            stack.push(child);
            continue;
        }
        if token.is_some_and(|t| sort_key.as_slice() <= t) {
            continue;
        }
        let key = String::from_utf8(key).expect("only UTF-8 names are walked");
        out.push(if is_dir {
            Item::Prefix(key)
        } else {
            Item::Object { key, ino }
        });
    }
    let next = if out.len() > max {
        out.truncate(max);
        out.last().map(|i| i.key().to_string())
    } else {
        None
    };
    Ok((out, next))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// An in-memory tree. Inode 1 is the root; a directory's inode is its
    /// index in `dirs`.
    struct Tree {
        dirs: Vec<BTreeMap<Vec<u8>, (u64, u8)>>,
        scans: usize,
        /// Entries returned by scans, i.e. values a real source would fetch.
        scanned: usize,
        /// Simulate every entry of the next scan being deleted between the key
        /// scan and the value fetch.
        vanish_next_scan: bool,
    }

    impl Tree {
        fn new(paths: &[&str]) -> Self {
            let mut t = Tree {
                dirs: vec![BTreeMap::new(), BTreeMap::new()],
                scans: 0,
                scanned: 0,
                vanish_next_scan: false,
            };
            for p in paths {
                let mut dir = 1u64;
                let parts: Vec<&str> = p.split('/').collect();
                for (i, part) in parts.iter().enumerate() {
                    let last = i + 1 == parts.len();
                    let next = t.dirs.len() as u64;
                    let (ino, _) = *t.dirs[dir as usize]
                        .entry(part.as_bytes().to_vec())
                        .or_insert((if last { 0 } else { next }, if last { DT_REG } else { DT_DIR }));
                    if !last {
                        if ino == next {
                            t.dirs.push(BTreeMap::new());
                        }
                        dir = ino;
                    }
                }
            }
            t
        }
    }

    impl DirSource for Tree {
        async fn children(&mut self, dir: u64, from: &[u8], limit: u32) -> Result<(Vec<DirChild>, Option<Vec<u8>>)> {
            self.scans += 1;
            let out: Vec<DirChild> = self.dirs[dir as usize]
                .range(from.to_vec()..)
                .take(limit as usize)
                .map(|(n, &(ino, kind))| DirChild { name: n.clone(), ino, kind })
                .collect();
            self.scanned += out.len();
            let resume = (out.len() == limit as usize)
                .then(|| autumn_fuse::dir::name_successor(&out.last().expect("full").name));
            if std::mem::take(&mut self.vanish_next_scan) {
                return Ok((Vec::new(), resume));
            }
            Ok((out, resume))
        }

        async fn lookup(&mut self, dir: u64, names: &[Vec<u8>]) -> Result<Vec<DirChild>> {
            Ok(names
                .iter()
                .filter_map(|n| {
                    self.dirs[dir as usize].get(n).map(|&(ino, kind)| DirChild { name: n.clone(), ino, kind })
                })
                .collect())
        }
    }

    fn run(tree: &mut Tree, prefix: &str, recursive: bool, after: Option<&str>, max: usize) -> (Vec<String>, Option<String>) {
        // The caller resolves the prefix's directory, as `list_objects` does.
        let mut root = 1u64;
        for part in prefix.split('/').rev().skip(1).collect::<Vec<_>>().into_iter().rev() {
            match tree.dirs[root as usize].get(part.as_bytes()) {
                Some(&(ino, DT_DIR)) => root = ino,
                _ => return (Vec::new(), None),
            }
        }
        let (items, next) = futures::executor::block_on(list_page(tree, root, prefix, recursive, after, max)).unwrap();
        (items.iter().map(|i| i.key().to_string()).collect(), next)
    }

    /// Page through everything and check it equals the brute-force answer.
    fn check(paths: &[&str], prefix: &str, recursive: bool, page: usize) {
        let mut expected: Vec<String> = if recursive {
            paths.iter().filter(|p| p.starts_with(prefix)).map(|p| p.to_string()).collect()
        } else {
            let mut v: Vec<String> = paths
                .iter()
                .filter(|p| p.starts_with(prefix))
                .map(|p| match p[prefix.len()..].find('/') {
                    Some(i) => p[..prefix.len() + i + 1].to_string(),
                    None => p.to_string(),
                })
                .collect();
            v.dedup();
            v
        };
        expected.sort();
        expected.dedup();
        let mut tree = Tree::new(paths);
        let (mut got, mut after) = (Vec::new(), None::<String>);
        loop {
            let (keys, next) = run(&mut tree, prefix, recursive, after.as_deref(), page);
            assert!(keys.len() <= page);
            got.extend(keys);
            match next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        assert_eq!(got, expected, "prefix={prefix:?} recursive={recursive} page={page}");
    }

    const PATHS: &[&str] = &[
        "a.txt", "a/b", "a/c/d", "a/c.x", "a0", "a-1/z", "ab/c", "b", "d.txt/x", "d/y",
        "data/0001.lance", "data/0002.lance", "data/0010.lance", "x!/q", "x/q", "x0",
    ];

    #[test]
    fn matches_bytewise_key_order_at_every_page_size() {
        for page in 1..=PATHS.len() + 1 {
            for prefix in ["", "a", "a/", "a/c", "d", "data/", "data/000", "x", "zz"] {
                check(PATHS, prefix, true, page);
                check(PATHS, prefix, false, page);
            }
        }
    }

    #[test]
    fn directories_sort_after_names_extending_them_below_slash() {
        let mut tree = Tree::new(PATHS);
        let (keys, _) = run(&mut tree, "", false, None, 100);
        // `a.txt` and `a-1/` sort before `a/`, `a0` after it.
        assert_eq!(
            keys,
            ["a-1/", "a.txt", "a/", "a0", "ab/", "b", "d.txt/", "d/", "data/", "x!/", "x/", "x0"]
        );
    }

    #[test]
    fn a_page_does_not_rescan_the_directories_before_its_token() {
        let paths: Vec<String> = (0..500).map(|i| format!("d{i:03}/f")).collect();
        let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
        let mut tree = Tree::new(&refs);
        let (_, next) = run(&mut tree, "", true, Some("d400/f"), 10);
        assert!(next.is_some());
        // One scan of the root plus the ten subdirectories read for the page
        // (and one to learn the page is full), not all five hundred.
        assert!(tree.scans <= 13, "scans = {}", tree.scans);
    }

    /// A page must not rescan the entries before its token when the names
    /// share a stem followed by a byte below `/` — the shape of `part-NNNNN`,
    /// `model-00001-of-00009` and every uuid-dashed name. Starting the scan at
    /// that stem made page k scan about k pages.
    #[test]
    fn a_page_scans_about_a_page_for_dashed_names() {
        let paths: Vec<String> = (0..5000).map(|i| format!("data/part-{i:05}.parquet")).collect();
        let mut refs: Vec<&str> = paths.iter().map(String::as_str).collect();
        // A directory whose name is a proper prefix of the token's name with a
        // byte below `/` after it: it sorts after every `part-...` key.
        refs.push("data/part/x");
        let mut tree = Tree::new(&refs);
        let (_, next) = run(&mut tree, "data/", true, Some("data/part-04000.parquet"), 100);
        assert!(next.is_some());
        assert!(tree.scanned <= 2 * 101 + 10, "scanned {} entries for one page", tree.scanned);
        check(&refs, "data/", true, 100);
        check(&refs, "data/", false, 100);
        check(&refs, "data/part", true, 7);
    }

    /// The same from the other side: a token inside `part/` must not rescan
    /// the `part-...` siblings that sort before `part/`.
    #[test]
    fn a_page_inside_a_directory_skips_its_dashed_siblings() {
        let mut paths: Vec<String> = (0..5000).map(|i| format!("data/part-{i:05}.parquet")).collect();
        paths.extend((0..300).map(|i| format!("data/part/f{i:03}")));
        let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
        let mut tree = Tree::new(&refs);
        let (keys, _) = run(&mut tree, "data/", true, Some("data/part/f100"), 10);
        assert_eq!(keys.first().map(String::as_str), Some("data/part/f101"));
        assert!(tree.scanned <= 2 * 11 + 10, "recursive: scanned {} entries", tree.scanned);
        tree.scanned = 0;
        let (keys, _) = run(&mut tree, "data/", false, Some("data/part/"), 10);
        assert!(keys.is_empty(), "{keys:?}");
        assert!(tree.scanned <= 32, "delimited: scanned {} entries", tree.scanned);
        check(&refs, "data/", true, 97);
        check(&refs, "data/", false, 97);
    }

    /// A scan whose every entry vanished before its values were read must not
    /// end the directory.
    #[test]
    fn a_fully_deleted_scan_does_not_end_the_directory() {
        let paths: Vec<String> = (0..100).map(|i| format!("d/f{i:03}")).collect();
        let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
        let mut tree = Tree::new(&refs);
        let (_, next) = run(&mut tree, "d/", true, None, 10);
        let after = next.expect("truncated");
        assert_eq!(after, "d/f009");
        // The next scan reads 32 names from the token, `f009`..=`f040`, and
        // all of them vanish; the listing must carry on from `f041`.
        tree.vanish_next_scan = true;
        let (keys, next) = run(&mut tree, "d/", true, Some(&after), 10);
        let want: Vec<String> = (41..51).map(|i| format!("d/f{i:03}")).collect();
        assert_eq!(keys, want);
        assert!(next.is_some());
    }

    #[test]
    fn zero_and_out_of_range_tokens() {
        let mut tree = Tree::new(PATHS);
        assert_eq!(run(&mut tree, "", true, None, 0), (vec![], None));
        assert_eq!(run(&mut tree, "a/", true, Some("zzz"), 10), (vec![], None));
        let (keys, _) = run(&mut tree, "a/", true, Some("A"), 10);
        assert_eq!(keys, ["a/b", "a/c.x", "a/c/d"]);
    }
}
