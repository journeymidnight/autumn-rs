//! Processes picking ports at the same time never get the same one.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

#[test]
fn processes_picking_at_once_never_share_a_port() {
    const PROCS: usize = 16;
    const EACH: usize = 25;
    let mut children: Vec<_> = (0..PROCS)
        .map(|_| {
            Command::new(env!("CARGO_BIN_EXE_hold_test_ports"))
                .arg(EACH.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("spawn hold_test_ports")
        })
        .collect();
    let mut seen = std::collections::HashMap::new();
    for (i, c) in children.iter_mut().enumerate() {
        let out = BufReader::new(c.stdout.take().unwrap());
        for line in out.lines().take(EACH) {
            let base: u16 = line.unwrap().parse().unwrap();
            for p in base..base + autumn_test_ports::BLOCK {
                if let Some(j) = seen.insert(p, i) {
                    panic!("port {p} held by processes {j} and {i}");
                }
            }
        }
    }
    assert_eq!(seen.len(), PROCS * EACH * autumn_test_ports::BLOCK as usize);
    for mut c in children {
        drop(c.stdin.take());
        c.wait().unwrap();
    }
}
