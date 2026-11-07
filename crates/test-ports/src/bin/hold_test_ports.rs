//! Picks the given number of ports, prints them one per line, and holds them
//! until stdin closes (`tests/across_processes.rs`).

use std::io::Read;

fn main() {
    let n: usize = std::env::args().nth(1).and_then(|a| a.parse().ok()).expect("usage: hold_test_ports <count>");
    for _ in 0..n {
        println!("{}", autumn_test_ports::port());
    }
    // Hold until the test closes our stdin; a read error ends the hold too.
    std::io::stdin().read_to_end(&mut Vec::new()).ok();
}
