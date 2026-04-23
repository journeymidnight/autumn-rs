//! Transport selection — Phase 1 Task 5 + Phase 4 Task 20 fill in `probe_ucx()`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Tcp,
    Ucx,
}

pub fn decide() -> Decision {
    Decision::Tcp
}
