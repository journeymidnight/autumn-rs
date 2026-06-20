//! Chaos-gap round 2 (2026-06-20, coco P0) — `inode_leases/` replay must be
//! FAIL-LOUD on an undecodable persisted writer lease.
//!
//! A writer lease is the single-writer safety boundary for its inode. Pre-fix,
//! `replay_from_etcd` WARN+`continue`'d an undecodable `inode_leases/<ino>`
//! record, so a new leader came up with NO record of that writer (and no
//! `last_version` high-water) and could grant a SECOND writer for the same
//! inode while the old writer's cache / dirty pages were still live →
//! double-writer corruption. There is no TTL backstop for a SKIPPED malformed
//! record (unlike a legitimately-expired one).
//!
//! Fix: `inode_leases/` decode failure (key parse OR payload) returns
//! `replay_decode_err`, exactly like core metadata — the manager refuses to
//! lead rather than serve a state it cannot prove is single-writer.
//! `new_with_etcd` runs `replay_from_etcd().await?` before anything else, so a
//! corrupt writer lease makes construction itself fail.
//!
//! Requires the `etcd` binary on `$PATH`. Marked `#[ignore]` per repo convention.

mod support;

use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{rkyv_encode, MgrClientId, MgrInodeLeaseRecord};

use support::start_etcd;

#[test]
#[ignore] // requires embedded etcd (go runtime)
fn malformed_inode_lease_replay_fails_closed() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        // Seed an UNDECODABLE writer-lease record BEFORE the manager starts.
        // The bytes are not a valid rkyv `MgrInodeLeaseRecord` archive.
        let aux = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("aux etcd client");
        let ino: u64 = 4242;
        aux.put(
            format!("inode_leases/{ino}").as_bytes(),
            b"not-a-valid-rkyv-MgrInodeLeaseRecord".as_slice(),
        )
        .await
        .expect("seed malformed inode_leases record");

        // Invariant A: with an undecodable persisted writer lease present, the
        // manager must NOT come up as a (writable) leader — `new_with_etcd`
        // replays first and propagates the decode error.
        let res = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()]).await;
        assert!(
            res.is_err(),
            "FAIL-LOUD: a malformed inode_leases/<ino> writer lease must refuse \
             manager construction/leadership (got Ok — replay silently skipped it, \
             the double-writer hazard)"
        );

        // Invariant C: the error must be diagnosable (mentions the prefix/inode
        // and the decode failure), so an operator can find the bad key.
        let msg = format!("{:#}", res.err().unwrap());
        assert!(
            msg.contains("inode_leases"),
            "error must name the inode_leases prefix for diagnosability; got: {msg}"
        );

        // Sanity: a well-formed (empty) etcd — no malformed record — constructs
        // fine, proving the fail-loud is specific to the corrupt record, not a
        // blanket refusal. (Delete the bad key, then a fresh manager leads.)
        aux.delete(format!("inode_leases/{ino}").as_bytes())
            .await
            .expect("delete malformed record");
        let ok = AutumnManager::new_with_etcd(vec![etcd_endpoint]).await;
        assert!(
            ok.is_ok(),
            "after removing the malformed record the manager must construct/lead \
             normally (fail-loud is specific to the corrupt key, not blanket): {:?}",
            ok.err()
        );
    });
}

/// coco P1 (round 2): a record that decodes FINE but whose payload `ino` does
/// not match the key's inode is semantically corrupt — `install_persisted_writer`
/// keys on `rec.ino`, so it would install the writer under the wrong inode and
/// leave the KEY's inode writer-less (a second-writer hazard). Must fail-loud.
#[test]
#[ignore] // requires embedded etcd (go runtime)
fn inode_lease_key_payload_ino_mismatch_fails_closed() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        let aux = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("aux etcd client");

        // A perfectly-valid rkyv MgrInodeLeaseRecord, but stored under a key
        // whose inode (100) disagrees with the payload's ino (200).
        let rec = MgrInodeLeaseRecord {
            ino: 200,
            writer: MgrClientId {
                kind: 1,
                uuid: [7u8; 16],
                host: "h".to_string(),
            },
            version: 1,
            expires_at: 1_000_000_000,
        };
        aux.put(b"inode_leases/100".as_slice(), &rkyv_encode(&rec))
            .await
            .expect("seed ino-mismatch record");

        let res = AutumnManager::new_with_etcd(vec![etcd_endpoint]).await;
        assert!(
            res.is_err(),
            "FAIL-LOUD: an inode_leases/<key> whose payload ino != key id must \
             refuse leadership (install keys on rec.ino → wrong-inode writer + \
             key-inode left writer-less = second-writer hazard)"
        );
        let msg = format!("{:#}", res.err().unwrap());
        assert!(
            msg.contains("ino mismatch") && msg.contains("inode_leases/100"),
            "error must name the mismatch + key inode for diagnosability; got: {msg}"
        );
    });
}
