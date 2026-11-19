//! `autumn-convert extent-records` (the real binary) against a real etcd: a
//! manager refuses format-1 `extents/<id>` records; the tool refuses while a
//! manager is alive and refuses an extent kept only by `vp_table_refs` (in no
//! stream), writing nothing; one with `refs > 0` converts whatever its count; `--dry-run` writes nothing; a run converts every record
//! with its fields kept, a rerun finds nothing to do, and the manager then
//! leads on them.

mod support;

use autumn_manager::upgrade::extent_records::ExtentRecordV1;
use autumn_manager::AutumnManager;
use support::{start_etcd, support_binary_path};

fn v1_value(v: &ExtentRecordV1) -> Vec<u8> {
    let mut out = b"AUMG".to_vec();
    out.extend_from_slice(&[4, 1]);
    out.extend_from_slice(&autumn_rpc::manager_rpc::rkyv_encode(v));
    out
}

fn extent(id: u64, refs: u64, vp_table_refs: u64) -> ExtentRecordV1 {
    ExtentRecordV1 {
        extent_id: id,
        replicates: vec![1, 2, 3],
        parity: vec![],
        eversion: id + 10,
        refs,
        vp_table_refs,
        sealed_length: id * 1000,
        sealed: true,
        avali: 0b111,
        replicate_disks: vec![11, 12, 13],
        parity_disks: vec![],
        ec_converted: false,
    }
}

fn run_tool(etcd: &str, extra: &[&str]) -> (bool, String) {
    let out = std::process::Command::new(support_binary_path("autumn-convert"))
        .args(["extent-records", "--etcd", etcd])
        .args(extra)
        .output()
        .expect("run autumn-convert");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    (out.status.success(), text)
}

async fn values(aux: &autumn_etcd::EtcdClient) -> Vec<(Vec<u8>, Vec<u8>)> {
    let resp = aux.get_prefix("extents/").await.unwrap();
    resp.kvs.iter().map(|kv| (kv.key.clone(), kv.value.clone())).collect()
}

async fn clear_manager_keys(aux: &autumn_etcd::EtcdClient) {
    for prefix in ["autumn-rs/stream-manager/leader", "managerAlive/"] {
        for kv in aux.get_prefix(prefix).await.unwrap().kvs {
            aux.delete(&kv.key).await.unwrap();
        }
    }
}

#[test]
fn format_1_extent_records_convert_and_the_manager_leads_on_them() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, endpoint) = start_etcd().await;
        let aux = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
        for (id, vp) in [(7u64, 0u64), (8, 0), (9, 4)] {
            aux.put(format!("extents/{id}"), v1_value(&extent(id, 1, vp))).await.unwrap();
        }

        let res = AutumnManager::new_with_etcd(vec![endpoint.clone()], support::manager_identity()).await;
        let Err(e) = res else { panic!("a manager led on format-1 extent records") };
        let msg = format!("{e:#}");
        assert!(msg.contains("format version 1") && msg.contains("conversion has not run"), "{msg}");
        clear_manager_keys(&aux).await;
        let seeded = values(&aux).await;

        // A manager alive: refused, nothing written.
        aux.put("managerAlive/5", "x").await.unwrap();
        let (ok, text) = run_tool(&endpoint, &[]);
        assert!(!ok && text.contains("still running"), "{text}");
        aux.delete("managerAlive/5").await.unwrap();
        assert_eq!(values(&aux).await, seeded);

        // One extent kept only by vp_table_refs: refused, nothing written.
        aux.put("extents/10", v1_value(&extent(10, 0, 3))).await.unwrap();
        let (ok, text) = run_tool(&endpoint, &[]);
        assert!(!ok && text.contains("kept only by vp_table_refs = 3"), "{text}");
        aux.delete("extents/10").await.unwrap();
        assert_eq!(values(&aux).await, seeded);

        let (ok, text) = run_tool(&endpoint, &["--dry-run"]);
        assert!(ok && text.contains("3 to convert"), "{text}");
        assert_eq!(values(&aux).await, seeded);

        let (ok, text) = run_tool(&endpoint, &[]);
        assert!(ok && text.contains("converted 3"), "{text}");
        for (key, value) in values(&aux).await {
            assert_eq!(&value[..6], b"AUMG\x04\x02", "{}", String::from_utf8_lossy(&key));
            let info = support::try_decode_persisted_extent(&value).unwrap();
            let id = info.extent_id;
            assert_eq!(key, format!("extents/{id}").into_bytes());
            assert_eq!((info.eversion, info.refs, info.sealed_length), (id + 10, 1, id * 1000));
            assert_eq!(info.replicates, vec![1, 2, 3]);
            assert_eq!(info.replicate_disks, vec![11, 12, 13]);
            assert!(info.sealed && !info.ec_converted);
            assert_eq!(info.avali, 0b111);
        }
        let (ok, text) = run_tool(&endpoint, &[]);
        assert!(ok && text.contains("0 to convert, 3 already"), "{text}");

        AutumnManager::new_with_etcd(vec![endpoint], support::manager_identity())
            .await
            .expect("the manager leads on converted records");
    });
}
