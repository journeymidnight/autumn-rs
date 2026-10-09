//! One-shot converter for the tenant removal. **Run once against a STOPPED
//! cluster, then delete this file** (and the `autumn-etcd` dependency it
//! brings into this crate).
//!
//! Three changes to the manager's etcd records:
//!
//! 1. `tenantAccount/<name>` moves to `principal/<name>`. The value is copied
//!    byte for byte: the record was only renamed (`TenantAccountRecord` →
//!    `PrincipalAccountRecord`, field `tenant` → `principal`), which does not
//!    move rkyv's layout — `persist/freeze.rs` still matches the bytes it
//!    recorded before the rename. The envelope (type 2, version 1) is checked,
//!    the body is not decoded.
//! 2. `namespace/<name>` goes from format 1 to 2: v2 dropped `owner_tenant`
//!    (an `Option<String>` after `prefix`), so each value is decoded with the
//!    vendored v1 shape and re-encoded as v2. A dropped owner is printed — it
//!    fed only the retired `protected_prefixes` list, which no PS read.
//! 3. `autoPolicy/config` in mode 1 (the removed observe mode) goes to mode 0
//!    (off): observing ran nothing, and a manager refuses to lead on a mode it
//!    does not have. The active policy stays selected; `auto-policy start`
//!    runs it. The record is the bare rkyv `MgrAutoPolicyConfig`, unchanged in
//!    layout.
//!
//! Namespaces go LAST: the built-in `fs`/`kvc`/`mem` rows exist on every
//! bootstrapped cluster, and a new manager refuses to lead while any of them is
//! still v1, so a run interrupted anywhere leaves a cluster that will not start
//! on the new binary rather than one that starts with principals missing.
//! Every step skips what is already done, so an interrupted run is re-run.
//! It refuses while any manager — leader or follower — is still up.
//!
//! ```bash
//! cargo run --bin migratev1_v2 -- --etcd http://127.0.0.1:2379 --dry-run
//! cargo run --bin migratev1_v2 -- --etcd http://127.0.0.1:2379
//! ```

use std::process::ExitCode;

use autumn_etcd::{Cmp, Op};
use rkyv::{Archive, Deserialize, Serialize};

/// Must match `crates/manager/src/persist/mod.rs`. Duplicated rather than
/// widening the manager's `pub(crate)` persist boundary for a file that is
/// about to be deleted.
const PERSIST_MAGIC: [u8; 4] = *b"AUMG";
const HEADER_LEN: usize = 6;
const RECORD_TYPE_PRINCIPAL_ACCOUNT: u8 = 2;
const PRINCIPAL_ACCOUNT_VERSION: u8 = 1;
const RECORD_TYPE_NAMESPACE: u8 = 3;

/// Must match `crates/manager/src/lib.rs` and `manager_members.rs`. Every
/// running manager, follower included, holds a `managerAlive/<id>` key; the
/// leader also holds the leader key. Either present means a manager is still
/// up — a follower can win the election mid-run — which is the one situation
/// this tool must not be used in.
const LEADER_KEY: &str = "autumn-rs/stream-manager/leader";
const MANAGER_ALIVE_PREFIX: &str = "managerAlive/";

const OLD_ACCOUNT_PREFIX: &str = "tenantAccount/";
const AUTO_POLICY_CONFIG_KEY: &str = "autoPolicy/config";
/// The removed observe mode's byte; 0 is off.
const AUTO_POLICY_MODE_OBSERVE: u8 = 1;
const PRINCIPAL_PREFIX: &str = "principal/";
const NAMESPACE_PREFIX: &str = "namespace/";

/// `NamespaceRecord` at format 1. rkyv's layout depends on the field list,
/// not the type name, so this decodes v1 bytes exactly.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
struct NamespaceV1 {
    name: String,
    prefix: Vec<u8>,
    owner_tenant: Option<String>,
    presplit: Vec<Vec<u8>>,
    created_at: i64,
}

/// `NamespaceRecord` at format 2, vendored like v1 so the tool does not need
/// the manager's private type. `v2_bytes_match_the_managers_freeze` pins it to
/// the manager's recorded encoding.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
struct NamespaceV2 {
    name: String,
    prefix: Vec<u8>,
    presplit: Vec<Vec<u8>>,
    created_at: i64,
}

fn envelope(record_type: u8, version: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&PERSIST_MAGIC);
    out.push(record_type);
    out.push(version);
    out.extend_from_slice(body);
    out
}

/// `(record_type, version)` of an enveloped value.
fn header(value: &[u8]) -> Result<(u8, u8), String> {
    if value.len() < HEADER_LEN || value[0..4] != PERSIST_MAGIC {
        return Err("not an enveloped manager record".to_string());
    }
    Ok((value[4], value[5]))
}

enum NamespaceStep {
    AlreadyV2,
    Convert {
        value: Vec<u8>,
        dropped_owner: Option<String>,
    },
}

fn convert_namespace(value: &[u8]) -> Result<NamespaceStep, String> {
    match header(value)? {
        (RECORD_TYPE_NAMESPACE, 2) => Ok(NamespaceStep::AlreadyV2),
        (RECORD_TYPE_NAMESPACE, 1) => {
            let v1: NamespaceV1 = autumn_rpc::manager_rpc::rkyv_decode(&value[HEADER_LEN..])
                .map_err(|e| format!("v1 body does not decode: {e}"))?;
            let v2 = NamespaceV2 {
                name: v1.name,
                prefix: v1.prefix,
                presplit: v1.presplit,
                created_at: v1.created_at,
            };
            let body = autumn_rpc::manager_rpc::rkyv_encode(&v2);
            Ok(NamespaceStep::Convert {
                value: envelope(RECORD_TYPE_NAMESPACE, 2, &body),
                dropped_owner: v1.owner_tenant,
            })
        }
        (t, v) => Err(format!("record type {t} version {v}; this tool converts namespace v1 only")),
    }
}

fn check_account(value: &[u8]) -> Result<(), String> {
    match header(value)? {
        (RECORD_TYPE_PRINCIPAL_ACCOUNT, PRINCIPAL_ACCOUNT_VERSION) => Ok(()),
        (t, v) => Err(format!(
            "record type {t} version {v}; expected the account record (type \
             {RECORD_TYPE_PRINCIPAL_ACCOUNT} version {PRINCIPAL_ACCOUNT_VERSION})"
        )),
    }
}

/// Moves every `tenantAccount/<name>` to `principal/<name>`, one txn per
/// account (create the new key only if absent, delete the old). Returns
/// `(moved, already)`.
async fn move_accounts(
    client: &autumn_etcd::EtcdClient,
    dry_run: bool,
) -> Result<(usize, usize), String> {
    let resp = client
        .get_prefix(OLD_ACCOUNT_PREFIX)
        .await
        .map_err(|e| format!("get_prefix {OLD_ACCOUNT_PREFIX}: {e}"))?;
    let (mut moved, mut already) = (0, 0);
    for kv in &resp.kvs {
        let old_key = String::from_utf8_lossy(&kv.key).into_owned();
        check_account(&kv.value).map_err(|e| format!("{old_key}: {e}"))?;
        let name = &kv.key[OLD_ACCOUNT_PREFIX.len()..];
        let new_key = [PRINCIPAL_PREFIX.as_bytes(), name].concat();
        let existing = client
            .get(&new_key)
            .await
            .map_err(|e| format!("get {}: {e}", String::from_utf8_lossy(&new_key)))?;
        let put_new = match existing.kvs.first() {
            None => true,
            // A rerun after the put landed but before this delete was seen.
            Some(n) if n.value == kv.value => false,
            Some(_) => {
                return Err(format!(
                    "{} already holds a DIFFERENT account than {old_key}; resolve \
                     by hand (keep one, delete the other) and re-run",
                    String::from_utf8_lossy(&new_key)
                ))
            }
        };
        if put_new {
            moved += 1;
        } else {
            already += 1;
        }
        if dry_run {
            continue;
        }
        let mut success = vec![Op::delete(&kv.key)];
        let mut compare = vec![Cmp::value(&kv.key, &kv.value)];
        if put_new {
            success.insert(0, Op::put(&new_key, &kv.value));
            compare.push(Cmp::create_revision(&new_key, 0));
        }
        let txn = client
            .txn(autumn_etcd::proto::TxnRequest {
                compare,
                success,
                failure: vec![],
            })
            .await
            .map_err(|e| format!("txn {old_key}: {e}"))?;
        if !txn.succeeded {
            return Err(format!("{old_key}: changed while converting; re-run"));
        }
    }
    Ok((moved, already))
}

/// Rewrites every v1 `namespace/<name>` as v2. Returns `(converted, already)`.
async fn convert_namespaces(
    client: &autumn_etcd::EtcdClient,
    dry_run: bool,
) -> Result<(usize, usize), String> {
    let resp = client
        .get_prefix(NAMESPACE_PREFIX)
        .await
        .map_err(|e| format!("get_prefix {NAMESPACE_PREFIX}: {e}"))?;
    let (mut converted, mut already) = (0, 0);
    for kv in &resp.kvs {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        match convert_namespace(&kv.value).map_err(|e| format!("{key}: {e}"))? {
            NamespaceStep::AlreadyV2 => already += 1,
            NamespaceStep::Convert {
                value,
                dropped_owner,
            } => {
                if let Some(owner) = dropped_owner {
                    println!("{key}: dropping owner_tenant {owner:?}");
                }
                converted += 1;
                if dry_run {
                    continue;
                }
                let txn = client
                    .txn(autumn_etcd::proto::TxnRequest {
                        compare: vec![Cmp::value(&kv.key, &kv.value)],
                        success: vec![Op::put(&kv.key, &value)],
                        failure: vec![],
                    })
                    .await
                    .map_err(|e| format!("txn {key}: {e}"))?;
                if !txn.succeeded {
                    return Err(format!("{key}: changed while converting; re-run"));
                }
            }
        }
    }
    Ok((converted, already))
}

/// The `autoPolicy/config` value with an observe mode turned off, or `None`
/// when it needs nothing.
fn convert_auto_policy(value: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let mut cfg: autumn_rpc::manager_rpc::MgrAutoPolicyConfig =
        autumn_rpc::manager_rpc::rkyv_decode(value)
            .map_err(|e| format!("decode {AUTO_POLICY_CONFIG_KEY}: {e}"))?;
    if cfg.mode != AUTO_POLICY_MODE_OBSERVE {
        return Ok(None);
    }
    cfg.mode = 0;
    Ok(Some(autumn_rpc::manager_rpc::rkyv_encode(&cfg).to_vec()))
}

/// Turns an observing auto-policy off. Returns whether it changed anything.
async fn convert_auto_policy_config(
    client: &autumn_etcd::EtcdClient,
    dry_run: bool,
) -> Result<bool, String> {
    let resp = client
        .get(AUTO_POLICY_CONFIG_KEY.as_bytes())
        .await
        .map_err(|e| format!("get {AUTO_POLICY_CONFIG_KEY}: {e}"))?;
    let Some(kv) = resp.kvs.first() else {
        return Ok(false);
    };
    let Some(value) = convert_auto_policy(&kv.value)? else {
        return Ok(false);
    };
    if dry_run {
        return Ok(true);
    }
    let txn = client
        .txn(autumn_etcd::proto::TxnRequest {
            compare: vec![Cmp::value(&kv.key, &kv.value)],
            success: vec![Op::put(&kv.key, &value)],
            failure: vec![],
        })
        .await
        .map_err(|e| format!("txn {AUTO_POLICY_CONFIG_KEY}: {e}"))?;
    if !txn.succeeded {
        return Err(format!("{AUTO_POLICY_CONFIG_KEY}: changed while converting; re-run"));
    }
    Ok(true)
}

const USAGE: &str = "usage: migratev1_v2 --etcd <http://host:2379[,…]> [--dry-run]";

#[compio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let mut endpoints = String::new();
    let mut dry_run = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--etcd" => {
                i += 1;
                endpoints = args.get(i).cloned().unwrap_or_default();
            }
            "--dry-run" => dry_run = true,
            other => {
                eprintln!("unknown argument: {other}\n{USAGE}");
                return ExitCode::FAILURE;
            }
        }
        i += 1;
    }
    if endpoints.is_empty() {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    }

    let list: Vec<String> = endpoints.split(',').map(|s| s.trim().to_string()).collect();
    let client = match autumn_etcd::EtcdClient::connect_many(&list).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connect etcd {endpoints}: {e}");
            return ExitCode::FAILURE;
        }
    };

    // An OLD manager can lead on unconverted data and would race these writes;
    // a NEW one refuses to lead until the namespaces are v2.
    let mut live: Vec<String> = Vec::new();
    for prefix in [LEADER_KEY, MANAGER_ALIVE_PREFIX] {
        match client.get_prefix(prefix).await {
            Ok(resp) => live.extend(resp.kvs.iter().map(|kv| String::from_utf8_lossy(&kv.key).into_owned())),
            Err(e) => {
                eprintln!("could not read {prefix} to check the cluster is stopped: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    if !live.is_empty() {
        eprintln!(
            "REFUSING: a manager is still running ({}). Stop every manager, wait \
             up to 10 s for its etcd lease to expire, then re-run.",
            live.join(", ")
        );
        return ExitCode::FAILURE;
    }

    if dry_run {
        println!("DRY RUN — nothing is written");
    }
    match move_accounts(&client, dry_run).await {
        Ok((moved, already)) => println!(
            "{OLD_ACCOUNT_PREFIX} -> {PRINCIPAL_PREFIX}  moved={moved} already={already}"
        ),
        Err(e) => {
            eprintln!("FAILED moving accounts: {e}\nnamespaces untouched; fix the cause and re-run");
            return ExitCode::FAILURE;
        }
    }
    match convert_auto_policy_config(&client, dry_run).await {
        Ok(true) => println!("{AUTO_POLICY_CONFIG_KEY}: observe mode -> off"),
        Ok(false) => println!("{AUTO_POLICY_CONFIG_KEY}: nothing to convert"),
        Err(e) => {
            eprintln!("FAILED converting the auto-policy config: {e}\nnamespaces untouched; fix the cause and re-run");
            return ExitCode::FAILURE;
        }
    }
    match convert_namespaces(&client, dry_run).await {
        Ok((converted, already)) => {
            println!("{NAMESPACE_PREFIX} v1 -> v2  converted={converted} already={already}");
            if converted == 0 && already == 0 {
                println!("no namespace records at all — check --etcd points at the right cluster");
            }
        }
        Err(e) => {
            eprintln!("FAILED converting namespaces: {e}\nfix the cause and re-run");
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Same fixture as `namespace_fixture()` in
    /// `crates/manager/src/persist/freeze.rs`.
    fn v2_fixture() -> NamespaceV2 {
        NamespaceV2 {
            name: "name-field".to_string(),
            prefix: b"prefix-field/".to_vec(),
            presplit: vec![b"cut-one".to_vec(), b"cut-two".to_vec()],
            created_at: 0x6162636465666768,
        }
    }

    /// The vendored v2 must write exactly what the manager reads:
    /// `NAMESPACE_FROZEN` from `persist/freeze.rs`, copied.
    #[test]
    fn v2_bytes_match_the_managers_freeze() {
        let body = autumn_rpc::manager_rpc::rkyv_encode(&v2_fixture());
        assert_eq!(
            hex(&envelope(RECORD_TYPE_NAMESPACE, 2, &body)),
            "41554d4703026e616d652d6669656c647072656669782d6669656c642f6375742d6f6e656375742d74776f000000efffffff07000000eeffffff070000008a000000c8ffffffcaffffff0d000000e0ffffff020000006867666564636261"
        );
    }

    /// The v1 bytes the manager recorded before the bump (freeze.rs at the
    /// parent commit) convert to the v2 freeze, owner reported.
    #[test]
    fn a_recorded_v1_value_converts_to_the_v2_freeze() {
        let v1 = "41554d4703016e616d652d6669656c647072656669782d6669656c642f6f776e65722d74656e616e742d6669656c646375742d6f6e656375742d74776f00f1ffffff07000000f0ffffff070000008a000000b8ffffffbaffffff0d0000000100000092000000bbffffffd4ffffff02000000000000006867666564636261";
        let raw: Vec<u8> = (0..v1.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&v1[i..i + 2], 16).unwrap())
            .collect();
        let NamespaceStep::Convert {
            value,
            dropped_owner,
        } = convert_namespace(&raw).unwrap()
        else {
            panic!("a v1 value must convert");
        };
        assert_eq!(dropped_owner.as_deref(), Some("owner-tenant-field"));
        let body = autumn_rpc::manager_rpc::rkyv_encode(&v2_fixture());
        assert_eq!(value, envelope(RECORD_TYPE_NAMESPACE, 2, &body));
        assert!(matches!(convert_namespace(&value).unwrap(), NamespaceStep::AlreadyV2));
    }

    #[test]
    fn foreign_records_are_refused() {
        let audit = envelope(1, 1, b"x");
        assert!(convert_namespace(&audit).is_err());
        assert!(check_account(&audit).is_err());
        assert!(check_account(&envelope(RECORD_TYPE_PRINCIPAL_ACCOUNT, 1, b"x")).is_ok());
        assert!(convert_namespace(b"bare").is_err());
    }

    #[test]
    fn an_observing_auto_policy_is_turned_off_and_nothing_else_changes() {
        use autumn_rpc::manager_rpc::{rkyv_decode, rkyv_encode, MgrAutoPolicyConfig, MgrAutoPolicyEntry};
        let cfg = |mode| MgrAutoPolicyConfig {
            ver: 1,
            mode,
            active: "my-policy".to_string(),
            policies: vec![MgrAutoPolicyEntry {
                name: "my-policy".to_string(),
                switches: vec![false, false, false, true],
                interval_sec: 30,
                ..Default::default()
            }],
        };
        let out = convert_auto_policy(&rkyv_encode(&cfg(1))).unwrap().expect("converted");
        let got: MgrAutoPolicyConfig = rkyv_decode(&out).unwrap();
        assert_eq!(got.mode, 0);
        assert_eq!(got.active, "my-policy");
        assert_eq!(got.policies.len(), 1);
        assert_eq!(got.policies[0].switches, vec![false, false, false, true]);
        assert!(convert_auto_policy(&rkyv_encode(&cfg(0))).unwrap().is_none());
        assert!(convert_auto_policy(&rkyv_encode(&cfg(2))).unwrap().is_none());
        assert!(convert_auto_policy(b"not rkyv").is_err());
    }

}
