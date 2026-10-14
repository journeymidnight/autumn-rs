//! Runtime parameter overrides (`autumn-op config set/unset/list`).
//!
//! The leader keeps one row per overridden key in etcd (`runtimeConfig/<key>`,
//! key = `<role>.<flag>`) and serves the whole set on `MSG_GET_RUNTIME_CONFIG`;
//! every PS and EN fetches it next to its authz config and applies the entries
//! of its role. The manager applies the `manager.` entries to itself when it
//! takes the leadership and on every change.

use std::collections::BTreeMap;

use autumn_params::en::EnParams;
use autumn_params::manager::ManagerParams;
use autumn_params::ps::PsParams;
use autumn_params::{apply_overrides, runtime_spec, Params};
use autumn_rpc::manager_rpc::*;
use autumn_rpc::{HandlerResult, StatusCode};
use bytes::Bytes;

use crate::persist::records::RuntimeOverrideRecord;
use crate::AutumnManager;

pub(crate) const PREFIX: &str = "runtimeConfig/";

pub(crate) type Overrides = BTreeMap<String, RuntimeOverrideRecord>;

/// The role a key belongs to, by its prefix.
fn role_of(key: &str) -> &str {
    key.split_once('.').map_or("", |(role, _)| role)
}

/// Validate `value` for `key` against the parameter declarations.
pub(crate) fn check_entry(key: &str, value: &str) -> Result<(), String> {
    let spec = match role_of(key) {
        ManagerParams::ROLE => runtime_spec::<ManagerParams>(key),
        PsParams::ROLE => runtime_spec::<PsParams>(key),
        EnParams::ROLE => runtime_spec::<EnParams>(key),
        _ => {
            return Err(format!(
                "'{key}': a key is <role>.<flag> with role manager, ps or en"
            ))
        }
    };
    let spec = spec.ok_or_else(|| {
        format!("'{key}' is not a runtime parameter (`autumn-op config keys` lists them)")
    })?;
    (spec.check)(value).map_err(|e| format!("{key} = {value}: {e}"))
}

/// The entries this binary can apply. A row left by an older binary for a
/// parameter that no longer exists stays listed (the operator unsets it) but
/// is left out here, so it neither blocks a later `set` nor reaches a trial.
fn applicable(set: &Overrides) -> Vec<(String, String)> {
    set.values()
        .filter(|r| check_entry(&r.key, &r.value).is_ok())
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect()
}

/// What a process of `P` started at the default flags refuses in `set`. The
/// process re-checks against its own flags and refuses the entries that fail
/// there.
fn trial<P: Params + Default>(set: &[(String, String)]) -> Vec<String> {
    apply_overrides(&P::default(), set)
}

/// The refusals `new` adds over `old`: a row stored earlier that this build
/// refuses is the operator's to unset, not a reason to refuse other keys.
fn added(new: Vec<String>, old: &[String]) -> Result<(), String> {
    let added: Vec<String> = new.into_iter().filter(|e| !old.contains(e)).collect();
    if added.is_empty() {
        Ok(())
    } else {
        Err(added.join("; "))
    }
}

fn entry(r: &RuntimeOverrideRecord) -> RuntimeConfigEntry {
    RuntimeConfigEntry {
        key: r.key.clone(),
        value: r.value.clone(),
        set_by: r.set_by.clone(),
        set_at: r.set_at,
    }
}

impl AutumnManager {
    /// Install the `manager.` overrides on this process.
    pub(crate) fn apply_manager_overrides(&self) -> Vec<String> {
        let set = applicable(&self.runtime_overrides.borrow());
        apply_overrides(&*self.params, &set)
    }

    /// Replay `runtimeConfig/` (called from `replay_from_etcd`).
    pub(crate) fn install_replayed_overrides(&self, set: Overrides) {
        for r in set.values() {
            if let Err(e) = check_entry(&r.key, &r.value) {
                tracing::warn!(key = %r.key, error = %e, "runtime override not applicable; unset it");
            }
        }
        *self.runtime_overrides.borrow_mut() = set;
        for e in self.apply_manager_overrides() {
            tracing::warn!(error = %e, "runtime override refused");
        }
    }

    /// `MSG_GET_RUNTIME_CONFIG`. Leader-gated: only the leader has replayed
    /// the rows.
    pub(crate) fn handle_get_runtime_config(&self) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&GetRuntimeConfigResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                entries: Vec::new(),
            }));
        }
        Ok(rkyv_encode(&GetRuntimeConfigResp {
            code: CODE_OK,
            message: String::new(),
            entries: self.runtime_overrides.borrow().values().map(entry).collect(),
        }))
    }

    /// `MSG_SET_RUNTIME_CONFIG`. Leader-gated, admin only; audited.
    pub(crate) async fn handle_set_runtime_config(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
        }
        let req: SetRuntimeConfigReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let (code, message) = self.set_runtime_config(&req).await;
        self.append_audit(MgrAuditEntry {
            op: AUDIT_OP_RUNTIME_CONFIG,
            node_id: 0,
            extent_id: 0,
            by: req.set_by.clone(),
            reason: match &req.value {
                Some(v) => format!("{} = {v}", req.key),
                None => format!("unset {}", req.key),
            },
            result_code: code,
            result_message: message.clone(),
            ts_ns: 0,
        })
        .await;
        Self::code_resp(code, message)
    }

    async fn set_runtime_config(&self, req: &SetRuntimeConfigReq) -> (u8, String) {
        if let Some(value) = &req.value {
            if let Err(e) = check_entry(&req.key, value) {
                return (CODE_INVALID_ARGUMENT, e);
            }
        }
        let _guard = self.runtime_config_lock.lock().await;
        let old = self.runtime_overrides.borrow().clone();
        let mut new = old.clone();
        match &req.value {
            Some(value) => {
                new.insert(
                    req.key.clone(),
                    RuntimeOverrideRecord {
                        key: req.key.clone(),
                        value: value.clone(),
                        set_by: req.set_by.clone(),
                        set_at: Self::epoch_seconds(),
                    },
                );
            }
            None => {
                if new.remove(&req.key).is_none() {
                    return (CODE_NOT_FOUND, format!("no override of '{}'", req.key));
                }
            }
        }

        // The manager trials on itself, so its own flags count; a PS or EN
        // set is checked against the defaults.
        let is_manager = role_of(&req.key) == ManagerParams::ROLE;
        let trialled = match role_of(&req.key) {
            ManagerParams::ROLE => {
                let before = apply_overrides(&*self.params, &applicable(&old));
                let r = added(apply_overrides(&*self.params, &applicable(&new)), &before);
                if r.is_err() {
                    apply_overrides(&*self.params, &applicable(&old));
                }
                r
            }
            PsParams::ROLE => added(
                trial::<PsParams>(&applicable(&new)),
                &trial::<PsParams>(&applicable(&old)),
            ),
            _ => added(
                trial::<EnParams>(&applicable(&new)),
                &trial::<EnParams>(&applicable(&old)),
            ),
        };
        if let Err(e) = trialled {
            return (CODE_INVALID_ARGUMENT, e);
        }

        if let Some(etcd) = &self.etcd {
            let key = format!("{PREFIX}{}", req.key);
            let written = match new.get(&req.key) {
                Some(row) => etcd.put_msgs_txn(vec![(key, crate::persist::encode(row))]).await,
                None => etcd.put_and_delete_txn(Vec::new(), vec![key]).await,
            };
            if let Err(err) = written {
                if is_manager {
                    apply_overrides(&*self.params, &applicable(&old));
                }
                return (Self::err_to_code(&err), err.to_string());
            }
        }
        tracing::info!(
            key = %req.key,
            value = ?req.value,
            set_by = %req.set_by,
            "runtime override changed"
        );
        *self.runtime_overrides.borrow_mut() = new;
        (CODE_OK, String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_checked_against_its_roles_declaration() {
        assert!(check_entry("ps.gc-rate-bytes-per-sec", "64M").is_ok());
        assert!(check_entry("en.recovery-parallelism", "4").is_ok());
        assert!(check_entry("manager.recovery-max-global", "8").is_ok());
        // Out of range.
        assert!(check_entry("en.recovery-parallelism", "0").is_err());
        // One spelling per key.
        assert!(check_entry("ps.gc_rate_bytes_per_sec", "1").is_err());
        // A startup-only parameter is not overridable.
        assert!(check_entry("en.fd-cache-cap", "100").is_err());
        // Wrong role for the flag, unknown role, no role.
        assert!(check_entry("en.gc-rate-bytes-per-sec", "1").is_err());
        assert!(check_entry("client.rpc-timeout-ms", "1").is_err());
        assert!(check_entry("recovery-parallelism", "1").is_err());
    }

    #[test]
    fn a_set_that_breaks_a_cross_field_check_is_refused() {
        let row = |k: &str, v: &str| {
            (
                k.to_string(),
                RuntimeOverrideRecord {
                    key: k.to_string(),
                    value: v.to_string(),
                    ..Default::default()
                },
            )
        };
        let ok: Overrides = [row("ps.compact-max-files", "8")].into_iter().collect();
        assert!(trial::<PsParams>(&applicable(&ok)).is_empty());
        let min = PsParams::default().compact_min_files.get();
        let bad: Overrides = [row("ps.compact-max-files", &(min - 1).to_string())]
            .into_iter()
            .collect();
        assert!(
            !trial::<PsParams>(&applicable(&bad)).is_empty(),
            "compact-max-files below compact-min-files must be refused"
        );
    }

    #[test]
    fn only_the_refusals_a_change_adds_refuse_it() {
        assert!(added(vec!["a".into()], &["a".to_string()]).is_ok());
        assert!(added(vec!["a".into(), "b".into()], &["a".to_string()]).is_err());
    }

    #[test]
    fn a_row_this_binary_cannot_apply_is_listed_but_not_applied() {
        let mut set = Overrides::new();
        set.insert(
            "ps.retired-knob".into(),
            RuntimeOverrideRecord {
                key: "ps.retired-knob".into(),
                value: "1".into(),
                ..Default::default()
            },
        );
        assert!(applicable(&set).is_empty());
        assert!(trial::<PsParams>(&applicable(&set)).is_empty());
    }
}
