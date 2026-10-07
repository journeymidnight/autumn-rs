//! Manager identity and membership.
//!
//! Each manager runs under an operator-assigned `--manager-id` and holds
//! `managerAlive/<id>` on its own etcd lease while it runs. That key is two
//! things: the claim that keeps a second process from running under the same
//! id, and the presence the fleet count reads. The leader turns presence into
//! the persistent membership `managerMembers/<id>` (`MemberRecord`), which is
//! what "expected managers" means; only an operator's remove shrinks it.
//!
//! Losing the lease is not a reason to stop: the leader fence already makes a
//! manager without leadership harmless, and an etcd blip must not take every
//! manager down. A manager that lost its key claims it again, and exits only
//! if another process holds the id by then.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use autumn_common::AppError;

use crate::persist::records::MemberRecord;
use crate::AutumnManager;

pub(crate) const MANAGER_ALIVE_PREFIX: &str = "managerAlive/";
pub(crate) const MANAGER_MEMBERS_PREFIX: &str = "managerMembers/";
const PRESENCE_TTL_SECS: i64 = 10;
const RETRY: Duration = Duration::from_secs(1);

/// Who this manager process is, from its command line.
#[derive(Clone, Debug)]
pub struct ManagerIdentity {
    /// `--manager-id`, non-zero.
    pub id: u64,
    /// The RPC address it serves on, for listings and conflict messages.
    pub address: String,
}

pub(crate) fn alive_key(id: u64) -> String {
    format!("{MANAGER_ALIVE_PREFIX}{id}")
}

/// `managerAlive/<id>` value: `<instance_id>\n<address>`. The instance id
/// tells this process's own key from another holder's.
fn alive_value(instance_id: &str, address: &str) -> Vec<u8> {
    format!("{instance_id}\n{address}").into_bytes()
}

fn parse_alive_value(v: &[u8]) -> (String, String) {
    let s = String::from_utf8_lossy(v);
    match s.split_once('\n') {
        Some((instance, address)) => (instance.to_string(), address.to_string()),
        None => (s.into_owned(), String::new()),
    }
}

pub(crate) enum Claim {
    /// This process holds the id on this lease.
    Held(i64),
    /// Another process holds it; its address.
    Taken(String),
}

impl AutumnManager {
    /// One attempt to hold `managerAlive/<id>`. A key still carrying this
    /// process's instance id is ours (a keepalive that failed while the lease
    /// lived on), and its lease is adopted.
    pub(crate) async fn claim_presence(&self) -> Result<Claim> {
        let (etcd, ident) = match (&self.etcd, &self.identity) {
            (Some(e), Some(i)) => (e, i),
            _ => anyhow::bail!("presence needs etcd and a manager id"),
        };
        let key = alive_key(ident.id);
        loop {
            let got = etcd.client.get(key.as_bytes()).await?;
            if let Some(kv) = got.kvs.first() {
                let (instance, address) = parse_alive_value(&kv.value);
                if instance == *self.instance_id {
                    return Ok(Claim::Held(kv.lease));
                }
                return Ok(Claim::Taken(address));
            }
            let lease = etcd.client.lease_grant(PRESENCE_TTL_SECS).await?.id;
            let txn = autumn_etcd::proto::TxnRequest {
                compare: vec![autumn_etcd::Cmp::create_revision(key.as_bytes(), 0)],
                success: vec![autumn_etcd::Op::put_with_lease(
                    key.as_bytes(),
                    &alive_value(&self.instance_id, &ident.address),
                    lease,
                )],
                failure: vec![],
            };
            if etcd.client.txn(txn).await?.succeeded {
                return Ok(Claim::Held(lease));
            }
            // Someone created it between the get and the txn; the next get
            // names them. The unused lease expires on its own.
        }
    }

    /// Startup: wait as long as it takes to hold the id. A process still
    /// holding it is either our predecessor (its lease expires within
    /// `PRESENCE_TTL_SECS`) or a misconfigured duplicate, which the log names.
    pub(crate) async fn claim_presence_until_held(&self) -> i64 {
        let id = self.identity.as_ref().map_or(0, |i| i.id);
        loop {
            match self.claim_presence().await {
                Ok(Claim::Held(lease)) => return lease,
                Ok(Claim::Taken(holder)) => {
                    tracing::error!(manager_id = id, %holder, "manager id is held by another process; waiting");
                }
                Err(e) => tracing::warn!(manager_id = id, error = %e, "claiming the manager id failed"),
            }
            compio::time::sleep(RETRY).await;
        }
    }

    /// Keep `managerAlive/<id>` alive; reclaim it after a loss. Exits the
    /// process only when the reclaim finds the id held by someone else.
    pub(crate) async fn presence_loop(self) {
        let id = self.identity.as_ref().map_or(0, |i| i.id);
        let Some(etcd) = &self.etcd else {
            return;
        };
        loop {
            if let Ok(keeper) = etcd.client.lease_keep_alive(self.presence_lease.get()).await {
                loop {
                    compio::time::sleep(Duration::from_secs(2)).await;
                    match keeper.keep_alive().await {
                        Ok(r) if r.ttl > 0 => {}
                        _ => break,
                    }
                }
            }
            tracing::warn!(manager_id = id, "manager id keepalive failed; reclaiming");
            loop {
                match self.claim_presence().await {
                    Ok(Claim::Held(lease)) => {
                        self.presence_lease.set(lease);
                        tracing::info!(manager_id = id, "manager id reclaimed");
                        break;
                    }
                    Ok(Claim::Taken(holder)) => {
                        tracing::error!(
                            manager_id = id,
                            %holder,
                            "manager id taken by another process while this one lost its lease; exiting"
                        );
                        std::process::exit(1);
                    }
                    Err(e) => {
                        tracing::warn!(manager_id = id, error = %e, "reclaiming the manager id failed");
                        compio::time::sleep(RETRY).await;
                    }
                }
            }
        }
    }

    /// Leader, every 2 s: read who is present and fold it into the
    /// membership. A present id becomes (or stays) a member; a member that
    /// is no longer present gets the time the leader noticed.
    pub(crate) async fn manager_member_loop(self) {
        loop {
            compio::time::sleep(Duration::from_secs(2)).await;
            if !self.leader.get() {
                continue;
            }
            if let Err(e) = self.sync_manager_members().await {
                tracing::warn!(error = %e, "syncing manager members failed");
            }
        }
    }

    /// `managerAlive/` as etcd has it now: id → address.
    pub(crate) async fn read_manager_presence(&self) -> Result<BTreeMap<u64, String>, AppError> {
        let Some(etcd) = &self.etcd else {
            return Ok(BTreeMap::new());
        };
        let got = etcd
            .client
            .get_prefix(MANAGER_ALIVE_PREFIX)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        got.kvs
            .iter()
            .map(|kv| {
                let id = Self::parse_id_from_key(MANAGER_ALIVE_PREFIX, &kv.key)
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                Ok((id, parse_alive_value(&kv.value).1))
            })
            .collect()
    }

    pub(crate) async fn sync_manager_members(&self) -> Result<(), AppError> {
        let Some(etcd) = &self.etcd else {
            return Ok(());
        };
        let alive = self.read_manager_presence().await?;

        let _members = self.manager_member_lock.lock().await;
        let now_ms = Self::now_s_ms().1;
        let puts: Vec<(u64, MemberRecord)> = {
            let s = self.store.inner.borrow();
            let joined = alive.iter().filter_map(|(id, address)| {
                match s.manager_members.get(id) {
                    Some(m) if m.address == *address && m.left_at_ms == 0 => None,
                    Some(m) => Some((
                        *id,
                        MemberRecord {
                            address: address.clone(),
                            joined_at_ms: m.joined_at_ms,
                            left_at_ms: 0,
                        },
                    )),
                    None => Some((
                        *id,
                        MemberRecord {
                            address: address.clone(),
                            joined_at_ms: now_ms,
                            left_at_ms: 0,
                        },
                    )),
                }
            });
            let left = s.manager_members.iter().filter_map(|(id, m)| {
                (!alive.contains_key(id) && m.left_at_ms == 0).then(|| {
                    (
                        *id,
                        MemberRecord {
                            left_at_ms: now_ms,
                            ..m.clone()
                        },
                    )
                })
            });
            joined.chain(left).collect()
        };
        if puts.is_empty() {
            return Ok(());
        }
        etcd.put_msgs_txn(
            puts.iter()
                .map(|(id, m)| (format!("{MANAGER_MEMBERS_PREFIX}{id}"), crate::persist::encode(m)))
                .collect(),
        )
        .await?;
        let mut s = self.store.inner.borrow_mut();
        for (id, m) in puts {
            s.manager_members.insert(id, m);
        }
        Ok(())
    }

    /// Delete a manager member, in one txn that also requires its
    /// `managerAlive/` key to be absent: a running manager is refused.
    pub(crate) async fn remove_manager_member(&self, id: u64) -> Result<(), AppError> {
        let Some(etcd) = &self.etcd else {
            return Err(AppError::InvalidArgument(
                "manager members exist only with etcd".to_string(),
            ));
        };
        let _members = self.manager_member_lock.lock().await;
        if !self.store.inner.borrow().manager_members.contains_key(&id) {
            return Err(AppError::NotFound(format!("manager {id} is not a member")));
        }
        let removed = etcd
            .txn_fenced(
                vec![autumn_etcd::Cmp::create_revision(alive_key(id).as_bytes(), 0)],
                vec![autumn_etcd::Op::delete(
                    format!("{MANAGER_MEMBERS_PREFIX}{id}").as_bytes(),
                )],
                vec![],
            )
            .await?;
        if !removed {
            return Err(AppError::Precondition(format!(
                "manager {id} is running (it holds {}); stop it before removing it",
                alive_key(id)
            )));
        }
        self.store.inner.borrow_mut().manager_members.remove(&id);
        Ok(())
    }
}
