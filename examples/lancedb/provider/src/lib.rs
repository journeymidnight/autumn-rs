//! Registers the `autumn://` scheme with Lance's object-store registry.
//!
//! The native demo hands LanceDB a store OBJECT through the deprecated
//! `ObjectStoreParams::object_store` field, which only a caller compiled
//! against lance can do. A prebuilt LanceDB — the Python wheel above all —
//! never sees that field: it resolves a store from the URL SCHEME through
//! `ObjectStoreRegistry`. This crate is the other half, so the same adapter
//! serves Rust and Python instead of Python falling back to a FUSE mount.
//!
//! Deliberately outside the server workspace: it depends on `lance-io`, and
//! lance pulls DataFusion behind it. That graph has no business near the
//! partition server's.

use std::collections::HashMap;
use std::sync::Arc;

use autumn_object_store::AutumnObjectStore;
use lance_core::error::{Error, Result};
use lance_io::object_store::{
    DEFAULT_CLOUD_IO_PARALLELISM, ObjectStore, ObjectStoreParams, ObjectStoreProvider,
    StorageOptions,
};
use url::Url;

/// The URL scheme this provider answers to.
pub const SCHEME: &str = "autumn";

/// Required. The existing namespace/sub-prefix that holds this dataset.
pub const OPT_SCOPE: &str = "autumn_scope";

/// Optional. A credential file as `autumn-op principal-create` writes it.
/// Parsed by `autumn_client::read_credential_file`, the same reader
/// `autumnfs --credential-file` uses, so every form that tool emits is
/// accepted here too — including the LABELED `principal:`/`credential:` pair
/// it actually prints.
pub const OPT_CREDENTIAL_FILE: &str = "autumn_credential_file";

/// Maps `autumn://<manager host:port>/<object path>` onto [`AutumnObjectStore`].
///
/// The manager rides in the URL because that is what makes a dataset URI
/// self-contained. The SCOPE does not: `new_store` is handed the table's URL,
/// not the connection's, so `autumn://mgr/objects/demo/vectors.lance` gives no
/// rule for where the scope stops and the object path starts. Splitting it by
/// counting segments would be a guess that silently writes to the wrong prefix
/// when it is wrong, so the scope is a named option and its absence is an
/// error rather than a default.
#[derive(Debug, Default)]
pub struct AutumnStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for AutumnStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let manager = manager_of(&base_path)?;
        let options = params.storage_options();
        let scope = scope_of(options)?;

        let store = match credential_of(options)? {
            Some((principal, secret)) => {
                AutumnObjectStore::connect_with_credential(&manager, &scope, principal, secret)
                    .await
            }
            None => AutumnObjectStore::connect(&manager, &scope).await,
        }
        .map_err(|e| {
            Error::invalid_input(format!(
                "autumn: cannot open scope '{scope}' on manager '{manager}': {e}"
            ))
        })?;

        let storage_options = StorageOptions(options.cloned().unwrap_or_default());
        Ok(ObjectStore::new(
            Arc::new(store),
            base_path,
            params.block_size,
            // None, not params.object_store_wrapper: `ObjectStoreRegistry::build_store`
            // applies that wrapper to whatever a provider returns, so passing it
            // here too runs it twice — a mirroring wrapper would mirror twice.
            None,
            params.use_constant_size_upload_parts,
            // Autumn's list is a prefix range scan over ordered KV, so a page
            // arrives in key order. Lance uses this to skip re-sorting and to
            // stop a scan early.
            params.list_is_lexically_ordered.unwrap_or(true),
            // The adapter's bridge admits 32 active plus 32 queued jobs, so a
            // full 64 fits and anything past it is backpressure, not an error.
            DEFAULT_CLOUD_IO_PARALLELISM,
            storage_options.download_retry_count(),
            options,
        ))
    }

    /// Two scopes on one manager are two different stores.
    ///
    /// The default is `scheme$authority`, which for us is the manager alone and
    /// would give both scopes the same identity. That identity is the registry's
    /// cache key (`ObjectStoreRegistry::get_store`) and the metrics label, and
    /// `ObjectStore`'s own contract is that the prefix plus the path names an
    /// object uniquely — which the manager alone does not do.
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        let scope = scope_of(storage_options)?;
        Ok(format!("{SCHEME}${}${scope}", authority_of(url)?))
    }
}

fn authority_of(url: &Url) -> Result<String> {
    let host = url.host_str().ok_or_else(|| {
        Error::invalid_input(format!(
            "autumn: '{url}' has no manager address; expected autumn://<host>:<port>/<path>"
        ))
    })?;
    match url.port() {
        Some(port) => Ok(format!("{host}:{port}")),
        None => Ok(host.to_string()),
    }
}

fn manager_of(url: &Url) -> Result<String> {
    let manager = authority_of(url)?;
    if url.port().is_none() {
        return Err(Error::invalid_input(format!(
            "autumn: '{url}' names no manager port; expected autumn://<host>:<port>/<path>"
        )));
    }
    Ok(manager)
}

fn scope_of(options: Option<&HashMap<String, String>>) -> Result<String> {
    let scope = options
        .and_then(|o| o.get(OPT_SCOPE))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::invalid_input(format!(
                "autumn: storage option '{OPT_SCOPE}' is required and names the existing \
                 namespace/sub-prefix holding this dataset, e.g. objects/lance-demo"
            ))
        })?;
    Ok(scope.trim_matches('/').to_string())
}

/// Reuses the client's reader rather than re-deriving the format.
///
/// A second parser written from the doc comment got this wrong: it assumed two
/// bare lines, while `autumn-op principal-create` prints `principal: <name>`
/// and `credential: <hex>`, so every authenticated open would have failed on
/// "not hex". `read_credential_file` is what `autumnfs` already uses and
/// accepts all the forms that tool emits.
fn credential_of(options: Option<&HashMap<String, String>>) -> Result<Option<(String, Vec<u8>)>> {
    let Some(path) = options.and_then(|o| o.get(OPT_CREDENTIAL_FILE)) else {
        return Ok(None);
    };
    let (principal, secret) = autumn_client::read_credential_file(path).map_err(|e| {
        Error::invalid_input(format!("autumn: '{OPT_CREDENTIAL_FILE}' {path}: {e}"))
    })?;
    if principal.is_empty() {
        return Err(Error::invalid_input(format!(
            "autumn: {path} carries a secret but no principal name; autumn-op \
             principal-create emits both lines and the connect needs the name"
        )));
    }
    Ok(Some((principal, secret)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn manager_comes_from_the_url_authority() {
        let url = Url::parse("autumn://10.0.0.5:9001/lancedb/vectors.lance").unwrap();
        assert_eq!(manager_of(&url).unwrap(), "10.0.0.5:9001");
    }

    #[test]
    fn a_manager_without_a_port_is_refused_rather_than_guessed() {
        let url = Url::parse("autumn://10.0.0.5/lancedb").unwrap();
        assert!(manager_of(&url).is_err());
    }

    /// The scope decides which prefix bytes land in, so a missing one must stop
    /// the open rather than resolve to some root.
    #[test]
    fn a_missing_scope_is_an_error_not_a_default() {
        assert!(scope_of(None).is_err());
        assert!(scope_of(Some(&opts(&[(OPT_SCOPE, "   ")]))).is_err());
        assert_eq!(
            scope_of(Some(&opts(&[(OPT_SCOPE, "/objects/demo/")]))).unwrap(),
            "objects/demo"
        );
    }

    /// Without the scope in the prefix these two share a registry cache entry
    /// and a metrics label.
    #[test]
    fn two_scopes_on_one_manager_get_different_prefixes() {
        let url = Url::parse("autumn://mgr:9001/lancedb").unwrap();
        let provider = AutumnStoreProvider;
        let one = provider
            .calculate_object_store_prefix(&url, Some(&opts(&[(OPT_SCOPE, "objects/a")])))
            .unwrap();
        let two = provider
            .calculate_object_store_prefix(&url, Some(&opts(&[(OPT_SCOPE, "objects/b")])))
            .unwrap();
        assert_ne!(one, two);
        assert_eq!(one, "autumn$mgr:9001$objects/a");
    }

    /// The format is what `autumn-op principal-create` PRINTS — a labeled
    /// pair, not two bare lines. A parser written from the prose got this
    /// wrong and would have failed every authenticated open on "not hex".
    #[test]
    fn the_labeled_form_autumn_op_emits_is_accepted() {
        let dir = std::env::temp_dir().join(format!("autumn-prov-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cred");
        std::fs::write(&path, "principal: alice\ncredential: 00ff10\n").unwrap();
        let got = credential_of(Some(&opts(&[(
            OPT_CREDENTIAL_FILE,
            path.to_str().unwrap(),
        )])))
        .unwrap();
        assert_eq!(got, Some(("alice".to_string(), vec![0x00, 0xff, 0x10])));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A bare hex line parses, but names no principal, and the connect needs
    /// one — so it is refused here rather than sent as an empty name.
    #[test]
    fn a_secret_without_a_principal_is_refused() {
        let dir = std::env::temp_dir().join(format!("autumn-prov-anon-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cred");
        std::fs::write(&path, "00ff10\n").unwrap();
        assert!(
            credential_of(Some(&opts(&[(
                OPT_CREDENTIAL_FILE,
                path.to_str().unwrap()
            )])))
            .is_err()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn no_credential_option_means_an_unauthenticated_connect() {
        assert_eq!(credential_of(Some(&opts(&[(OPT_SCOPE, "x")]))).unwrap(), None);
    }
}
