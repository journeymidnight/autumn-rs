//! autumn-op global parameters. They may stand anywhere on the line, before or
//! after the command.

crate::params! {
    /// autumn-op global parameters.
    pub struct OpParams("op") {
        /// Manager endpoints.
        manager: String = "127.0.0.1:9001".into();
        /// Must match the manager's transport.
        transport: crate::Transport;
        /// JSON output.
        [switch] json: bool;
        /// The cluster secret the servers were started with; every server
        /// refuses an operator connection without it.
        cluster_secret_file: Option<std::path::PathBuf>;
        /// Data-plane credential (`<principal>\n<hex>`) for the commands that
        /// read or write partition keys (e.g. `presplit --namespace fs`).
        credential_file: Option<String>;
        /// Async op commands (split, merge, rebalance, compact, gc, forcegc,
        /// force-ec-convert): wait for the op to finish and exit on its outcome.
        [switch] wait: bool;
        /// Longest --wait, seconds (0 = the default).
        timeout: u64 = 600;
    }
    removed {
        "admin-token" => "--admin-token was removed: pass the cluster secret with --cluster-secret-file",
        "admin-token-file" => "--admin-token-file was removed: pass the cluster secret with --cluster-secret-file",
    }
}

impl crate::CrossCheck for OpParams {}
