//! autumn-client global parameters (before the command).

crate::params! {
    /// autumn-client global parameters.
    pub struct ClientCliParams("client-cli") {
        /// Manager endpoints.
        manager: String = "127.0.0.1:9001".into();
        transport: crate::Transport;
        /// Key scope of the data-plane commands (`ns` or `ns/sub`).
        namespace: Option<String>;
        /// Credential file (`<principal>\n<hex>`, from `autumn-op
        /// principal-create`); needed when the namespace is protected.
        credential_file: Option<String>;
    }
    aliases {
        "scope" => "namespace",
    }
}

impl crate::CrossCheck for ClientCliParams {}
