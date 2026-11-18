//! convert_fs_v5 parameters.

crate::params! {
    /// convert_fs_v5 parameters.
    pub struct ConvertFsParams("convert_fs_v5") {
        /// Manager endpoints.
        manager: String = "127.0.0.1:9001".into();
        transport: crate::Transport;
        /// Credential file (`<principal>\n<hex>`); needed when the cluster
        /// protects `fs/`.
        credential_file: Option<std::path::PathBuf>;
        /// Count what would move, write nothing.
        [switch] dry_run: bool;
    }
}

impl crate::CrossCheck for ConvertFsParams {}
