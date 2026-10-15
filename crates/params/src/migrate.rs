//! migratev1_v2 parameters.

crate::params! {
    /// migratev1_v2 parameters.
    pub struct MigrateParams("migrate") {
        /// etcd endpoints.
        [required] etcd: Vec<String>;
        /// Report what would change, write nothing.
        [switch] dry_run: bool;
    }
}

impl crate::CrossCheck for MigrateParams {}
